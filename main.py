
import random
import string
import asyncio
import aiohttp
import logging
import socket
import os
import time
import math
from urllib.parse import urlencode
from asyncio import Queue
from hashlib import sha1
from struct import unpack, pack
from collections import OrderedDict, namedtuple, defaultdict

#software version
VERSION = '0.0.1'

#consts for decoding bencoded bytes
TOKEN_INT = b'i'
TOKEN_LIST = b'l'
TOKEN_DICT = b'd'
TOKEN_END = b'e'
TOKEN_STRING_SEPARATOR = b':'

REQUEST_SIZE = 2**14
MAX_PEER_CONNECTIONS = 40



class Client:
    '''
    Client is the local peer
    '''

    def __init__(self, torrent):
        self.tracker = Tracker(torrent)
        self.available_peers = Queue()
        self.peers = []
        self.piece_manager = None #####TODO add class
        self.abort = False

    async def start(self):
        '''
        start downloading the torrent
        '''
        pass



class Block:
    '''
    partial piece, transfered between peers.
    
    equal in size to REQUEST_SIZE, except the final block
    '''
    Missing = 0
    Pending = 1
    Retrieved = 2

    def __init__(self, piece: int, offset: int, length: int):
        self.piece = piece
        self.offset = offset
        self.length = length
        self.status = Block.Missing
        self.data = None



class Piece:
    '''
    a part of the torrents contents.

    pieces are defined in the .torrent metadata, however
    blocks are whats actually exchanged
    '''
    def __init__(self, index: int, blocks: list, hash_value):
        self.index = index
        self.blocks = blocks
        self.hash = hash_value

    def reset(self):
        '''
        reset all blocks to missing
        '''
        for block in self.blocks:
            block.status = Block.Missing

    def next_request(self) -> Block:
        '''
        get the next block to request
        '''
        missing = [b for b in self.blocks if b.status is Block.Missing]
        if missing:
            missing[0].status = Block.Pending
            return missing[0]
        return None

    def block_recieved(self, offset: int, data: bytes):
        '''
        update the block info now it has been recieved
        '''
        matches = [b for b in self.blocks if b.offset == offset]
        block = matches[0] if matches else None
        if block:
            block.status = Block.Retrieved
            block.data = data
        else:
            logging.warning(f'trying to complete a non existing block {offset}')

    def is_complete(self) -> bool:
        '''
        checks if all blocks are recieved (but not hashed)
        '''
        blocks = [b for b in self.blocks if b.status is not Block.Retrieved]
        return len(blocks) is 0

    def is_hash_matching(self):
        '''
        checks if SHA1 hash for all blocks match the piece hash from .torrent file
        '''
        piece_hash = sha1(self.data).digest()
        return self.hash == piece_hash

    @property
    def data(self):
        '''
        returns the data for this piece
        '''
        retrieved = sorted(self.blocks, key=lambda b: b.offset)
        blocks_data = [b.data for b in retrieved]
        return b''.join(blocks_data)



class PieceManager:
    '''
    keeps track of all the available pieces from the currently
    connected peers, as well as all pieces available for others
    '''
    #####TODO add smarter piece request algorithm
    def __init__(self, torrent):
        self.torrent = torrent
        self.peers = {}
        self.pending_blocks = []
        self.missing_pieces = []
        self.ongoing_pieces = []
        self.have_pieces = []
        self.max_pending_time = 300 * 1000 #5min
        self.missing_pieces = None #self._initiate_pieces()
        self.total_pieces = len(torrent.pieces)
        self.fd = os.open(self.torrent.output_file, os.O_RDWR | os.O_CREAT)

    def _initiate_pieces(self) -> list(Piece):
        '''
        create list of pieces and blocks based on the num of pieces
        and size of torrent
        '''
        torrent = self.torrent
        pieces = []
        total_pieces = len(torrent.pieces)
        std_piece_blocks = math.ceil(torrent.piece_length / REQUEST_SIZE)

        for index, hash_val in enumerate(torrent.pieces):
            if index < (total_pieces - 1):
                blocks = [Block(index, offset * REQUEST_SIZE, REQUEST_SIZE)
                          for offset in range(std_piece_blocks)]
            else:
                last_length = torrent.total_size % torrent.piece_length
                num_blocks = math.ceil(last_length / REQUEST_SIZE)
                blocks = [Block(index, offset * REQUEST_SIZE, REQUEST_SIZE)
                          for offset in range(num_blocks)]

                if last_length % REQUEST_SIZE > 0:
                    last_block = blocks[-1]
                    last_block.length = last_length % REQUEST_SIZE
                    blocks[-1] = last_block
            pieces.append(Piece(index, blocks, hash_val))
        return pieces

    def close(self):
        '''
        close any active resources if used
        '''
        if self.fd:
            os.close(self.fd)

    @property
    def complete(self):
        '''
        checks if all pieces are downloaded
        '''
        return len(self.have_pieces) == self.total_pieces

    @property
    def bytes_downloaded(self) -> int:
        '''
        gets the number of bytes downloaded
        '''
        return len(self.have_pieces) * self.torrent.piece_length

    @property
    def bytes_uploaded(self) -> int:
        #####TODO
        return 0

    def add_peer(self, peer_id, bitfield):
        '''
        add a peer and the bitfield representing their pieces
        '''
        self.peers[peer_id] = bitfield

    def update_peer(self, peer_id, index: int):
        '''
        updates info about pieces a peer has
        '''
        if peer_id in self.peers:
            self.peers[peer_id][index] = 1

    def remove_peer(self, peer_id):
        '''
        remove a previously added peer
        '''
        if peer_id in self.peers:
            del self.peers[peer_id]

    def next_request(self, peer_id) -> Block:
        '''
        get the next block that should be requested from a peer.
        '''
        if peer_id not in self.peers:
            return None
        block = self._expired_request(peer_id)
        if not block:
            block = self._next_ongoing(peer_id)
            if not block:
                block = self._get_rarest_piece(peer_id).next_request()
        return block

    def _expired_request(self, peer_id) -> Block:
        '''
        see if any pending blocks have been requested for longer then 
        MAX_PENDING_TIME
        '''
        current = int(round(time.time() * 1000))
        for request in self.pending_blocks:
            if self.peers[peer_id][request.block.piece]:
                if request.added + self.max_pending_time < current:
                    logging.info(f're-requesting block {request.block.offset}',
                                  f'for piece {request.block.piece}')
                    request.added = current
                    return request.block
        return None

    def _next_onging(self, peer_id) -> Block:
        '''
        go through the ongoing piece and request next block
        '''
        for piece in self.ongoing_pieces:
            if self.peers[peer_id][piece.index]:
                block = piece.next_request()
                if block:
                    self.pending_blocks.append(
                        PendingRequest(block, int(round(time.time() * 1000)))
                    )
                    return block
        return None

    def _next_missing(self, peer_id) -> Block:
        '''
        look at missing pieces and return the next block to request
        '''
        for index, piece in enumerate(self.missing_pieces):
            if self.peers[peer_id][piece.index]:
                piece = self.missing_pieces.pop(index)
                self.ongoing_pieces.append(piece)
                return piece.next_request()
        return None

    def _get_rarest_piece(self, peer_id):
        '''
        looking at all the missing pieces, get the piece that the least
        peers have
        '''
        piece_count = defaultdict(int)
        for piece in self.missing_pieces:
            if not self.peers[peer_id][piece.index]:
                continue
            for p in self.peers:
                if self.peers[p][piece.index]:
                    piece_count[piece] += 1
        
        rarest_piece = min(piece_count, key=lambda p: piece_count[p])
        self.missing_pieces.remove(rarest_piece)
        self.ongoing_pieces.append(rarest_piece)
        return rarest_piece


    def _write(self, piece):
        '''
        write a piece to disk
        '''
        pos = piece.index * self.torrent.piece_length
        os.lseek(self.fd, pos, os.SEEK_SET)
        os.write(self.fd, piece.data)

    def block_recieved(self, peer_id, piece_index, block_offset, data):
        '''
        once piece is recieved, hash is checked, if it fails then
        all the blocks are returned to missing, if it succeeds
        then theyre written to disk and the piece is 'have'
        '''
        logging.debug(f'recieved block {block_offset} for piece {piece_index} from peer {peer_id}')
        for index, request in enumerate(self.pending_blocks):
            if request.block.piece == piece_index and request.block.offset == block_offset:
                del self.pending_blocks[index]
                break

        pieces = [p for p in self.ongoing_pieces if p.index == piece_index]
        piece = pieces[0] if pieces else None
        if piece:
            piece.block_recieved(block_offset, data)
            if piece.is_complete():
                if piece.is_hash_matching():
                    self._write(piece)
                    self.ongoing_pieces.remove(piece)
                    self.have_pieces.append(piece)
                    complete = (self.total_pieces - len(self.missing_pieces) - len(self.ongoing_pieces))
                    logging.info(f'{complete} / {self.total_pieces} downloaded {((complete/self.total_pieces)*100):.3f}%')
                else:
                    logging.info(f'discarding corrupt piece {piece.index}')
                    piece.reset()
        else:
            logging.warning('trying to update piece not currently ongoing')

    

PendingRequest = namedtuple('PendingRequest', ['block', 'added'])



class Tracker:
    '''
    Tracker is the connection to the active torrents tracker
    '''

    def __init__(self, torrent):
        self.torrent = torrent
        self.peer_id = create_peer_id()
        self.http_client = aiohttp.ClientSession()

    async def connect_tracker(self,
                              first: bool = None,
                              uploaded: int = 0,
                              downloaded: int = 0):
        '''
        announce to tracker current stats and status,
        get a list of peers avaliable for connection.

        :param first: wheather or not this is the first announce
        :param uploaded: total num of bytes uploaded for this torrent
        :param downloaded: total num of bytes downloaded for this torrent
        '''
        params = {
            'info_hash': self.torrent.info_hash,
            'peer_id': self.peer_id,
            'port': 6889,
            'uploaded': uploaded,
            'downloaded': downloaded,
            'left': self.torrent.total_size - downloaded,
            'compact': 1,
        }
        if first:
            params['event'] = 'started'

        url = self.torrent.announce + '?' + urlencode(params)
        logging.info('Connecting to traqcker at: ' + self.torrent.announce)

        async with self.http_client.get(url) as response:
            if not response.status == 200:
                raise ConnectionError(f'failed to connect to tracker: status code {response.status}')
            data = await response.read()
            self.raise_hidden_error(data)
            return TrackerResponse(Decoder(data).decode())
            
    def raise_hidden_error(self, tracker_response):
        '''
        detect tracker error even if response has status code 200

        tracker error will be encoded utf-8, an a sucessful response will not
        '''
        try:
            message = tracker_response.decode('utf-8')
            if 'failure' in message:
                raise ConnectionError(f'failed to connect to tracker: {message}')
        except UnicodeDecodeError:
            pass

    def close(self):
        self.http_client.close()



class TrackerResponse:
    '''
    Tracks the response from the tracker after a sucessful connection
    to the announce URL
    '''

    def __init__(self, response: dict):
        self.response = response

    @property
    def failure(self):
        '''
        contains the error message if the response failed, else will be None
        '''
        if b'failure reason' in self.response:
            return self.response[b'failure reason'].decode('utf-8')
        return None
    
    @property
    def interval(self) -> int:
        '''
        contains the interval in seconds the client should wait before
        re-engaging the tracker
        '''
        return self.response.get(b'interval', 0)

    @property
    def complete(self) -> int:
        '''
        num of peers with the complete file (seeders)
        '''
        return self.response.get(b'complete', 0)

    @property
    def peers(self):
        '''
        a list of each peer, structured as tuple(ip, port)

        the response from the tracker can either be a list of dicts,
        or a single string
        '''
        peers = self.response[b'peers']
        if type(peers) == list:
            logging.debug('Tracker returned Dict of peers')
            #####TODO Dict peer list support
            raise NotImplementedError()
        else:
            logging.debug('Tracker returned bString of peers')
            peers = [peers[i:i+6] for i in range(0, len(peers), 6)]
            return [(socket.inet_ntoa(p[:4]), decode_port(p[4:])) for p in peers]
    
    def __str__(self):
        return f"incomplete: {self.incomplete}\n" \
               f"complete: {self.complete}\n" \
               f"interval: {self.interval}\n" \
               f"peers: {''.join([x for (x, _) in self.peers])}\n"



class Decoder:
    '''
    decodes bencoded bytes
    '''
    def __init__(self, data: bytes):
        self._data = data
        self._index = 0

    def decode(self):
        '''
        decodes the data and returns a matching python object
        '''
        c = self._peek()
        if c is None:
            raise EOFError('Unexpected end-of-file')
        elif c == TOKEN_INT:
            self._consume()  # The token
            return self._decode_int()
        elif c == TOKEN_LIST:
            self._consume()  # The token
            return self._decode_list()
        elif c == TOKEN_DICT:
            self._consume()  # The token
            return self._decode_dict()
        elif c == TOKEN_END:
            return None
        elif c in b'01234567899':
            return self._decode_string()
        else:
            raise RuntimeError('Invalid token read at {0}'.format(
                str(self._index)))

    def _peek(self):
        '''
        return the next char, or None
        '''
        if self._index + 1 >= len(self._data):
            return None
        return self._data[self._index + 1]

    def _consume(self) -> bytes:
        '''
        read and consume the next char
        '''
        self._index += 1

    def _read(self, length: int) -> bytes:
        '''
        read the 'length' number of bytes and return result
        '''
        if self._index + length > len(self._data):
            raise IndexError(f'Cannot read {str(length)} bytes from current position {str(self._index)}')
        res = self._data[self._index:self._index+length]
        self._index += length
        return res

    def _read_until(self, token: bytes) -> bytes:
        """
        Read from the bencoded data until the given token is found and return
        the characters read.
        """
        try:
            occurrence = self._data.index(token, self._index)
            result = self._data[self._index:occurrence]
            self._index = occurrence + 1
            return result
        except ValueError:
            raise RuntimeError(f'Unable to find token {str(token)}')

    def _decode_int(self):
        return int(self._read_until(TOKEN_END))

    def _decode_list(self):
        res = []
        # Recursive decode the content of the list
        while self._data[self._index: self._index + 1] != TOKEN_END:
            res.append(self.decode())
        self._consume()  # The END token
        return res

    def _decode_dict(self):
        res = OrderedDict()
        while self._data[self._index: self._index + 1] != TOKEN_END:
            key = self.decode()
            obj = self.decode()
            res[key] = obj
        self._consume()  # The END token
        return res

    def _decode_string(self):
        bytes_to_read = int(self._read_until(TOKEN_STRING_SEPARATOR))
        data = self._read(bytes_to_read)
        return data



class Encoder:
    """
    Encodes a python object to a bencoded sequence of bytes.
    Supported python types is:
        - str
        - int
        - list
        - dict
        - bytes
    Any other type will simply be ignored.
    """
    def __init__(self, data):
        self._data = data

    def encode(self) -> bytes:
        """
        Encode a python object to a bencoded binary string
        :return The bencoded binary data
        """
        return self.encode_next(self._data)

    def encode_next(self, data):
        if type(data) == str:
            return self._encode_string(data)
        elif type(data) == int:
            return self._encode_int(data)
        elif type(data) == list:
            return self._encode_list(data)
        elif type(data) == dict or type(data) == OrderedDict:
            return self._encode_dict(data)
        elif type(data) == bytes:
            return self._encode_bytes(data)
        else:
            return None

    def _encode_int(self, value):
        return str.encode('i' + str(value) + 'e')

    def _encode_string(self, value: str):
        res = str(len(value)) + ':' + value
        return str.encode(res)

    def _encode_bytes(self, value: str):
        result = bytearray()
        result += str.encode(str(len(value)))
        result += b':'
        result += value
        return result

    def _encode_list(self, data):
        result = bytearray('l', 'utf-8')
        result += b''.join([self.encode_next(item) for item in data])
        result += b'e'
        return result

    def _encode_dict(self, data: dict) -> bytes:
        result = bytearray('d', 'utf-8')
        for k, v in data.items():
            key = self.encode_next(k)
            value = self.encode_next(v)
            if key and value:
                result += key
                result += value
            else:
                raise RuntimeError('Bad dict')
        result += b'e'
        return 





#helper functions
def create_peer_id():
    '''
    Create a unique peer id to be sent to the tracker

    peer id is 20 bytes, and structured = 
    
    'pTor-{3 byte version number truncating least sig bits}-0x{9 random bytes using 0-9 and A-F}'

    'pTor-001-0x4F9A10CCB'
    '''
    return f'pTor-{"".join([x[0] for x in VERSION.split(".")])}-0x' + ''.join(
        [random.choice(string.hexdigits.upper()) for _ in range(9)]
    )

def decode_port(port):
    '''
    convert 32-bit packed binary port num to int
    '''
    return unpack(">H", port)[0]

