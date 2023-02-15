


import asyncio
import logging
import math
import os
import time
from asyncio import Queue
from collections import namedtuple, defaultdict
from hashlib import sha1

from protocol import PeerConnection, REQUEST_SIZE
from tracker import Tracker



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
        self.peers = [PeerConnection(self.available_peers,
                                     self.tracker.torrent.info_hash,
                                     self.tracker.peer_id,
                                     self.piece_manager,
                                     self._on_block_retrieved)
                      for _ in range(MAX_PEER_CONNECTIONS)]
        previous = None
        interval = 30*60

        while True:
            if self.piece_manager.complete:
                logging.info('Torrent Done')
                break
            if self.abort:
                logging.info('Aborting torrent')
                break

            current = time.time()
            if (not previous) or (previous + interval < current):
                response = await self.tracker.connect_tracker(
                    first=previous if previous else False,
                    uploaded=self.piece_manager.bytes_uploaded,
                    downloaded=self.piece_manager.bytes_downloaded
                )
                if response:
                    previous = current
                    interval = response.interval
                    self._empty_queue()
                    for peer in response.peers:
                        self.available_peers.put_nowait(peer)
            
            else:
                await asyncio.sleep(5)
        self.stop()

    def _empty_queue(self):
        while not self.available_peers.empty():
            self.available_peers.get_nowait()

    def stop(self):
        self.abort = True
        for peer in self.peers:
            peer.stop()
        self.piece_manager.close()
        self.tracker.close()

    def on_block_retrieved(self, peer_id, piece_index, block_offset, data):
        '''
        callback called by peer connection when a block is retrieved
        '''
        self.piece_manager.block_recieved(peer_id=peer_id, piece_index=piece_index, block_offset=block_offset, data=data)



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



PendingRequest = namedtuple('PendingRequest', ['block', 'added'])



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
