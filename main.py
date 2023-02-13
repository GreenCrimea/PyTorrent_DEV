
import random
import string
import asyncio
import aiohttp
import logging
import socket
from urllib.parse import urlencode
from struct import unpack
from collections import OrderedDict

#software version
VERSION = '0.0.1'

#consts for decoding bencoded bytes
TOKEN_INT = b'i'
TOKEN_LIST = b'l'
TOKEN_DICT = b'd'
TOKEN_END = b'e'
TOKEN_STRING_SEPARATOR = b':'



class Client:
    '''
    Client is the local peer
    '''

    def __init__(self, torrent):
        pass



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
            return TrackerResponse() #####TODO add bencoding here 
            
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

