import asyncio
import logging
from struct import pack, unpack
from asyncio import Queue
from concurrent.futures import CancelledError



REQUEST_SIZE = 2**14



class ProtocolError(BaseException):
    pass



class PeerConnection:
    '''
    a connection used to download and upload pieces

    will take a peer from the queue and attemnt to open connection and handshake.
    after handshake this will be choked, and an interested message is sent.
    after interested message is sent this is unchoked, and can request pieces.
    '''
    def __init__(self, queue: Queue, info_hash, peer_id, piece_manager, on_block_cb=None):
        '''
        creates a peer connection and adds it to the asyncio event loop.

        :param queue: The async Queue containing available peers
        :param info_hash: The SHA1 hash for the meta-data's info
        :param peer_id: Our peer ID used to to identify ourselves
        :param piece_manager: The manager responsible to determine which pieces
        to request
        :param on_block_cb: The callback function to call when a block is
        received from the remote peer
        '''
        self.my_state = []
        self.peer_state = []
        self.queue = queue
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.remote_id = None
        self.writer = None
        self.reader = None
        self.piece_manager = piece_manager
        self.on_block_cb = on_block_cb
        self.future = asyncio.ensure_future(self._start())

    async def _start(self):
        while 'stopped' not in self.my_state:
            ip, port = await self.queue.get()
            logging.info(f'assigned peer with: {ip}')

            try:
                self.reader, self.writer = await asyncio.open_connection(ip, port)
                logging.info(f'connected to peer: {ip}')

                buffer = await self._handshake()

                #####TODO support for sending data



class PeerMessage:
    '''
    a message between peers.

    takes the form of:
        <length prefix><message ID><payload>

    length prefix is a 4 byte big endian value, ID is a single 
    decimal byte, and payload depends on the message type
    ''' 
    Choke = 0
    Unchoke = 1
    Interested = 2
    NotInterested = 3
    Have = 4
    BitField = 5
    Request = 6
    Piece = 7
    Cancel = 8
    Port = 9
    Handshake = None
    KeepAlive = None

    def encode(self) -> bytes:
        '''
        Encode this object to raw bytes representing the message
        '''
        pass

    @classmethod
    def decode(cls, data: bytes):
        '''
        Decodes the message into a object inst 
        '''
        pass



class Piece(PeerMessage):
    '''
    called 'piece' to match spec, but really represents a 'block'.

    Message format:
        <length prefix><message ID><index><begin><block>
    '''
    length = 9

    def __init__(self, index: int, begin: int, block: bytes):
        '''
        create a piece message

        :param index: The zero based piece index
        :param begin: The zero based offset within a piece
        :param block: The block data
        '''
        self.index = index
        self.begin = begin
        self.block = block

    def encode(self):
        message_length = Piece.length + len(self.block)
        return pack('>IbII' + str(len(self.block)) + 's',
                    message_length,
                    PeerMessage.Piece,
                    self.index,
                    self.begin,
                    self.block)

    @classmethod
    def decode(cls, data: bytes):
        logging.debug(f'Decoding piece of length {len(data)}')
        length = unpack('>I', data[:4])[0]
        parts = unpack('>IbII' + str(length - Piece.length) + 's',
                       data[:length + 4])
        return cls(parts[2], parts[3], parts[4])

    def __str__(self):
        return 'Piece'