import asyncio
import logging
from struct import pack, unpack
from asyncio import Queue
from concurrent.futures import CancelledError





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