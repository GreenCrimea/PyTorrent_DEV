import asyncio
import logging
import bitstring
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

                #initiate handshake
                buffer = await self._handshake()

                #####TODO support for sending data
                #begin connection choked
                self.my_state.append('choked')

                #tell peer interested
                await self._send_interested()
                self.my_state.append('interested')

                #read responses as a stream of messages while connection open
                async for message in PeerStreamIterator(self.reader, buffer):
                    if 'stopped' in self.my_state:
                        break
                    if type(message) is BitField:
                        self.piece_manager.add_peer(self.remote_id, message.bitfield)
                    elif type(message) is Interested:
                        self.peer_state.append('interested')
                    elif type(message) is NotInterested:
                        if 'interested' in self.peer_state:
                            self.peer_state.remove('interested')
                    elif type(message) is Choke:
                        self.my_state.append('choked')
                    elif type(message) is Unchoke:
                        if 'choked' in self.my_state:
                            self.my_state.remove('choked')
                    elif type(message) is Have:
                        self.piece_manager.update_peer(self.remote_id, message.index)
                    elif type(message) is KeepAlive:
                        pass
                    elif type(message) is Piece:
                        self.my_state.remove('pending_request')
                        self.on_block_cb(
                            peer_id=self.remote_id,
                            piece_index=message.index,
                            block_offset=message.begin,
                            data=message.block
                        )
                    elif type(message) is Request:
                        #####TODO sending data
                        logging.info('ignoring request message')
                    elif type(message) is Cancel:
                        logging.info('ignoring cancel messsage')

                    if 'choked' not in self.my_state:
                        if 'interested' in self.my_state:
                            if 'pending_request' not in self.my_state:
                                self.my_state.append('pending_request')
                                await self._request_piece()

            except ProtocolError as e:
                logging.exception('protocol error')
            except (ConnectionRefusedError, TimeoutError):
                logging.warning('unable to connect to peer')
            except (ConnectionResetError, CancelledError):
                logging.warning('connection closed')
            except Exception as e:
                logging.exception('error with peer connection')
                self.cancel()
                raise e
            self.cancel()

    def cancel(self):
        '''
        sends the cancel message and close connection
        '''
        logging.info(f'closing peer {self.remote_id}')
        if not self.future.done():
            self.future.cancel()
        if self.writer:
            self.writer.close()

        self.queue.task_done()

    def stop(self):
        '''
        stop the connection from current peer and stop any new peer connections
        '''
        self.my_state.append('stopped')
        if not self.future.done():
            self.future.cancel()

    async def _request_piece(self):
        block = self.piece_manager.next_request(self.remote_id)
        if block:
            message = Request(block.piece, block.offset, block.length).encode()
            logging.debug(f'requesting block {block.offset} for piece {block.piece}' \
                          f' of length {block.length} fom peer {self.remote_id}')
        self.writer.write(message)
        await self.writer.drain()

    async def _handshake(self):
        '''
        send the handshake and wait for the handshake return
        '''
        self.writer.write(Handshake(self.info_hash, self.peer_id).encode())
        await self.writer.drain()

        buf = b''
        tries = 1
        while len(buf) < Handshake.length and tries < 10:
            tries += 1
            buf = await self.reader.read(PeerStreamIterator.CHUNK_SIZE)

        response = Handshake.decode(buf[:Handshake.length])
        if not response:
            raise ProtocolError('unable to recieve and parse handshake')
        if not response.info_hash == self.info_hash:
            raise ProtocolError('Handshake returned invalid hash')

        #####TODO validate peer id is same as recieved from tracker
        self.remote_id = response.peer_id
        logging.info('handshake with peer was successful')
        return buf[Handshake.length:]

    async def _send_interested(self):
        message = Interested()
        logging.debug(f'sending message: {message}')
        self.writer.write(message.encode())
        await self.writer.drain()



class PeerStreamIterator:
    '''
    async iterator that continuously reads from the reader, and finds valid protocol
    messages.
    '''
    CHUNK_SIZE = 10*1024

    def __init__(self, reader, initial: bytes=None):
        self.reader = reader
        self.buffer = initial if initial else b''

    async def __aiter__(self):
        return self

    async def __anext__(self):
        '''
        read data from the socket, and parse if there is enough
        '''
        while True:
            try:
                data = await self.reader.read(PeerStreamIterator.CHUNK_SIZE)
                if data:
                    self.buffer += data
                    message = self.parse()
                    if message:
                        return message
                
                else:
                    logging.debug('no data read from stream')
                    if self.buffer:
                        message = self.parse()
                        if message:
                            return message
                    raise StopAsyncIteration()

            except ConnectionResetError:
                logging.debug('connection closed by peer')
                raise StopAsyncIteration()

            except CancelledError:
                raise StopAsyncIteration()

            except StopAsyncIteration as e:
                raise e

            except Exception:
                logging.exception('Error when iterating stream')
                raise StopAsyncIteration()
        
    def parse(self):
        '''
        tries to parse protocol messages out of buffer.

        messages are structed as:
            <length prefix><message ID><payload>

        length prefix is a 4 byte big-endian value
        message id is a decimal value
        payload is the value of length prefix

        the length prefix is not counted as part of the actual length
        '''
        header_length = 4

        if len(self.buffer) > 4:
            message_length = unpack('>I', self.buffer[0:4])[0]

            if message_length == 0:
                return KeepAlive()

            if len(self.buffer) >= message_length:
                message_id = unpack('>b', self.buffer[4:5])[0]

                def _consume():
                    self.buffer = self.buffer[header_length + message_length:]

                def _data():
                    return self.buffer[:header_length + message_length]

                if message_id is PeerMessage.BitField:
                    data = _data()
                    _consume()
                    return BitField.decode(data)
                
                elif message_id is PeerMessage.Interested:
                    _consume()
                    return Interested()

                elif message_id is PeerMessage.NotInterested:
                    _consume()
                    return NotInterested()

                elif message_id is PeerMessage.Choke:
                    _consume()
                    return Choke()

                elif message_id is PeerMessage.Unchoke:
                    _consume()
                    return Unchoke()

                elif message_id is PeerMessage.Have:
                    data = _data()
                    _consume()
                    return Have.decode(data)

                elif message_id is PeerMessage.Piece:
                    data = _data()
                    _consume()
                    return Request.decode(data)

                elif message_id is PeerMessage.Cancel:
                    data = _data()
                    _consume()
                    return Cancel.decode(data)
                else:
                    logging.info('unsupported message recieved')
            else:
                logging.debug('not enough in buffer to parse')
        return None



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



class Handshake(PeerMessage):
    '''
    handshake message sent first then recieved back from peer.

    always 68 bytes long. message format:
        <pstrlen><pstr><reserved><info_hash><peer_id>

    in bittorrent V1:
    pstrlen = 19
    pstr = "Bittorent Protocol"
    '''
    length = 68

    def __init__(self, info_hash: bytes, peer_id: bytes):
        '''
        construct handshake message
        '''
        if isinstance(info_hash, str):
            info_hash = info_hash.encode('utf-8')
        if isinstance(peer_id, str):
            peer_id = peer_id.encode('utf-8')
        self.info_hash = info_hash
        self.peer_id = peer_id

    def encode(self) -> bytes:
        '''
        encode object as raw bytes ready to be transmitted
        '''
        return pack(
            '>B19s8x20s20s',
            19,                         #single byte
            b'BitTorrent protocol',     #string 19s
                                        #8x pad
            self.info_hash,             #string 20s
            self.peer_id                #string 20s
        )

    @classmethod
    def decode(cls, data: bytes):
        '''
        decodes handshake message
        '''
        logging.debug(f'decoding handshake of length {len(data)}')
        if len(data) < 68:
            return None
        parts = unpack('>B19s8x20s20s', data)
        return cls(info_hash=parts[2], peer_id=parts[3])

    def __str__(self):
        return 'Handshake'



class KeepAlive(PeerMessage):
    '''
    keepalive has no payload and len 0
    '''
    def __str__(self):
        return 'KeepAlive'



class BitField(PeerMessage):
    '''
    bitfield is variable length where the payload is a bit array representing
    all the bits a peer has (1) or doesnt have (0)

    message format:
        <len=0001+X><id=5><bitfield>
    '''      
    def __init__(self, data):
        self.bitfield = bitstring.BitArray(bytes=data)

    def encode(self) -> bytes:
        bits_length = len(self.bitfield)
        return pack('>Ib' + str(bits_length) + 's',
                        1 + bits_length,
                        PeerMessage.BitField,
                        self.bitfield)

    @classmethod
    def decode(cls, data: bytes):
        message_length = unpack('>I', data[:4])[0]
        logging.debug(f'decoding bitfield of length {message_length}')
        parts = unpack('>Ib' + str(message_length - 1) + s, data)
        return cls(parts[2])

    def __str__(self):
        return 'BitField'



class Interested(PeerMessage):
    '''
    interested message is fixed length and has no payoad. used to 
    notify interest in downloading pieces.

    message format:
        <len=0001><id=2>
    '''

    def encode(self) -> bytes:
        return pack('Ib', 1, PeerMessage.Interested)

    def __str__(self):
        return 'Interested'



class NotInterested(PeerMessage):
    '''
    notinterested message is fixed length and has no payoad. used to 
    notify no interest in downloading pieces.

    message format:
        <len=0001><id=3>
    '''

    def __str__(self):
        return 'NotInterested'



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