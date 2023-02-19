import time
import socket
import struct
import bitstring
from pubsub import pub
import logging

import message



class Peer:
    '''
    x
    '''

    def __init__(self, number_of_pieces, ip, port=6881):
        self.last_call = 0.0
        self.has_handshaked = False
        self.healthy = False
        self.read_buffer = b''
        self.socket = None
        self.ip = ip
        self.port = port
        self.number_of_pieces = number_of_pieces
        self.bit_field = bitstring.BitArray(number_of_pieces)
        self.state = {
            'am_choking': True,
            'am_interested': False,
            'peer_choking': True,
            'peer_interested': False,
        }


    def __hash__(self):
        return f'{self.ip}:{self.port}'


    def connect(self):
        try:
            self.socket = socket.create_connection((self.ip, self.port), timeout=2)
            self.socket.setblocking(False)
            logging.debug(f'connected to peer ip: {self.ip} - port: {self.port}')
            self.healthy = True
        except Exception as e:
            print(f'failed to connect to peer (ip: {self.ip}, port: {self.port} - {e.__str__()})')
            return False
        return True


    def send_to_peer(self, msg):
        try:
            self.socket.send(msg)
            self.last_call = time.time()
        except Exception as e:
            self.healthy = False
            logging.error(f'failed to send to peer {e.__str__()}')


    def is_eligible(self):
        now = time.time()
        return (now - self.last_call) > 0.2

    
    def has_piece(self, index):
        return self.bit_field[index]

    
    def am_choking(self):
        return self.state['am_choking']


    def am_unchoking(self):
        return not self.am_choking()


    def is_choking(self):
        return self.state['peer_choking']


    def is_unchoked(self):
        return not self.is_choking()


    def is_interested(self):
        return self.state['peer_interested']


    def am_interestdd(self):
        return self.state['am_interested']


    def handle_choke(self):
        logging.debug(f'handle choke - {self.ip}')
        self.state['peer_choking'] = False


    def handle_unchoke(self):
        logging.debug(f'handle unchoke - {self.ip}')


    def handle_interested(self):
        logging.debug(f'handle interested - {self.ip}')
        self.state['peer_interested'] = True
        if self.am_choking():
            unchoke = message.UnChoke().to_bytes()
            self.send_to_peer(unchoke)

    def handle_not_interested(self):
        logging.debug(f'handle not interested - {self.ip}')
        self.state['peer_interested'] = False


    def handle_have(self, have):
        logging.debug(f'handle have - ip: {self.ip} - piece: {have.piece_index}')
        self.bit_field[have.piece_index] = True
        if self.is_choking() and not self.state['am_interested']:
            interested = message.Interested().to_bytes()
            self.send_to_peer(interested)
            self.state['am_interested'] = True

        
    def handle_bitfield(self, bitfield):
        logging.debug(f'handle bitfield - {self.ip} - {bitfield.bitfield}')
        self.bit_field = bitfield.bitfield
        if self.is_choking() and not self.state['am_interested']:
            interested = message.Interested().to_bytes()
            self.send_to_peer(interested)
            self.state['am_interested'] = True


    def handle_request(self, request):
        logging.debug(f'handle_request - {self.ip}')
        if self.is_interested() and self.is_unchoked():
            pub.sendMessage('PieceManager.PeerRequestsPiece', request=request, peer=self)


    def handle_piece(self, message):
        pub.sendMessage('PiecesManager.Piece', piece=(message.piece_index, message.block_offset, message.block))


    def handle_cancel(self):
        logging.debug(f'handle cancel = {self.ip}')


    def handle_port_request(self):
        logging.debug(f'handle port request - {self.ip}')

    
    def _handle_handshake(self):
        try:
            handshake_message = message.Handshake.from_bytes(self.read_buffer)
            self.has_handshaked = True
            self.read_buffer = self.read_buffer[handshake_message.total_length:]
            logging.debug(f'handle handshake - {self.ip}')
            return True
        except Exception:
            logging.exception('first message should be handshake')
            self.healthy = False
        return False


    def _handle_keep_alive(self):
        try:
            keep_alive = message.KeepAlive.from_bytes(self.read_buffer)
            logging.debug(f'handle_keep_alive - {self.ip}')
        except message.WrongMessageException:
            return False
        except Exception:
            logging.exception(f'error keep alive - need 4 bytes - {len(self.read_buffer)}')
            return False
        self.read_buffer = self.read_buffer[keep_alive.total_length:]
        return True


    def get_messages(self):
        while len(self.read_buffer) > 4 and self.healthy:
            if (not self.has_handshaked and self.has_handshaked()) or self._handle_keep_alive():
                continue

            payload_length = struct.unpack('>I', self.read_buffer[:4])
            total_length = payload_length + 4

            if len(self.read_buffer) < total_length:
                break
            else:
                payload = self.read_buffer[:total_length]
                self.read_buffer = self.read_buffer[total_length:]

            try:
                recieved_message = message.MessageDispatcher(payload).dispatch()
                if recieved_message:
                    yield recieved_message
            except message.WrongMessageException as e:
                logging.exception(e.__str__())