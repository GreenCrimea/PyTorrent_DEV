import ipaddress
import struct
import peer
import requests
import logging
import socket
from urllib.parse import urlparse
from bcoding import bdecode
from message import UdpTrackerConnection, UdpTrackerAnnounce, UdpTrackerAnnounceOutput
from peers_manager import PeersManager



MAX_PEERS_TRY_CONNECT = 30
MAX_PEERS_CONNECTED = 8



class SockAddr:
    '''
    x
    '''

    def __init__(self, ip, port, allowed=True):
        self.ip = ip
        self.port = port
        self.allowed = allowed


    def __hash__(self):
        return f'{self.ip}:{self.port}'



class Tracker:
    '''
    x
    '''

    def __init__(self, torrent):
        self.torrent = torrent
        self.threads_list = []
        self.connected_peers = {}
        self.dict_sock_addr = {}


    def get_peers_from_tracker(self):
        for i, tracker in enumerate(self.torrent.announce_list):
            if len(self.dict_sock_addr) >= MAX_PEERS_TRY_CONNECT:
                break

            tracker_url = tracker[0]

            if str.startswith(tracker_url, 'http'):
                try:
                    self.http_scraper(self.torrent, tracker_url)
                except Exception as e:
                    logging.error(f'HTTP scrape failed - {e.__str__()}')

            elif str.startswith(tracker_url, 'udp'):
                try:
                    self.udp_scraper(self.torrent, tracker_url)
                except Exception as e:
                    logging.error(f'HTTP scrape failed - {e.__str__()}')

            else:
                logging.error(f'unknown scheme for {tracker_url}')

        self.try_peer_connect()

        return self.connected_peers


    def try_peer_connect(self):
        logging.info(f'trying to connect to {len(self.dict_sock_addr)} peer(s)')

        for _, sock_addr in self.dict_sock_addr.items():
            if len(self.connected_peers) >= MAX_PEERS_CONNECTED:
                break

            new_peer = peer.Peer(int(self.torrent.number_of_pieces), sock_addr.ip, sock_addr.port)
            if not new_peer.connect():
                continue

            print(f'connected to {len(self.connected_peers)}/{MAX_PEERS_CONNECTED} peers')
            self.connected_peers[new_peer.__hash__()] = new_peer


    def http_scraper(self, torrent, tracker):
        params = {
            'info_hash': torrent.info_hash,
            'peer_id': torrent.peer_id,
            'uploaded': 0,
            'downloaded': 0,
            'port': 6881,
            'left': torrent.total_length,
            'event': 'started'
        }

        try:
            answer_tracker = requests.get(tracker, params=params, timeout=5)
            list_peers = bdecode(answer_tracker.content)
            offset = 0
            if not type(list_peers['peers']) == list:

                for _ in range(len(list_peers['peers']) // 6):
                    ip = struct.unpack_from('!i', list_peers['peers'], offset)[0]
                    ip = socket.inet_ntoa(struct.pack('!i', ip))
                    offset += 4
                    port = struct.unpack_from('!H', list_peers['peers'], offset)[0]
                    offset += 2
                    s = SockAddr(ip, port)
                    self.dict_sock_addr[s.__hash__()] = s

            else:
                for p in list_peers['peers']:
                    s = SockAddr(p['ip'], p['port'])
                    self.dict_sock_addr[s.__hash__()] = s

        except Exception as e:
            logging.exception(f'HTTP scrape failed = {e.__str__()}')


    def udp_scraper(self, announce):
        torrent = self.torrent
        parsed = urlparse(announce)

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(4)
        ip, port = socket.gethostbyname(parsed.hostname), parsed.port

        if ipaddress.ip_address(ip).is_private:
            return

        tracker_connection_input = UdpTrackerConnection()
        response = self.send_message((ip, port), sock, tracker_connection_input)

        if not response:
            raise Exception('no response from UDP tracker connection')

        tracker_connection_output = UdpTrackerConnection()
        tracker_connection_output.from_bytes(response)

        tracker_announce_input = UdpTrackerAnnounce(torrent.info_hash, tracker_connection_output.conn_id, torrent.peer_id)
        response = self.send_message((ip, port), sock, tracker_announce_input)

        if not response:
            raise Exception('no response from UDP tracker announce')

        tracker_announce_output = UdpTrackerAnnounceOutput()
        tracker_announce_output.from_bytes(response)

        for ip, port in tracker_announce_output.list_sock_addr:
            sock_addr = SockAddr(ip, port)

            if sock_addr.__hash__() not in self.dict_sock_addr:
                self.dict_sock_addr[sock_addr.__hash__()] = sock_addr

        print(f'got {len(self.dict_sock_addr)} peers')


    def send_message(self, conn, sock, tracker_message):
        message = tracker_message.to_bytes()
        trans_id = tracker_message.trans_id
        action = tracker_message.action
        size = len(message)

        sock.sendto(message, conn)

        try:
            response = PeersManager._read_from_socket(sock)
        except socket.timeout as e:
            logging.debug(f'timeout : {e}')
            return
        except Exception as e:
            logging.exception(f'undexpected error when sending message - {e.__str__()}')
            return
        
        if len(response) < size:
            logging.debug('did not get full message')

        if action != response[0:4] or trans_id != response[4:8]:
            logging.debug('transaction or action id did not match')

        return response