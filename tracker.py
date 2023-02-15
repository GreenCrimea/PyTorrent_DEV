


import aiohttp
import string
import random
import logging
import socket
from struct import unpack
from urllib.parse import urlencode

import bencoding



#software version
VERSION = '0.0.1'




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
            return TrackerResponse(bencoding.Decoder(data).decode())
            
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

    def _construct_tracker_parameters(self):
        '''
        constructs the URL params for issuing the announce call to tracker
        '''
        return {
            'info_hash': self.torrent.info_hash,
            'peer_id': self.peer_id,
            'port': 6889,
            #####TODO communicate with tracker
            'uploaded': 0,
            'downloaded': 0,
            'left': 1,
            'compact': 0
        }

    def close(self):
        self.http_client.close()



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

