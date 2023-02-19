import math
import hashlib
import random
import string
from bcoding import bencode, bdecode
import logging
import os



VERSION = '0.0.1'



class Torrent(object):
    '''
    base class for the decoded torrent file
    '''

    def __init__(self):
        self.torrent_file = {}
        self.total_length: int = 0
        self.piece_length: int = 0
        self.pieces: int = 0
        self.info_hash: str = ''
        self.peer_id: str = ''
        self.announce_list = ''
        self.file_names = []
        self.number_of_pieces: int = 0


    def load_from_path(self, path):
        '''
        decode the torrent file and initialise the object
        '''
        with open(path, 'rb') as file:
            contents = bdecode(file)

        self.torrent_file = contents
        self.piece_length = self.torrent_file['info']['piece length']
        self.pieces = self.torrent_file['info']['pieces']
        raw_info_hash = bencode(self.torrent_file['info'])
        self.info_hash = hashlib.sha1(raw_info_hash).digest()
        self.peer_id = self.generate_peer_id()
        self.announce_list = self.get_trackers()
        self.init_files()
        self.number_of_pieces = math.ceil(self.total_length / self.piece_length)
        logging.debug(self.announce_list)
        logging.debug(self.file_names)

        assert(self.total_length > 0)
        assert(len(self.file_names) > 0)

        return self


    def init_files(self):
        '''
        determine if this is a single or multifile torrent,
        and if it is a multifile, create the directory structure
        '''
        root = self.torrent_file['info']['nane']

        if 'files' in self.torrent_file['info']:
            if not os.path.exists(root):
                os.mkdir(root, 0o0766)

            for file in self.torrent_file['info']['files']:
                path_file = os.path.join(root, *file['path'])

                if not os.path.exists(os.path.dirname(path_file)):
                    os.makedirs(os.path.dirname(path_file))
                
                self.file_names.append({'path': path_file, 'length': file['length']})
                self.total_length += file['length']

        else:
            self.file_names.append({'path': root, 'length': self.torrent_file['info']['length']})
            self.total_length = self.torrent_file['info']['length']


    def get_trackers(self):
        '''
        return a list of trackers if there is a list contained
        in the torrent file, else return the single tracker
        '''
        if 'announce-list' in self.torrent_file:
            return self.torrent_file['announce-list']
        else:
            return [[self.torrent_file['announce']]]


    def generate_peer_id(self):
        '''
        Create a unique peer id to be sent to the tracker
        peer id is 20 bytes, and structured = 

        'pTor-{3 byte version number truncating least sig bits}-0x{9 random bytes using 0-9 and A-F}'

        'pTor-001-0x4F9A10CCB'
        '''
        return f'pTor-{"".join([x[0] for x in VERSION.split(".")])}-0x' + ''.join(
            [random.choice(string.hexdigits.upper()) for _ in range(9)]
        )

    