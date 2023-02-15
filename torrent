


from hashlib import sha1
from collections import namedtuple

from . import bencoding



TorrentFile = namedtuple('TorrentFile', ['name', 'length'])



class Torrent:
    '''
    wraps the bencoded .torrent file data with utility functions
    '''
    def __init__(self, filename):
        self.filename = filename
        self.files = []

        with open(self.filename, 'rb') as f:
            meta_info = f.read()
            self.meta_info = bencoding.Decoder(meta_info).decode()
            info = bencoding.Encoder(self.meta_info[b'info']).encode()
            self.info_hash = sha1(info).digest()
            self._identify_files()

    def _indentify_files(self):
        '''
        identifies the files in the .torrent
        '''
        if self.multi_file:
            #####TODO multi file support
            raise RuntimeError('Multifile not supported')
        self.files.append(
            TorrentFile(
                self.meta_info[b'info'][b'name'].decode('utf-8'),
                self.meta_info[b'info'][b'length']
            )
        )

    @property
    def announce(self) -> str:
        '''
        announce URL for tracker
        '''
        return self.meta_info[b'announce'].decode('utf-8')

    @property
    def multi_file(self) -> bool:
        '''
        are there multiple files
        '''
        return b'files' in self.meta_info[b'info']

    @property
    def piece_length(self) -> int:
        '''
        length in bytes of each piece
        '''
        return self.meta_info[b'info'][b'piece_length']

    @property
    def total_size(self) -> int:
        '''
        total size in bytes for the files
        '''
        if self.multi_file:
            raise RuntimeError('multifile not supported')
        return self.files[0].length

    @property
    def pieces(self):
        '''
        20 byte long sha1 hashes for each piece
        '''
        data = self.meta_info[b'info'][b'pieces']
        pieces = []
        offset = 0
        length = len(data)

        while offset < length:
            pieces.append(data[offset:offset + 20])
            offset += 20
        return pieces

    @property
    def output_file(self):
        return self.meta_info[b'info'][b'name'].decode('utf-8')

    def __str__(self):
        return f'Filename: {self.meta_info[b"info"][b"name"]}\n' \
               f'File Length: {self.meta_info[b"info"][b"length"]}\n' \
               f'Announce URL: {self.meta_info[b"announce"]}\n' \
               f'Hash: {self.info_hash}'  