from download import Run
import time

'''
tracked stats:
- ID
- size magnitude
- downloaded
- status
- size
- remaining
- time began
- time elapsed
- percentage complete
- connected peers
- download speed
- upload speed
- availability
- estimated finish
- categories
'''

class Hypervisor:

    def __init__(self, download: Run, id: int):
        self.download = download
        self.attributes = {
            'id': id,
            'status': None,
            'size': None,
            'size_magnitude': None,
            'downloaded': None,
            'remaining': None,
            'time_began': None,
            'time_elapsed': None,
            'percentage_complete': None,
            'connected_peers': None,
            'download_speed': None,
            'upload_speed': None,
            'availability': None,
            'estimated_finish': None,
            'categories': None,
        }

    def start(self):
        length = self.download.torrent.total_length
        if 1000000 > length:
            self.attributes['size_magnitude'] = [1000, 'KB']
        elif 1000000000 > length > 1000000:
            self.attributes['size_magnitude'] = [1000000, 'MB']
        elif length > 1000000000:
            self.attributes['size_magnitude'] = [1000000000, 'GB']

        self.attributes['size'] = length / self.attributes['size_magnitude'][0]
        self.attributes['remaining'] = length / self.attributes['size_magnitude'][0]
        self.attributes['downloaded'] = ''
        self.attributes['time_began'] = time.time()
        self.attributes['time_elapsed'] = 0.0
        self.attributes['percentage_complete'] = 0.0
        self.attributes['connected_peers'] = 0
        self.attributes['download_speed'] = 0.0
        self.attributes['upload_speed'] = 0.0
        self.attributes['availability'] = 0.0
        
        self.attributes['status'] = 'starting'

        while self.attributes['status'] != 'terminated':
            self.download.run(self.attributes['status'])

            self.downloaded_stats()
            print(f"DOWNLOADED = {self.attributes['downloaded']}")
            
            if not self.download.pieces_manager.all_pieces_completed():
                self.attributes['status'] == 'running'
            else:
                self.attributes['status'] == 'seeding'

            time.sleep(0.1)

    def downloaded_stats(self):
        amount = (self.download.pieces_manager.complete_pieces / self.download.pieces_manager.number_of_pieces) \
                  * self.download.torrent.piece_length
        if 1000000 > amount:
            self.attributes['downloaded'] = f'{amount / 1000} KB'
        elif 1000000000 < amount < 1000000:
            self.attributes['downloaded'] = f'{amount / 1000000} MB'
        elif 1000000000 < amount:
            self.attributes['downloaded'] = f'{amount / 1000000000} GB'

    def calculate_speed(self):
        pass
        