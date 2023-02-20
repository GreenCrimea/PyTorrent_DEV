import sys
import time
import peers_manager
import pieces_manager
import torrent
import tracker
import logging
import os
import message
from block import State



class Run:
    '''
    x
    '''
    percentage_complete = -1
    last_log_line = ''

    def __init__(self):
        try:
            torrent_file = sys.argv[1]
        except IndexError:
            logging.error('no torrent provided')
            sys.exit(0)

        self.torrent = torrent.Torrent().load_from_path(torrent_file)
        self.tracker = tracker.Tracker(self.torrent)
        self.pieces_manager = pieces_manager.PiecesManager(self.torrent)
        self.peers_manager = peers_manager.PeersManager(self.torrent, self.pieces_manager)

        self.peers_manager.start()
        logging.info('PeersManager started')
        logging.info('piecesManager Started')


    def start(self):
        peers_dict = self.tracker.get_peers_from_tracker()
        self.peers_manager.add_peers(peers_dict.values())

        while not self.peers_manager.all_pieces_completed():
            if not self.peers_manager.has_unchoked_peers():
                time.sleep(1)
                logging.info('no unchoked peers')
                continue

            for piece in self.pieces_manager.pieces:
                index = piece.piece_index

                if self.pieces_manager.pieces[index].is_full:
                    continue

                peer = self.peers_manager.get_random_peer_having_piece(index)
                if not peer:
                    continue

                self.pieces_manager.pieces[index].update_block_status()

                data = self.pieces_manager.pieces[index].get_empty_block()
                if not data:
                    continue
                    
                piece_index, block_offset, block_length = data
                piece_data = message.Request(piece_index, block_offset, block_length).to_bytes()
                peer.send_to_peer(piece_data)

            self.display_progression()
            time.sleep(0.1)

        logging.info("file(s) downloaded sucessfully")
        self.display_progression()

        self._exit_threads()


    def display_progression(self):
        new_progression = 0

        for i in range(self.pieces_manager.number_of_pieces):
            for j in range(self.pieces_manager.pieces[i].number_of_blocks):
                if self.pieces_manager.pieces[i].blocks[j].state == State.FULL:
                    new_progression += len(self.pieces_manager.pieces[i].blocks[j].data)

        if new_progression ==self.percentage_complete:
            return

        number_of_peers = self.peers_manager.unchoked_peers_count()
        percentage_complete = float((float(new_progression) / self.torrent.total_length) * 100)

        current_log_line = f'Connected peers: {number_of_peers} - {round(percentage_complete, 2)}% complete | {self.pieces_manager.complete_pieces}/{self.pieces_manager.number_of_pieces} pieces'

        if current_log_line != self.last_log_line:
            print(current_log_line)

        self.last_log_line = current_log_line
        self.percentage_complete = new_progression

    
    def _exit_threads(self):
        self.peers_manager.is_active = False
        os.exit(0)



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    run = Run()
    run.start()