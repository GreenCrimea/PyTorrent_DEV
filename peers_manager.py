import time
import select
from threading import Thread
from pubsub import pub
import rarest_piece
import logging
import message
import peer 
import errno
import socket
import random



class PeersManager(Thread):
    '''
    x
    '''

    def __init__(self, torrent, pieces_manager):
        Thread.__init__(self)
        self.peers = []
        self.torrent = torrent
        self.pieces_manager = pieces_manager
        self.rarest_pieces = rarest_piece.RarestPiece(pieces_manager)
        self.pieces_by_peer = [[0, []] for _ in range(pieces_manager.number_of_pieces)]
        self.is_active = True

        pub.subscribe(self.peer_requests_piece, 'PeerManager.PeerRequestsPiece')
        pub.subscribe(self.peer_bitfield, 'PeerManager.updatePeersBitfield')