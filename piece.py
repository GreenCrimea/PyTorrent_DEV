import hashlib
import math
import time
import logging
from pubsub import pub
from block import Block, BLOCK_SIZE, State



class Piece:
    '''
    x
    '''

    def __init__(self, piece_index: int, piece_size: int, piece_hash: str):
        self.piece_index = piece_index
        self.piece_size = piece_size
        self.piece_hash = piece_hash
        self.is_full = False
        self.files = []
        self.raw_data = b''
        self.number_of_blocks = int(math.ceil(float(piece_size) / BLOCK_SIZE))
        self.blocks: list[Block] = []

        self._init_blocks()


    def update_block_status(self):
        for i, block in enumerate(self.blocks):
            if block.state == State.PENDING and (time.time() - block.last_seen) > 5:
                self.blocks[i] = Block()


    def set_block(self, offset, data):
        index = int(offset / BLOCK_SIZE)

        if not self.is_full and not self.blocks[index].state == State.FULL:
            self.blocks[index].data = data
            self.blocks[index].state = State.FULL


    def get_block(self, block_offset, block_length):
        return self.raw_data[block_offset:block_length]


    def get_empty_block(self):
        if self.is_full:
            return None

        for block_index, block in enumerate(self.blocks):
            if block.state == State.FREE:
                self.blocks[block_index].state = State.PENDING
                self.blocks[block_index].last_seen = time.time()
                return self.piece_index, block_index * BLOCK_SIZE, block.block_size

        return None


    def are_all_blocks_full(self):
        for block in self.blocks:
            if block.state == State.FREE or block.state == State.PENDING:
                return False

        return True


    def set_to_full(self):
        data = self._merge_blocks()

        if not self._valid_blocks(data):
            self._init_blocks()
            return False

        self.is_full = True
        self.raw_data = data
        self._write_piece_on_disk()
        pub.sendMessage('PiecesManager.PieceCompleted', piece_index=self.piece_index)
        return True


    def _init_blocks(self):
        self.blocks = []

        if self.number_of_blocks > 1:
            for i in range(self.number_of_blocks):
                self.blocks.append(Block())

            if (self.piece_size % BLOCK_SIZE) > 0:
                self.blocks[self.number_of_blocks - 1].block_size = self.piece_size % BLOCK_SIZE

        else:
            self.blocks.append(Block(block_size=int(self.piece_size)))


    def _write_piece_on_disk(self):
        for file in self.files:
            path_file = file['path']
            