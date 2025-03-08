class Piece:
    def __init__(self, piece_index: int, piece_length: int, piece_hash):
        self.piece_index = piece_index
        self.piece_length = piece_length
        self.piece_hash = piece_hash
        # Preallocate a buffer for the piece
        self.raw_data = bytearray(piece_length)
        # Keep track of received blocks by offset (optional, for completeness checking)
        self.received = {}

    def add_block(self, offset: int, data: bytes):
        # Write the block at the correct offset
        self.raw_data[offset:offset+len(data)] = data
        self.received[offset] = len(data)

    def is_complete(self):
        # Check if the sum of received block lengths equals piece_length
        total = sum(self.received.values())
        return total >= self.piece_length

    def flush(self):
        self.raw_data = bytearray(self.piece_length)
        self.received = {}
