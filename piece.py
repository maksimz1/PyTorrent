class Piece:
    def __init__(self, piece_index: int, piece_length: int, piece_hash):
        self.piece_index: int = piece_index
        self.piece_length: int = piece_length
        self.piece_hash: bytes = piece_hash
        self.raw_data: bytes = b'' # Store downloaded data

    def add_block(self, data):
        """Store a block"""
        self.raw_data += data

    def is_complete(self):
        return len(self.raw_data) == self.piece_length
    
    def flush(self):
        """
        Reset the data in case of a download failure
        """
        self.raw_data = b''
