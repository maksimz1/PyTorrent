from piece import Piece
from torrent import Torrent
import typing 
import hashlib

class PieceManager:
    def __init__(self, torrent: Torrent):
        self.pieces: typing.List[Piece] = []
        self.busy_pieces = set()
        self.torrent = torrent
        self.number_of_pieces = torrent.total_pieces

        self._generate_pieces()

        # Load expected hashes from the torrent metadata
        self.expected_hashes = [
            torrent.pieces[i * 20:(i + 1) * 20]
            for i in range(torrent.total_pieces)
        ]

    def recieve_block_piece(self, piece_index, piece_offset, piece_data):
        # piece_index, piece_offset, piece_data = piece
        piece:Piece = self.pieces[piece_index]

        piece.add_block(piece_data)

        if piece.is_complete():
            print("✅ Download completed! Checking validity...")
            # Validate the piece integrity
            if self._validate_piece(piece):
                print(f"✅ Piece {piece_index} completed and verified! Writing to disk...")
                with open(f"file_pieces/{piece_index}.part", 'wb') as f:
                    f.write(piece.raw_data)
                
                # Release the piece after download successful
                self.busy_pieces.remove(piece_index)
            else: 
                print(f"❌ Hash mismatch for piece {piece_index}. Retrying...")
                piece.flush() # Reset piece and redownload
                # Release the piece even if unsuccessful 
                self.busy_pieces.remove(piece_index)
        else:
            print(f"Download not complete :/ {len(piece.raw_data)}")


    def _generate_pieces(self):
        last_piece = self.number_of_pieces - 1
        piece_length = self.torrent.piece_length
        for i in range(self.number_of_pieces):
            start = i * 20
            end = start + 20

            if i == last_piece:
                piece_length = self.torrent.file_length - (self.number_of_pieces - 1) * self.torrent.piece_length

            self.pieces.append(Piece(i, piece_length, self.torrent.pieces[start:end]))
        
    def _validate_piece(self, piece: Piece):
        actual_hash = hashlib.sha1(piece.raw_data).digest()
        return actual_hash == piece.piece_hash
    
    def choose_next_piece(self, peer_bifield = None):
        """
        Selects next piece to download.

        To be ran by the PeerManager when ordering a peer to download a piece
        """
        for piece in self.pieces:
            if piece.is_complete():
                continue
            if piece.piece_index in self.busy_pieces:
                continue
            if peer_bifield is not None:
                if not peer_bifield[piece.piece_index]:
                    continue
            
            self.busy_pieces.add(piece.piece_index)
            return piece.piece_index