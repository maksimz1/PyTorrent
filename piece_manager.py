from piece import Piece
from torrent import Torrent
import typing 
import hashlib
import os

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

        piece.add_block(piece_offset, piece_data)

        if piece.is_complete():
            print("✅ Download completed! Checking validity...")
            # Validate the piece integrity
            if self._validate_piece(piece):
                print(f"✅ Piece {piece_index} completed and verified! Writing to disk...")
                with open(f"file_pieces/{piece_index}.part", 'wb') as f:
                    f.write(piece.raw_data)
                self.busy_pieces.remove(piece_index)
                
            else: 
                print(f"❌ Hash mismatch for piece {piece_index}. Retrying...")
                piece.flush() # Reset piece and redownload
                # Release the piece even if unsuccessful 
                self.busy_pieces.remove(piece_index)
        else:
            # print(f"Download not complete, current data:{len(piece.raw_data)}")
            pass


    def _generate_pieces(self):
        piece_length = self.torrent.piece_length

        # If the entire file is smaller than one piece, treat it as one piece.
        if self.torrent.file_length < piece_length:
            self.number_of_pieces = 1
            self.pieces.append(Piece(0, self.torrent.file_length, self.torrent.pieces[:20]))
            return

        # Otherwise, calculate the last piece length.
        last_piece_length = self.torrent.file_length - (self.number_of_pieces - 1) * piece_length
        if last_piece_length <= 0:
            raise ValueError(f"Inconsistent torrent metadata: calculated last piece length {last_piece_length} is not positive.")

        for i in range(self.number_of_pieces):
            start = i * 20
            end = start + 20
            # Use the standard piece_length for all pieces except the last.
            current_length = piece_length if i < self.number_of_pieces - 1 else last_piece_length
            self.pieces.append(Piece(i, current_length, self.torrent.pieces[start:end]))


        
    def _validate_piece(self, piece: Piece):
        actual_hash = hashlib.sha1(piece.raw_data).digest()
        return actual_hash == piece.piece_hash
    
    def is_piece_downloaded(self, piece: Piece) -> bool:
        """
        Checks if the file {index}.part exists, and if it exists, verify the contents
        """
        piece_path = f"file_pieces/{piece.piece_index}.part"
        if not os.path.exists(piece_path):
            return False
        
        with open(piece_path, 'rb') as f:
            data = f.read()

        if len(data) != piece.piece_length:
            return False

        if hashlib.sha1(data).digest() != piece.piece_hash:
            return False

        return True

    def choose_next_piece(self, peer_bifield = None):
        """
        Selects next piece to download.

        To be ran by the PeerManager when ordering a peer to download a piece
        """
        for piece in self.pieces:
            if piece.is_complete():
                continue
            if self.is_piece_downloaded(piece):
                continue
            if piece.piece_index in self.busy_pieces:
                continue
            if peer_bifield is not None:
                if not peer_bifield[piece.piece_index]:
                    continue
            
            self.busy_pieces.add(piece.piece_index)
            return piece.piece_index

    def release_piece(self, busy_piece_index):
        self.busy_pieces.discard(busy_piece_index)