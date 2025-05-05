import os
import hashlib
import random
import time
import bitstring
from typing import List, Dict, Set, Optional, Tuple, Any
from core.torrent import Torrent
from core.piece import Piece

class PieceManager:
    def __init__(self, torrent: Torrent, download_dir: str = "downloads"):
        """Initialize the PieceManager with a torrent file."""
        self.torrent = torrent
        self.download_dir = download_dir
        self.pieces: List[Piece] = []
        self.busy_pieces: Set[int] = set()  # Pieces currently being downloaded
        self.completed_pieces: Set[int] = set()  # Successfully downloaded pieces
        self.failed_attempts: Dict[int, int] = {}  # Track failed attempts per piece
        self.piece_lock_time: Dict[int, float] = {}  # When piece was marked busy
        self.number_of_pieces = torrent.total_pieces
        self.bitfield = bitstring.BitArray(self.number_of_pieces)  # Bitfield to track piece availability

        # Pre-allocate the output file(s)
        self.output_files = {} # Maps file paths to their info

        self._pre_allocate_files()

        # Stats for debugging
        self.stats = {
            "pieces_selected": 0,
            "pieces_completed": 0,
            "pieces_failed": 0,
            "pieces_validated": 0,
            "pieces_invalid": 0,
            "last_completed_time": time.time(),  # Track when the last piece was completed
            "download_rate_pieces": 0  # For tracking download speed
        }
        
        # Load expected hashes from the torrent metadata
        self.expected_hashes = [
            torrent.pieces[i * 20:(i + 1) * 20]
            for i in range(torrent.total_pieces)
        ]
        
        
        
        # Initialize piece objects
        self._generate_pieces()
        
        # Load any already downloaded pieces
        self._load_completed_pieces()
        
        print(f"PieceManager initialized with {self.number_of_pieces} pieces.")
        print(f"Loaded {len(self.completed_pieces)} already completed pieces.")

    def _pre_allocate_files(self):
        """Create empty files of the correct size."""
        if len(self.torrent.files) > 1:
            # Handle multi-file torrent
            base_dir = os.path.join(self.download_dir, self.torrent.name)
            os.makedirs(base_dir, exist_ok=True)
            
            # Create each file
            offset = 0
            for file_info in self.torrent.files:
                # Get file path
                path_parts = file_info['path']
                file_path = os.path.join(base_dir, *path_parts)
                
                # Create parent directories
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                
                # Pre-allocate the file
                with open(file_path, 'wb') as f:
                    f.seek(file_info['length'] - 1)
                    f.write(b'\0')
                
                # Store file info with byte offset
                self.output_files[file_path] = {
                    'length': file_info['length'],
                    'offset': offset
                }
                
                offset += file_info['length']
                
        else:
            # Handle single-file torrent
            file_path = os.path.join(self.download_dir, self.torrent.name)
            
            # Pre-allocate the file
            with open(file_path, 'wb') as f:
                f.seek(self.torrent.file_length - 1)
                f.write(b'\0')
            
            # Store file info
            self.output_files[file_path] = {
                'length': self.torrent.file_length,
                'offset': 0
            }
            
        print(f"Pre-allocated {len(self.output_files)} file(s) for download")

    def _generate_pieces(self):
        """Create Piece objects for all pieces in the torrent with proper length validation."""
        piece_length = self.torrent.piece_length
        file_length = self.torrent.file_length
        
        # Add debug info to help diagnose the issue
        print(f"\nPiece generation details:")
        print(f"- File length: {file_length} bytes")
        print(f"- Piece length: {piece_length} bytes")
        print(f"- Total pieces in torrent: {self.torrent.total_pieces}")
        
        # Validate inputs
        if piece_length <= 0:
            print(f"Warning: Invalid piece length ({piece_length}), using default of 16384 bytes")
            piece_length = 16384  # Default to 16KB if invalid
        
        if file_length <= 0:
            print(f"Warning: Invalid file length ({file_length}), cannot create pieces")
            return
        
        # Calculate the correct number of pieces based on file and piece length
        calculated_pieces = (file_length + piece_length - 1) // piece_length
        if calculated_pieces != self.number_of_pieces:
            print(f"Warning: Expected {calculated_pieces} pieces based on file size, but metadata has {self.number_of_pieces}")
            self.number_of_pieces = calculated_pieces
        
        # Create pieces with proper lengths
        for i in range(self.number_of_pieces):
            # For all pieces except the last one, use the standard piece length
            if i < self.number_of_pieces - 1:
                current_length = piece_length
            else:
                # Calculate the correct length for the last piece
                last_piece_length = file_length - (self.number_of_pieces - 1) * piece_length
                current_length = last_piece_length if last_piece_length > 0 else piece_length
            
            # Ensure we have a hash for this piece
            if i*20 + 20 <= len(self.torrent.pieces):
                piece_hash = self.torrent.pieces[i*20:(i+1)*20]
            else:
                print(f"Warning: Missing hash for piece {i}, using zeros")
                piece_hash = b'\x00' * 20
            
            # Create the piece
            self.pieces.append(Piece(i, current_length, piece_hash))
        
        # Verify all pieces have non-zero length
        zero_length_pieces = [p.piece_index for p in self.pieces if p.piece_length <= 0]
        if zero_length_pieces:
            print(f"Warning: {len(zero_length_pieces)} pieces still have zero length")
        else:
            print(f"Successfully created {len(self.pieces)} pieces with valid lengths")
            
    def _load_completed_pieces(self):
        """Check for already downloaded pieces and mark them as completed."""
        for piece in self.pieces:
            if self.is_piece_downloaded(piece):
                self.completed_pieces.add(piece.piece_index)
                self.bitfield.set(piece.piece_index, 1)
                self.stats["pieces_completed"] += 1

    def recieve_block_piece(self, piece_index: int, piece_offset: int, piece_data: bytes):
        """Process a received block and write it directly to the output file."""
        if piece_index >= self.number_of_pieces:
            print(f"Error: Received invalid piece index {piece_index}")
            return False
            
        # Add to in-memory piece for validation
        piece = self.pieces[piece_index]
        piece.add_block(piece_offset, piece_data)
        
        # If the piece is complete, validate its hash
        if piece.is_complete():
            if self._validate_piece(piece):
                # Mark as complete in bitmap
                self.completed_pieces.add(piece_index)
                self.bitfield[piece_index] = 1
                
                # Write the entire piece to file(s)
                self._write_piece_to_files(piece_index, piece.raw_data)
                
                # Release the piece from memory after writing to file
                self.release_piece(piece_index, failed=False)
                self.stats["pieces_completed"] += 1
                
                # Save progress
                self._save_progress()
                
                # Calculate and display progress
                progress = self.get_progress()
                print(f"✅ Piece {piece_index} validated and saved. Progress: {progress:.2f}%")
                
                return True
            else:
                # Hash mismatch, mark for re-download
                print(f"❌ Hash mismatch for piece {piece_index}. Retrying...")
                piece.flush()
                self.busy_pieces.discard(piece_index)
                self.stats["pieces_invalid"] += 1
                
        return False

    def _write_piece_to_files(self, piece_index, piece_data):
        """Write a complete piece to its correct position in the output file(s)."""
        # Calculate the absolute byte offset in the torrent
        piece_offset = piece_index * self.torrent.piece_length
        piece_length = len(piece_data)
        
        # Determine which file(s) this piece belongs to
        remaining_bytes = piece_length
        current_offset = 0
        
        # Sort files by offset for consistent ordering
        sorted_files = sorted(self.output_files.items(), key=lambda x: x[1]['offset'])
        
        for file_path, file_info in sorted_files:
            file_start = file_info['offset']
            file_end = file_start + file_info['length']
            
            # Check if this piece overlaps with the current file
            if piece_offset + current_offset < file_end and piece_offset + current_offset + remaining_bytes > file_start:
                # Calculate overlap
                overlap_start = max(0, file_start - (piece_offset + current_offset))
                overlap_end = min(remaining_bytes, file_end - (piece_offset + current_offset))
                overlap_length = overlap_end - overlap_start
                
                # Calculate file write position
                write_pos = max(0, (piece_offset + current_offset + overlap_start) - file_start)
                
                # Get the portion of data to write
                data_slice = piece_data[overlap_start:overlap_end]
                
                # Write to file
                try:
                    with open(file_path, 'r+b') as f:
                        f.seek(write_pos)
                        f.write(data_slice)
                except Exception as e:
                    print(f"Error writing to {file_path}: {e}")
                
                # Update tracking variables
                current_offset += overlap_length
                remaining_bytes -= overlap_length
                
                # Exit if we've written all data
                if remaining_bytes <= 0:
                    break

    def _save_progress(self):
        """Save current download progress to a file."""
        progress_path = os.path.join(self.download_dir, f"{self.torrent.name}.progress")
        
        try:
            with open(progress_path, 'wb') as f:
                f.write(self.bitfield.tobytes())
        except Exception as e:
            print(f"Error saving progress: {e}")

    def _validate_piece(self, piece: Piece) -> bool:
        """Validate piece data against expected hash."""
        actual_hash = hashlib.sha1(piece.raw_data).digest()
        return actual_hash == piece.piece_hash

    def is_piece_downloaded(self, piece: Piece) -> bool:
        """Check if a piece has already been successfully downloaded to disk."""
        return piece.piece_index in self.completed_pieces

    def choose_next_piece(self, peer_bitfield=None):
        """
        Select the next piece to download based on availability and rarity.
        Includes validation to prevent selecting invalid pieces.
        """
        # Identify all candidate pieces
        candidates = []
        
        for piece_index in range(len(self.pieces)):
            piece = self.pieces[piece_index]
            
            # Skip pieces with invalid length
            if piece.piece_length <= 0:
                continue
            
            # Skip completed pieces
            if piece_index in self.completed_pieces:
                continue
                
            # Skip busy pieces
            if piece_index in self.busy_pieces:
                continue
                
            # Check if peer has this piece
            if peer_bitfield is not None:
                # Handle different bitfield formats
                has_piece = False
                
                # Handle case where bitfield is shorter than piece count
                if piece_index >= len(peer_bitfield):
                    continue
                
                if hasattr(peer_bitfield, 'bin'):  # BitArray format
                    if piece_index < len(peer_bitfield.bin):
                        has_piece = peer_bitfield.bin[piece_index] == '1'
                else:  # List/array format
                    has_piece = bool(peer_bitfield[piece_index])
                    
                if not has_piece:
                    continue
            
            # Add to candidates with a priority score
            priority = 1.0
            
            # Reduce priority for pieces with previous failures
            if piece_index in self.failed_attempts:
                failures = self.failed_attempts[piece_index]
                priority *= (0.8 ** failures)  # Exponential backoff
            
            candidates.append((piece_index, priority))
        
        if not candidates:
            # Release any pieces locked for too long (over 2 minutes)
            current_time = time.time()
            for piece_index, lock_time in list(self.piece_lock_time.items()):
                if current_time - lock_time > 120:  # 2 minutes
                    print(f"Auto-releasing piece {piece_index} locked for too long")
                    self.release_piece(piece_index)
            return None
        
        # Select a piece using weighted random choice based on priority
        total_priority = sum(priority for _, priority in candidates)
        r = random.random() * total_priority
        
        cumulative = 0
        for piece_index, priority in candidates:
            cumulative += priority
            if cumulative >= r:
                # Final validation check
                piece = self.pieces[piece_index]
                
                # Mark the piece as busy
                self.busy_pieces.add(piece_index)
                self.piece_lock_time[piece_index] = time.time()
                self.stats["pieces_selected"] += 1
                
                # No debugging information to reduce verbosity
                return piece_index
        
        # If we reach here, just use the first candidate
        if candidates:
            selected = candidates[0][0]
            self.busy_pieces.add(selected)
            self.piece_lock_time[selected] = time.time()
            self.stats["pieces_selected"] += 1
            return selected
        
        return None
    
    def get_piece(self, piece_index: int, offset: int, length: int) -> Optional[bytes]:
        """Get a downloaded piece from disk"""

        if piece_index not in self.completed_pieces:
            return None
        
        if os.path.exists(f"file_pieces/{piece_index}.part"):
            with open(f"file_pieces/{piece_index}.part", 'rb') as f:
                f.seek(offset)
                data = f.read(length)
            return data

        return None

    def release_piece(self, piece_index: int, failed=True):
        """Release a busy piece so it can be downloaded by another peer."""
        if piece_index in self.busy_pieces:
            self.busy_pieces.discard(piece_index)
            if piece_index in self.piece_lock_time:
                del self.piece_lock_time[piece_index]
            if failed:
                self.stats["pieces_failed"] += 1
                print(f"Released piece {piece_index} for re-download")
            else:
                print(f"Released piece {piece_index} (completed)")

    def get_progress(self) -> float:
        """Calculate current download progress as a percentage."""
        if not self.pieces:
            return 0.0
        
        return (len(self.completed_pieces) / len(self.pieces)) * 100.0

    def is_complete(self) -> bool:
        """Check if all pieces have been downloaded successfully."""
        return len(self.completed_pieces) == len(self.pieces)

    def get_missing_pieces(self) -> List[int]:
        """Get indices of pieces that haven't been completed yet."""
        return [i for i in range(len(self.pieces)) if i not in self.completed_pieces]

    def get_stats(self) -> Dict[str, Any]:
        """Get detailed statistics about the download state."""
        stats = self.stats.copy()
        stats.update({
            "total_pieces": len(self.pieces),
            "completed_pieces": len(self.completed_pieces),
            "busy_pieces": len(self.busy_pieces),
            "remaining_pieces": len(self.pieces) - len(self.completed_pieces),
            "progress_percentage": self.get_progress(),
            "is_complete": self.is_complete()
        })
        return stats

    def debug_status(self, peer_bitfield=None, peer_info=None):
        """Print simplified status report."""
        stats = self.get_stats()
        
        print("\n=== Piece Manager Status ===")
        print(f"Total pieces: {stats['total_pieces']}")
        print(f"Completed: {stats['completed_pieces']} ({stats['progress_percentage']:.2f}%)")
        print(f"Busy pieces: {stats['busy_pieces']}")
        print(f"Remaining: {stats['remaining_pieces']}")
        
        # Calculate and display ETA if we have download rate
        if stats["download_rate_pieces"] > 0:
            remaining_pieces = stats['remaining_pieces']
            eta_seconds = remaining_pieces / stats["download_rate_pieces"]
            if eta_seconds < 60:
                eta = f"{eta_seconds:.0f} seconds"
            elif eta_seconds < 3600:
                eta = f"{eta_seconds/60:.1f} minutes"
            else:
                eta = f"{eta_seconds/3600:.1f} hours"
            print(f"Current rate: {stats['download_rate_pieces']:.2f} pieces/sec, ETA: {eta}")
        
        return stats
        
    def has_piece(self, piece_index):
        """Check if a piece has been successfully downloaded."""
        return piece_index in self.completed_pieces

    async def get_piece_block(self, piece_index, offset, length):
        """
        Retrieve a block of data from a completed piece.
        
        Args:
            piece_index: Index of the piece
            offset: Byte offset within the piece
            length: Number of bytes to retrieve
            
        Returns:
            Requested block data as bytes, or None if not available
        """
        if not self.has_piece(piece_index):
            return None
        
        # Calculate the absolute byte offset in the torrent
        absolute_offset = piece_index * self.torrent.piece_length + offset
        
        # Find which file(s) contain this data
        sorted_files = sorted(self.output_files.items(), key=lambda x: x[1]['offset'])
        
        # Prepare buffer for the result
        result_data = bytearray(length)
        bytes_read = 0
        
        for file_path, file_info in sorted_files:
            file_start = file_info['offset']
            file_end = file_start + file_info['length']
            
            # Skip files that don't contain our data
            if absolute_offset >= file_end or absolute_offset + length <= file_start:
                continue
            
            # Calculate overlap with this file
            read_start = max(0, absolute_offset - file_start)
            read_end = min(file_info['length'], absolute_offset + length - file_start)
            read_length = read_end - read_start
            
            # Skip if no actual overlap
            if read_length <= 0:
                continue
            
            # Calculate where in our result buffer this data belongs
            buffer_offset = max(0, file_start - absolute_offset)
            
            # Read the data from this file
            try:
                with open(file_path, 'rb') as f:
                    f.seek(read_start)
                    file_data = f.read(read_length)
                    result_data[buffer_offset:buffer_offset + len(file_data)] = file_data
                    bytes_read += len(file_data)
            except Exception as e:
                print(f"Error reading from {file_path}: {e}")
                continue
        
        # Verify we read all the requested data
        if bytes_read != length:
            print(f"Warning: Only read {bytes_read}/{length} bytes for piece {piece_index}")
            # Return whatever we got, or None if we got nothing
            return bytes(result_data[:bytes_read]) if bytes_read > 0 else None
        
        return bytes(result_data)