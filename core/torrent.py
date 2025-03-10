import bencodepy
from datetime import datetime


class Torrent:
    # A class to store and process metadata from a .torrent file

    def __init__(self) -> None:
        # Initialize all the metadata attributes
        self.announce = None  # The URL of the tracker
        self.announce_list = [] # List of tracker URLS
        self.files = []  # Information about the files in the torrent
        self.file_length: int = 0  # Total length of the files in bytes
        self.pieces = None  # A string containing SHA-1 hashes of the file pieces
        self.total_pieces: int = 0  # Total number of pieces in the torrent
        self.piece_length: int = 0  # Length of each piece in bytes
        self.name: str = None  # Name of the torrent (usually the folder or main file)
        self.creation_date = None
        self.comment = None
        self.info_dict = None # Info dictionary 

    def load_file(self, path: str) -> None:
        """
        Load a .torrent file, decode it, and extract metadata.

        :param path: Path to the .torrent file
        """
        # Open the .torrent file in binary mode
        with open(path, 'rb') as file:
            data = file.read()  # Read the file's content
            data = bencodepy.decode(data)  # Decode the bencoded data

            # Extract the 'announce' field (tracker URL)
            if b'announce' in data:
                self.announce = data[b'announce'].decode('utf-8')

            # Extract info dictionary 
            self.info_dict = data[b'info']

            # Extract optional metadata
            self.creation_date = data.get(b'creation date', None)
            self.comment = data.get(b'comment', b'').decode('utf-8', errors='ignore')

            self.name = data[b'info'][b'name'].decode('utf-8')

            # Extract piece length (size of each chunk)
            self.piece_length = data[b'info'][b'piece length']

            # Extract the pieces (concatenated SHA-1 hashes of file pieces)
            self.pieces = data[b'info'][b'pieces']
            self.total_pieces = len(self.pieces) // 20
            
            if b'announce-list' in data:
                self.announce_list = [
                    [url.decode('utf-8') for url in tier] 
                    for tier in data[b'announce-list']
                ]
            if not self.announce and self.announce_list and self.announce_list[0]:
                self.announce = self.announce_list[0][0]
                
            # Handle single-file vs multi-file torrents
            if b'files' in data[b'info']:
                # Multi-file torrent
                for file_info in data[b'info'][b'files']:
                    length = file_info[b'length']
                    path = [p.decode('utf-8') for p in file_info[b'path']]
                    
                    self.files.append({'length': length, 'path': path})
                    
            else:
                # Single-file torrent
                self.file_length = data[b'info'][b'length']
                self.files = [{'length': self.file_length, 'path': [self.name]}]
            
    def display_info(self):
        print(f"Name: {self.name}")
        print(f"Files: {self.files}")
        print(f"Announce: {self.announce}")
        print(f"Announce List: {self.announce_list}")
        if self.creation_date:
            print(f"Creation Date: {datetime.fromtimestamp(self.creation_date)}")
        print(f"Piece Length: {self.piece_length}")
        print(f"Total Pieces: {len(self.pieces)//20}")