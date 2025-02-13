import hashlib
import requests
import bencodepy
import random
import socket
from torrent import Torrent


class TrackerHandler:
    """
    A class to manage all the communication with the tracker
    """

    def __init__(self, torrent: Torrent):
        self.torrent = torrent
        self.peer_id = self.generate_peer_id().encode()
        self.port = random.randint(6881, 6889)  # Typical BitTorrent client ports

        self.info_hash = hashlib.sha1(bencodepy.encode(self.torrent.info_dict)).digest()
        self.peers_list = []
    
    def generate_peer_id(self):
        # Generate a 20-byte peer ID, e.g., -PYTHONCLIENT-00001
        return "-PYTORRENT-" + "".join(str(random.randint(0, 9)) for _ in range(9))


    # Method to build the request 
    def build_tracker_url(self, event:str = None):
        # Create the query string to send to the tracker
        query_params = {
            'info_hash': requests.utils.quote(self.info_hash),
            'peer_id': self.peer_id,
            'port': self.port,
            'uploaded': 0,
            'downloaded': 0,
            'left': self.torrent.file_length,
            'compact': 1,
        }
        if event:
            query_params['event'] = event

        # Build the full tracker URL for GET request
        query_string = "&".join([f"{k}={v}" for k, v in query_params.items()])
        tracker_url = self.torrent.announce + "?" + query_string 
        return tracker_url
    
    def send_request(self, event:str = None):
        """
        Send a request to the tracker, parse the response
        """
        url = self.build_tracker_url(event)
        
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                tracker_response = bencodepy.decode(response.content)
                self.response = tracker_response
                self.parse_tracker_response(tracker_response)

            else:
                print(f"Tracker request failed: HTTP {response.status_code}")
        except requests.RequestException as e:
            print(f"Tracker request failed: {e}")

    def parse_tracker_response(self, response: dict):
        # print(f"Tracker response: {response}")

        if b'peers' in response:
            self.decode_peers(response[b'peers'])
                
    def decode_peers(self, peers: bytes):
        peers_list = []
        for i in range(0, len(peers), 6):
            ip = socket.inet_ntoa(peers[i:i+4]) # Decode IP from bytes
            port = int.from_bytes(peers[i + 4:i + 6], 'big') # Decode port from bytes
            peers_list.append((ip, port))
        self.peers_list = peers_list


        
    