import hashlib
import requests
import bencodepy
import random
import socket
import struct
import time
import urllib.parse
from core.torrent import Torrent


class TrackerHandler:
    """
    A class to manage the communication with a single tracker
    """

    def __init__(self, torrent: Torrent):
        self.torrent = torrent
        # # Use provided tracker_url if specified, otherwise use the torrent's announce
        # self.tracker_url = tracker_url if tracker_url is not None else torrent.announce
        
        self.peer_id = self.generate_peer_id().encode()
        self.port = random.randint(6881, 6889)  # Typical BitTorrent client ports

        self.info_hash = hashlib.sha1(bencodepy.encode(self.torrent.info_dict)).digest()
        self.peers_list = []
        self.response = None
        
        # UDP tracker specific variables
        self.connection_id = None
        self.transaction_id = None
        self.udp_socket = None
        self.last_connection_time = 0
    
    def generate_peer_id(self):
        # Generate a 20-byte peer ID, e.g., -PYTHONCLIENT-00001
        return "-PYTORRENT-" + "".join(str(random.randint(0, 9)) for _ in range(9))

    # Method to build the request 
    def build_tracker_url(self, base_url:str, event:str = None):
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
        tracker_url = base_url + "?" + query_string 
        return tracker_url
    
    def send_request(self, event:str = None):
        """
        Send a request to the tracker, parse the response
        """
        for tier in self.torrent.announce_list:
            for tracker_url in tier:
                print(f"Getting data from tracker: {tracker_url}")
                # tracker_url = tracker[0]
                if tracker_url.startswith("http"):
                    success = self._send_http_request(tracker_url, event)
                else:
                    
                    success = self._send_udp_request(tracker_url, event)
                
                if success:
                    return True
        return False
        

    
    def _send_http_request(self, tracker_url:str, event:str = None):
        """
        Send an HTTP request to the tracker, parse the response
        """
        url = self.build_tracker_url(tracker_url, event)
        
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                tracker_response = bencodepy.decode(response.content)
                self.response = tracker_response
                self.parse_tracker_response(tracker_response)
                return True
            else:
                print(f"Tracker request failed: HTTP {response.status_code}")
                return False
        except requests.RequestException as e:
            print(f"Tracker request failed: {e}")
            return False
    
    def _send_udp_request(self, url:str, event:str = None):
        """
        Send a request to a UDP tracker
        """
        try:
            # Parse URL to get host and port
            parsed_url = urllib.parse.urlparse(url)
            host = parsed_url.hostname
            port = parsed_url.port or 80
            
            # Create socket
            if not self.udp_socket:
                self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.udp_socket.settimeout(15)
            
            # First establish a connection to get a connection_id
            if not self.connection_id or time.time() - self.last_connection_time > 60:
                if not self._udp_connect(host, port):
                    print("Failed to connect to UDP tracker")
                    return False
            
            # Send announce request
            return self._udp_announce(host, port, event)
        except Exception as e:
            print(f"UDP tracker error: {e}")
            return False
        finally:
            # Clean up socket when done
            if self.udp_socket:
                self.udp_socket.close()
                self.udp_socket = None
    
    def _udp_connect(self, host, port):
        """
        Establish connection with UDP tracker to get connection_id
        """
        # Connection message components per BEP 15
        protocol_id = 0x41727101980  # Magic constant
        action = 0  # Connect action
        self.transaction_id = random.randint(0, 0xFFFFFFFF)
        
        # Pack the message
        message = struct.pack('>QLL', protocol_id, action, self.transaction_id)
        
        # Send request with retries
        max_retries = 3
        timeout = 15  # seconds
        
        for attempt in range(max_retries):
            try:
                self.udp_socket.sendto(message, (host, port))
                
                # Wait for and process response
                start_time = time.time()
                while time.time() - start_time < timeout:
                    try:
                        response, _ = self.udp_socket.recvfrom(16)
                        
                        if len(response) < 16:
                            continue
                        
                        action, resp_transaction_id, connection_id = struct.unpack('>LLQ', response)
                        
                        # Verify response
                        if resp_transaction_id != self.transaction_id:
                            continue
                        
                        if action != 0:  # Not a connect response
                            continue
                        
                        # Success - store connection_id
                        self.connection_id = connection_id
                        self.last_connection_time = time.time()
                        return True
                    
                    except socket.timeout:
                        break
            
            except Exception as e:
                print(f"UDP connect error: {e}")
                
            # Exponential backoff
            time.sleep(2 ** attempt)
        
        print(f"Failed to connect to UDP tracker after {max_retries} attempts")
        return False
    
    def _udp_announce(self, host, port, event=None):
        """
        Send an announce request to the UDP tracker
        """
        if not self.connection_id:
            print("No valid connection ID for UDP tracker")
            return False
        
        # Prepare announce message
        action = 1  # Announce action
        self.transaction_id = random.randint(0, 0xFFFFFFFF)
        
        # Map event strings to event IDs
        event_map = {
            'started': 2,
            'completed': 1,
            'stopped': 3
        }
        event_id = event_map.get(event, 0) if event else 0
        
        # Pack the message
        # Format: connection_id (8), action (4), transaction_id (4), info_hash (20),
        # peer_id (20), downloaded (8), left (8), uploaded (8), event (4),
        # IP (4, 0 = default), key (4, random), num_want (4, -1 = default), port (2)
        announce_message = struct.pack(
            '>QLL20s20sQQQLLLLH',  # Note: 13 items total
            self.connection_id,
            action,
            self.transaction_id,
            self.info_hash,
            self.peer_id,
            0,  # downloaded
            self.torrent.file_length,  # left
            0,  # uploaded
            event_id,
            0,  # IP (default)
            random.randint(0, 0xFFFFFFFF),  # key
            0xFFFFFFFF,  # num_want
            self.port
        )
        
        # Send request with retries
        max_retries = 3
        timeout = 15  # seconds
        
        for attempt in range(max_retries):
            try:
                self.udp_socket.sendto(announce_message, (host, port))
                
                # Wait for and process response
                start_time = time.time()
                while time.time() - start_time < timeout:
                    try:
                        response, _ = self.udp_socket.recvfrom(4096)
                        
                        if len(response) < 20:  # Minimum response size
                            continue
                        
                        # Parse response header
                        action, resp_transaction_id, interval, leechers, seeders = struct.unpack('>LLLLL', response[:20])
                        
                        # Verify response
                        if resp_transaction_id != self.transaction_id:
                            continue
                        
                        if action != 1:  # Not an announce response
                            if action == 3:  # Error
                                error_msg = response[8:].decode('utf-8', errors='ignore')
                                print(f"UDP tracker error: {error_msg}")
                            continue
                        
                        # Success - parse peers
                        peers_data = response[20:]
                        self._decode_udp_peers(peers_data)
                        
                        # Create a response dict similar to HTTP tracker
                        self.response = {
                            b'interval': interval,
                            b'complete': seeders,
                            b'incomplete': leechers
                        }
                        
                        print(f"UDP tracker response: {seeders} seeders, {leechers} leechers, {len(self.peers_list)} peers")
                        return True
                    
                    except socket.timeout:
                        break
            
            except Exception as e:
                print(f"UDP announce error: {e}")
                
            # Exponential backoff
            time.sleep(2 ** attempt)
        
        print(f"Failed to announce to UDP tracker after {max_retries} attempts")
        return False
    
    def _decode_udp_peers(self, peers_data):
        """
        Decode peers from the UDP tracker response
        """
        peers_list = []
        for i in range(0, len(peers_data), 6):
            if i + 6 <= len(peers_data):
                ip = socket.inet_ntoa(peers_data[i:i+4])
                port = struct.unpack('>H', peers_data[i+4:i+6])[0]
                peers_list.append((ip, port))
        
        self.peers_list = peers_list
    
    def parse_tracker_response(self, response: dict):
        """
        Parse the response from an HTTP tracker
        """
        if b'peers' in response:
            self.decode_peers(response[b'peers'])
                
    def decode_peers(self, peers: bytes):
        """
        Decode compact peers format (6 bytes per peer: 4 for IP, 2 for port)
        """
        peers_list = []
        for i in range(0, len(peers), 6):
            if i + 6 <= len(peers):
                ip = socket.inet_ntoa(peers[i:i+4]) # Decode IP from bytes
                port = int.from_bytes(peers[i+4:i+6], 'big') # Decode port from bytes
                peers_list.append((ip, port))
        self.peers_list = peers_list