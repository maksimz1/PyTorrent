import socket

class Peer:
    def __init__(self, ip, port, info_hash, peer_id):
        self.ip = ip
        self.port = port
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.sock = None
        self.state = {
            'am_choking': True,
            'am_interested': False,
            'peer_choking': True,
            'peer_interested': False
        }
    
    def connect(self):
        """
        Establish connection with peer, and perform handshake
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.ip, self.port))
        print(f"Connected to {self.ip}:{self.port}")
        # self.sock = socket.create_connection((self.ip, self.port), timeout=5)
        try:

            # Perform handshake
            self._send_handshake()
            response = self._recv_handshake()

            print(response)
        
        except socket.error:
            print(f"Connection to {self.ip}:{self.port} failed")
    
    def _recv_handshake(self):
        return self.sock.recv(68)
    
    def _send_handshake(self):
        protocol = b'BitTorrent protocol'
        reserved = b'\x00\x00\x00\x00\x00\x00\x00\x00'
        handshake = (
            bytes([len(protocol)]) +
            protocol +
            reserved +
            self.info_hash + 
            self.peer_id
        )
        self.sock.sendall(handshake)
