import socket
from message import Message
from network import recv_by_size
class Peer:
    def __init__(self, ip : str, port : int, info_hash, peer_id) -> None:
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
        self.healthy = True

    
    def connect(self):
        """
        Establish connection with peer, and perform handshake
        """
        # self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.sock.connect((self.ip, self.port))
        try:
            self.sock = socket.create_connection((self.ip, self.port), timeout=4)
            print(f"Connected to {self.ip}:{self.port}")
            # Perform handshake
            self._send_handshake()
            response = self._recv_handshake()
            parsed_response = self._parse_handshake(response)
            
            
            print(f"Raw response: {response}")
            print(f'Parsed response: {parsed_response}\n')

        
        except socket.error as e:
            self.healthy = False
            # print(f"Connection failed: {e}")
    
    
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


    def _recv_handshake(self):
        return self.sock.recv(68)
    
    
    def _parse_handshake(self, response):
        """
        Parse the handshake response from the peer
        
        :param response: 68 byte handshake response
        :return: A dictionary containing parse handshake fields
        """
        if len(response) != 68:
            # print(f'Length of response: {len(response)}')
            raise ValueError("Invalid handshake response")
        
        """
        Format:
        pstrlen : 1 byte, length of the protocol identifier
        pstr : string, protocol identifier
        reserved : 8 bytes, reserved bytes
        info_hash : 20 bytes, SHA1 hash of the info dictionary
        peer_id : 20 bytes, peer ID
        """
        pstrlen = response[0]
        pstr = response[1:1 + pstrlen].decode()
        reserved = response[1 + pstrlen:1+ pstrlen + 8]
        info_hash = response[1 + pstrlen + 8:1 + pstrlen + 8 + 20]
        peer_id = response[1 + pstrlen + 8 + 20:]

        return {
            'pstrlen': pstrlen,
            'pstr': pstr,
            'reserved': reserved,
            'info_hash': info_hash,
            'peer_id': peer_id
        }
    
    def send(self, message):
        """
        Send a serialized message to the peer.

        :param message: An instance of a Message class.
        """
        if isinstance(message, Message):
            self.sock.sendall(message.serialize())
        else:
            raise ValueError("Invalid message object.")

    def recv(self):
        return recv_by_size(self.sock)
