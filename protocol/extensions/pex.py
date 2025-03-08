import socket
import struct
import time
import bencodepy
from protocol.message import Extended

# Extension message IDs
EXT_HANDSHAKE_ID = 0
UT_PEX_ID = 1  # Default ID for PEX

# PEX settings
PEX_INTERVAL = 45  # Send PEX messages every 45 seconds

class PEXExtension:
    """PEX functionality to extend the existing Peer class."""
    
    def __init__(self, peer):
        self.peer = peer
        self.supports_pex = False
        self.ut_pex_id = UT_PEX_ID  # Default to standard ID of 1
        self.last_pex_sent = 0
        self.sent_peers = set()
        self.pex_interval = PEX_INTERVAL
    
    async def send_extended_handshake(self):
        """Send an extended handshake message to negotiate PEX."""
        try:
            # Create the handshake dictionary
            handshake_dict = {
                b'm': {b'ut_pex': UT_PEX_ID},  # We support ut_pex with ID 1
                b'v': b'PythonBitTorrent/1.0',
                b'p': 6881,  # Listening port
            }
            
            # Encode the dictionary
            payload = bencodepy.encode(handshake_dict)
            
            # Create and send the extended message
            ext_msg = Extended(EXT_HANDSHAKE_ID, payload)
            await self.peer.send(ext_msg)
            print(f"Sent extended handshake to {self.peer.ip}:{self.peer.port}")
            return True
        except Exception as e:
            print(f"Error sending extended handshake to {self.peer.ip}:{self.peer.port}: {e}")
            return False
    
    def process_extended_handshake(self, payload: bytes):
        """Process an extended handshake message from the peer."""
        try:
            handshake_dict = bencodepy.decode(payload)
            
            client_name = handshake_dict.get(b'v', b'unknown')
            if isinstance(client_name, bytes):
                client_name = client_name.decode('utf-8', errors='replace')
            
            print(f"Received extended handshake from {self.peer.ip}:{self.peer.port} ({client_name})")
            
            # Check if the peer supports PEX
            if b'm' in handshake_dict and b'ut_pex' in handshake_dict[b'm']:
                self.supports_pex = True
                self.ut_pex_id = handshake_dict[b'm'][b'ut_pex']
                print(f"Peer {self.peer.ip}:{self.peer.port} supports PEX (ID: {self.ut_pex_id})")
                return True
            else:
                # Many clients support PEX with ID 1 by default, even if not explicitly stated
                self.supports_pex = True  # Assume support for PEX with default ID
                print(f"Peer {self.peer.ip}:{self.peer.port} assumed to support PEX with default ID")
                return True
        except Exception as e:
            print(f"Error parsing extended handshake: {e}")
            return False
    
    async def send_pex_message(self, added_peers, my_ip):
        """Send a PEX message with new peers."""
        if not self.supports_pex:
            return
        
        # Filter out peers we've already sent to this peer and ourselves
        new_peers = []
        for ip, port in added_peers:
            if (ip, port) in self.sent_peers:
                continue
            if ip == my_ip:  # Don't send ourselves
                continue
            try:
                # Validate IP and port
                socket.inet_aton(ip)
                if not isinstance(port, int) or port <= 0 or port > 65535:
                    continue
                new_peers.append((ip, port))
            except:
                continue
        
        if not new_peers:
            return
        
        # Update the sent peers set
        self.sent_peers.update(new_peers)
        
        # Convert peers to compact format
        added_bytes = b''
        for ip, port in new_peers:
            try:
                packed_ip = socket.inet_aton(ip)
                packed_port = struct.pack('>H', port)
                added_bytes += packed_ip + packed_port
            except Exception as e:
                print(f"Error packing peer {ip}:{port}: {e}")
                continue
        
        # Create the PEX dictionary
        pex_dict = {b'added': added_bytes}
        
        # Encode the dictionary
        payload = bencodepy.encode(pex_dict)
        
        # Create and send the extended message
        ext_msg = Extended(self.ut_pex_id, payload)
        await self.peer.send(ext_msg)
        
        self.last_pex_sent = time.time()
        print(f"Sent PEX message to {self.peer.ip}:{self.peer.port} with {len(new_peers)} new peers")
    
    def process_pex_message(self, payload: bytes, my_ip):
        """Process a PEX message from the peer."""
        try:
            pex_dict = bencodepy.decode(payload)
            
            added_peers = []
            if b'added' in pex_dict:
                added_bytes = pex_dict[b'added']
                
                # Process 6-byte chunks (4 for IP, 2 for port)
                for i in range(0, len(added_bytes), 6):
                    if i + 6 <= len(added_bytes):
                        try:
                            ip = socket.inet_ntoa(added_bytes[i:i+4])
                            port = struct.unpack('>H', added_bytes[i+4:i+6])[0]
                            
                            # Skip invalid IPs and ports
                            if ip == "0.0.0.0" or ip == my_ip or port <= 0 or port > 65535:
                                continue
                                
                            added_peers.append((ip, port))
                        except Exception as e:
                            print(f"Error unpacking peer: {e}")
                        
                # Also handle 'added.f' (encrypted connections) if present
                if b'added.f' in pex_dict:
                    print("PEX message contains encrypted peer flags (added.f)")
            
            # Handle dropped peers if present
            if b'dropped' in pex_dict:
                dropped_bytes = pex_dict[b'dropped']
                dropped_count = len(dropped_bytes) // 6
                print(f"PEX message contains {dropped_count} dropped peers")
            
            print(f"Received PEX message from {self.peer.ip}:{self.peer.port} with {len(added_peers)} peers")
            if added_peers:
                for ip, port in added_peers[:5]:  # Print first 5 peers
                    print(f"  - {ip}:{port}")
                if len(added_peers) > 5:
                    print(f"  - ... and {len(added_peers) - 5} more")
                    
            return added_peers
        except Exception as e:
            print(f"Error parsing PEX message: {e}")
            return []