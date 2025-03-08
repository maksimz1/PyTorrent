import asyncio
import socket
import struct
import bencodepy
import time
from protocol.message import Message
import protocol.message as message
import hashlib
import traceback

# Extension message IDs (BEP 10)
EXT_HANDSHAKE_ID = 0
UT_PEX_ID = 1  # Default ID for PEX

# PEX settings
PEX_INTERVAL = 45  # Send PEX messages every 45 seconds

class Peer:
    def __init__(self, ip: str, port: int, info_hash, peer_id, piece_manager) -> None:
        self.ip = ip
        self.port = port
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.reader: asyncio.StreamReader = None
        self.writer: asyncio.StreamWriter = None
        self.state = {
            'am_choking': True,
            'am_interested': False,
            'peer_choking': True,
            'peer_interested': False
        }
        self.healthy = True
        self.bitfield = None
        self.piece_manager = piece_manager

        # Queue to store incoming Piece messages
        self.incoming_queue = asyncio.Queue()
        # Listener task for continuously reading messages
        self.listener_task = None
        
        # PEX support
        self.supports_pex = False
        self.ut_pex_id = UT_PEX_ID  # Default to standard ID of 1
        self.last_pex_sent = 0
        self.sent_peers = set()
        self.pex_interval = PEX_INTERVAL
        
        # Source tracking for stats
        self.source = "unknown"
        
        # Reference to peer manager (set after creation)
        self.pex_manager = None

    async def connect(self):
        try:
            timeout = 5  # seconds
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.ip, self.port), timeout=timeout
            )
            print(f"Connected to {self.ip}:{self.port}")
            await self._send_handshake()
            response = await self._recv_handshake()
            parsed_response = self._parse_handshake(response)
            print(f"🤝 Handshake with {self.ip}:{self.port} successful")
            
            # Start the continuous listener for incoming messages
            self.listener_task = asyncio.create_task(self.listen_for_messages())
        except Exception as e:
            self.healthy = False
            print(f"Connection failed to {self.ip}:{self.port}: {e}")

    async def reconnect(self, delay=5, max_attempts=5):
        """Try to reconnect to the peer after a delay, with a limited number of attempts."""
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            print(f"Attempting reconnection {attempt}/{max_attempts} for {self.ip}:{self.port}...")
            try:
                await asyncio.sleep(delay)  # Wait before trying to reconnect
                await self.connect()
                if self.healthy:
                    print(f"Reconnected to {self.ip}:{self.port} successfully.")
                    return True
            except Exception as e:
                print(f"Reconnection attempt {attempt} failed: {e}")
        print(f"Failed to reconnect to {self.ip}:{self.port} after {max_attempts} attempts.")
        return False
    
    async def listen_for_messages(self):
        while True:
            try:
                # Read the first 4 bytes for the message size
                size_bytes = await self.reader.readexactly(4)
                size = int.from_bytes(size_bytes, 'big')
                
                if size == 0:
                    # Keep-alive message, ignore
                    continue
                    
                # Now read the full message based on the size
                data = await self.reader.readexactly(size)
                
                # Check for extended message type (20)
                if size > 0 and data[0] == 20:  # Extended message
                    ext_id = data[1]
                    payload = data[2:]
                    
                    if ext_id == EXT_HANDSHAKE_ID:
                        # Extended handshake
                        self.process_extended_handshake(payload)
                    elif ext_id == self.ut_pex_id:  # Handle PEX message with configured ID
                        # Process as PEX message
                        new_peers = self.process_pex_message(payload)
                        if new_peers and self.pex_manager:
                            # Add all new peers to the known peers list
                            for ip, port in new_peers:
                                self.pex_manager.add_peer(ip, port, source="pex")
                            
                            # Trigger immediate connection to the new peers
                            asyncio.create_task(self.pex_manager.connect_to_pex_peers(new_peers))
                    else:
                        # Just log the message type but don't print details
                        pass
                    continue
                    
                # Handle standard BitTorrent messages
                msg = Message.deserialize(data)
                
                # Dispatch messages based on their type
                if isinstance(msg, message.Unchoke):
                    self.handle_unchoke()
                elif isinstance(msg, message.Bitfield):
                    self.handle_bitfield(msg)
                elif isinstance(msg, message.Have):
                    self.handle_have(msg)
                elif isinstance(msg, message.Piece):
                    # Enqueue Piece messages for download responses
                    await self.incoming_queue.put(msg)
            except asyncio.TimeoutError:
                # Timeouts are normal for idle connections
                pass
            except asyncio.IncompleteReadError as e:
                print(f"Connection closed with {self.ip}:{self.port}: {e}")
                break
            except Exception as e:
                print(f"Error in listening from {self.ip}:{self.port}: {e}")
                # Print stack trace for unexpected errors in debug mode only
                # traceback.print_exc()
                break

    async def get_piece_message(self, expected_piece_index, expected_offset, timeout=5):
        """
        Wait until a Piece message with the expected piece index and offset is received.
        """
        while True:
            try:
                msg = await asyncio.wait_for(self.incoming_queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                raise Exception("Timeout waiting for piece message")
            if isinstance(msg, message.Piece) and msg.index == expected_piece_index and msg.begin == expected_offset:
                return msg
            else:
                # Don't log every mismatch to reduce verbosity
                # Only make a note if we got completely wrong piece
                if msg.index != expected_piece_index:
                    print(f"Ignoring piece {msg.index}, waiting for piece {expected_piece_index}")

    async def _send_handshake(self):
        """Send handshake with extension protocol support."""
        protocol = b'BitTorrent protocol'
        # Set 5th bit in reserved bytes to indicate support for extension protocol (BEP 10)
        reserved = b'\x00\x00\x00\x00\x00\x10\x00\x00'
        
        handshake = (
            bytes([len(protocol)]) +
            protocol +
            reserved +
            self.info_hash +
            self.peer_id
        )
        self.writer.write(handshake)
        await self.writer.drain()

    async def _recv_handshake(self) -> bytes:
        return await self.reader.readexactly(68)

    def _parse_handshake(self, response: bytes) -> dict:
        if len(response) != 68:
            raise ValueError(f"Invalid handshake response: {response}")
        pstrlen = response[0]
        pstr = response[1:1+pstrlen].decode()
        reserved = response[1+pstrlen:1+pstrlen+8]
        info_hash = response[1+pstrlen+8:1+pstrlen+8+20]
        peer_id = response[1+pstrlen+8+20:]
        
        # Check if peer supports extension protocol (5th bit in reserved bytes)
        supports_extensions = bool(reserved[5] & 0x10)
        if supports_extensions:
            print(f"Peer {self.ip}:{self.port} supports extension protocol")
        
        return {
            'pstrlen': pstrlen,
            'pstr': pstr,
            'reserved': reserved,
            'info_hash': info_hash,
            'peer_id': peer_id,
            'supports_extensions': supports_extensions
        }

    async def request_piece(self, index, begin, length):
        req = message.Request(index, begin, length)
        await self.send(req)
        # No logging for individual block requests to reduce verbosity

    async def send(self, msg):
        if hasattr(msg, 'serialize'):
            data = msg.serialize()
            self.writer.write(data)
            await self.writer.drain()
        else:
            raise ValueError("Invalid message object")

    async def handle_piece(self, piece_msg: message.Piece):
        # Process a Piece message (update piece manager, write block, etc.)
        # No logging for individual block receipts to reduce verbosity
        self.piece_manager.recieve_block_piece(piece_msg.index, piece_msg.begin, piece_msg.block)

    def handle_unchoke(self):
        self.state['peer_choking'] = False

    def handle_choke(self):
        self.state['peer_choking'] = True

    def handle_bitfield(self, bitfield_msg: message.Bitfield):
        self.bitfield = bitfield_msg.bitfield

    def handle_have(self, have_msg: message.Have):
        if self.bitfield is None:
            total_pieces = self.piece_manager.number_of_pieces
            self.bitfield = [0] * total_pieces
        self.bitfield[have_msg.index] = 1

    def is_choking(self):
        return self.state['peer_choking']

    def am_choking(self):
        return self.state['am_choking']

    def is_interested(self):
        return self.state['peer_interested']

    def am_interested(self):
        return self.state['am_interested']
    
    # PEX specific methods
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
            ext_msg = message.Extended(EXT_HANDSHAKE_ID, payload)
            await self.send(ext_msg)
            print(f"Sent extended handshake to {self.ip}:{self.port}")
            return True
        except Exception as e:
            print(f"Error sending extended handshake to {self.ip}:{self.port}: {e}")
            return False
    
    def process_extended_handshake(self, payload: bytes):
        """Process an extended handshake message from the peer."""
        try:
            handshake_dict = bencodepy.decode(payload)
            
            client_name = handshake_dict.get(b'v', b'unknown')
            if isinstance(client_name, bytes):
                client_name = client_name.decode('utf-8', errors='replace')
            
            # Check if the peer supports PEX
            if b'm' in handshake_dict and b'ut_pex' in handshake_dict[b'm']:
                self.supports_pex = True
                self.ut_pex_id = handshake_dict[b'm'][b'ut_pex']
                print(f"Peer {self.ip}:{self.port} supports PEX (ID: {self.ut_pex_id})")
                return True
            else:
                # Many clients support PEX with ID 1 by default, even if not explicitly stated
                self.supports_pex = True  # Assume support for PEX with default ID
                print(f"Peer {self.ip}:{self.port} assumed to support PEX with default ID")
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
        ext_msg = message.Extended(self.ut_pex_id, payload)
        await self.send(ext_msg)
        
        self.last_pex_sent = time.time()
        print(f"Sent PEX message to {self.ip}:{self.port} with {len(new_peers)} new peers")
    
    def process_pex_message(self, payload: bytes):
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
                            if ip == "0.0.0.0" or port <= 0 or port > 65535:
                                continue
                                
                            added_peers.append((ip, port))
                        except Exception as e:
                            # Silently ignore parsing errors to reduce verbosity
                            pass
            
            # Log only summary instead of details
            if added_peers:
                print(f"Received {len(added_peers)} new peers via PEX from {self.ip}:{self.port}")
            return added_peers
        except Exception as e:
            print(f"Error parsing PEX message: {e}")
            return []