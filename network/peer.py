import asyncio
import socket
import struct
import bencodepy
import time
from protocol.message import Message
from protocol.extensions.pex import PEXExtension
import protocol.message as message

# Extension message IDs (BEP 10)
EXT_HANDSHAKE_ID = 0
UT_PEX_ID = 1  # Default ID for PEX

# PEX settings
PEX_INTERVAL = 120  # Send PEX messages every 2 minutes

class Peer:
    def __init__(self, ip: str, port: int, info_hash, peer_id, piece_manager, peer_manager) -> None:
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

        # Queue to store incoming messages
        self.message_queue = asyncio.Queue()
        # Listener task for continuously reading messages
        self.listener_task = None
        
        # PEX task for using PEX(using this to avoid garbage collector stopping the task)
        self.pex_task = None
        
        # Source tracking for stats
        self.source = "unknown"
        
        # Reference to peer manager
        self.peer_manager = peer_manager
        self.pex = PEXExtension(self)

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

    async def disconnect(self):
        """Close the connection and stop the listener task."""
        if self.writer and not self.writer.is_closing():
            self.writer.close()
            await self.writer.wait_closed()
        if self.listener_task:
            self.listener_task.cancel()
        print(f"Disconnected from {self.ip}:{self.port}")

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
                        self.pex.process_extended_handshake(payload)
                    elif ext_id == self.pex.ut_pex_id:  # Handle PEX message with configured ID
                        # Process as PEX message
                        new_peers = self.pex.process_pex_message(payload, self.peer_manager.my_ip)
                        if new_peers and self.peer_manager:
                            # Add all new peers to the known peers list
                            for ip, port in new_peers:
                                self.peer_manager.add_peer(ip, port, source="pex")
                            
                            # Trigger immediate connection to the new peers
                            self.pex_task = asyncio.create_task(self.peer_manager.connect_to_pex_peers(new_peers))
                    else:
                        # Just log the message type but don't print details
                        pass
                    continue
                    
                # Handle standard BitTorrent messages
                msg = Message.deserialize(data)
                
                if isinstance(msg, message.Piece):
                    # Enqueue Piece messages for download responses
                    await self.incoming_queue.put(msg)
                else:
                    await self.message_queue.put(msg)
                
            except asyncio.TimeoutError:
                # Timeouts are normal for idle connections
                pass
            # Error for not passing the correct arguments to the message deserialize
            except ValueError as e:
                # Print the error exactly
                print(f"Didnt properly deserialize message from {self.ip}:{self.port}: {e}")
            except asyncio.IncompleteReadError as e:
                print(f"Connection closed with {self.ip}:{self.port}: {e}")
                break
            except Exception as e:
                print(f"Error in listening from {self.ip}:{self.port}: {e}")
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
        # Process the piece message, if it is done correctly, send out a have message
        if self.piece_manager.recieve_block_piece(piece_msg.index, piece_msg.begin, piece_msg.block):
            # Send a have message to the peer
            have_msg = message.Have(piece_msg.index)
            await self.send(have_msg)
            print(f"Sent HAVE message for piece {piece_msg.index} to {self.ip}:{self.port}")

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
    
    async def handle_request(self, request_msg):
        """Handle a Request message from a peer by sending the requested piece."""
        # Get the request details
        piece_index = request_msg.index
        offset = request_msg.begin
        length = request_msg.length
        
        print(f"Processing request for piece {piece_index}, offset {offset}, length {length} from {self.ip}:{self.port}")
        
        # Check if we have the requested piece
        if not self.piece_manager.has_piece(piece_index):
            print(f"Can't fulfill request for piece {piece_index} - we don't have it")
            return False
        
        # Get the piece data from the PieceManager
        piece_data = await self.piece_manager.get_piece_block(piece_index, offset, length)
        if not piece_data:
            print(f"Failed to read piece {piece_index} data from disk")
            return False
        
        # Create and send the Piece message
        piece_msg = message.Piece(piece_index, offset, piece_data)
        try:
            await self.send(piece_msg)
            print(f"✅ Sent piece {piece_index} block at offset {offset} to {self.ip}:{self.port}")
            return True
        except Exception as e:
            print(f"Error sending piece {piece_index} to {self.ip}:{self.port}: {e}")
            return False

    async def handle_interested(self):
        """Handle an Interested message by updating peer state."""
        self.state['peer_interested'] = True
        print(f"Peer {self.ip}:{self.port} is now interested in our pieces")
        
        # For simplicity, immediately unchoke this peer
        if self.state['am_choking']:
            await self.send_unchoke()

    async def send_unchoke(self):
        """Send an Unchoke message to the peer."""
        if self.state['am_choking']:
            unchoke_msg = message.Unchoke()
            await self.send(unchoke_msg)
            self.state['am_choking'] = False
            print(f"Sent UNCHOKE to {self.ip}:{self.port}")