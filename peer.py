import asyncio
from message import Message
import message

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

        # New: Queue to store incoming Piece messages
        self.incoming_queue = asyncio.Queue()
        # Listener task for continuously reading messages
        self.listener_task = None

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
            # print(f"Parsed handshake: {parsed_response}")
            print(f"🤝 Parsed handshake from: {self.ip}:{self.port}")
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
                # Now read the full message based on the size
                data = await self.reader.readexactly(size)
            except Exception as e:
                print(f"Error in listening from {self.ip}:{self.port}: {e}")
                
                # Check if the error is Win 10053(Conenction aborted)
                if "10053" in str(e):
                    print(f"Detected Win 10053 error for {self.ip}:{self.port}, scheduling reconnect")
                    await self.reconnect()
                break  # Exit the loop on error or disconnection
            msg = Message.deserialize(data)
            # Dispatch unsolicited control messages immediately
            if isinstance(msg, message.Unchoke):
                self.handle_unchoke()
                print(f"Peer {self.ip}:{self.port} unchoked us.")
            elif isinstance(msg, message.Bitfield):
                self.handle_bitfield(msg)
                print(f"Peer {self.ip}:{self.port} sent Bitfield.")
            elif isinstance(msg, message.Have):
                self.handle_have(msg)
            elif isinstance(msg, message.Piece):
                # Enqueue Piece messages for download responses
                await self.incoming_queue.put(msg)
            else:
                print(f"Received unexpected message type: {type(msg)}")

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
                print(f"Ignoring unexpected piece message: received piece {msg.index} at offset {msg.begin}, "
                      f"expected piece {expected_piece_index} at offset {expected_offset}")

    async def _send_handshake(self):
        protocol = b'BitTorrent protocol'
        reserved = b'\x00' * 8
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
        return {
            'pstrlen': pstrlen,
            'pstr': pstr,
            'reserved': reserved,
            'info_hash': info_hash,
            'peer_id': peer_id
        }

    async def request_piece(self, index, begin, length):
        req = message.Request(index, begin, length)
        await self.send(req)
        # print(f"Sent request for piece {index} at {begin} with length {length}")

    async def send(self, msg):
        if hasattr(msg, 'serialize'):
            data = msg.serialize()
            self.writer.write(data)
            await self.writer.drain()
        else:
            raise ValueError("Invalid message object")

    async def handle_piece(self, piece_msg: message.Piece):
        # Process a Piece message (update piece manager, write block, etc.)
        # print(f"📥 Received block for piece {piece_msg.index} at offset {piece_msg.begin}")
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
