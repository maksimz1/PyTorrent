import asyncio
from network.peer import Peer
class TorrentServer:
    def __init__(self, session_manager, port):
        self.session_manager = session_manager
        self.port = port
        self.server = None

    async def start(self):
        self.server = await asyncio.start_server(
            self.handle_connection, '0.0.0.0', self.port
        )
        print(f"Listening for incoming connections on port {self.port}")
        asyncio.create_task(self.server.serve_forever())

    async def handle_connection(self, reader, writer):
        # Read handshake
        try:
            handshake_data = await reader.readexactly(68)
            # Parse handshake to extract info_hash
            info_hash = handshake_data[28:48]
            
            # Find session for this info_hash
            session = self.session_manager.find_session_by_info_hash(info_hash)
            if not session:
                writer.close()
                return
                
            # Create a peer object
            peer = Peer(
                ip=writer.get_extra_info('peername')[0],
                port=writer.get_extra_info('peername')[1],
                info_hash=info_hash,
                peer_id=session.tracker.peer_id,
                piece_manager=session.piece_manager,
                peer_manager=session.peer_manager
            )
            
            # Assign reader/writer to peer
            peer.reader = reader
            peer.writer = writer
            
            # Send handshake back
            await peer._send_handshake()
            
            # Add peer to peer manager
            await session.peer_manager.add_incoming_peer(peer)
            
            # Start message handler for this peer
            peer.listener_task = asyncio.create_task(peer.listen_for_messages())
        except Exception as e:
            print(f"Error handling incoming connection: {e}")
            writer.close()