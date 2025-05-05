import asyncio
import time
import socket
import traceback
import os
import hashlib
from network.peer import Peer
from core.tracker import TrackerHandler
from core.piece_manager import PieceManager
from protocol.message import Interested
from protocol.extensions.pex import PEXExtension
from enum import Enum
import protocol.message as message

# Download settings
BLOCK_SIZE = 16 * 1024  # 16 KB blocks

class SessionState:
    STOPPED = 0
    DOWNLOADING = 1
    PAUSED = 2
    COMPLETED = 3
    ERROR = 4

class AsyncPeerManager:
    def __init__(self, session, tracker: TrackerHandler, piece_manager: PieceManager, my_ip,):
        self.tracker = tracker
        self.piece_manager = piece_manager
        self.my_ip = my_ip
        self.session = session

        # Add PEX-specific attributes
        self.known_peers = {}  # (ip, port) -> timestamp
        self.pex_peers = {}    # (ip, port) -> timestamp
        self.connected_peers = {}  # (ip, port) -> peer
        self.peer_processors = {}  # (ip, port) -> asyncio.Task

        # Tasks for managing peer connections
        self.tasks = set()
        
        # Stats for tracking peer sources
        self.stats = {
            "tracker_peers": 0,
            "pex_peers": 0,
            "connected_from_tracker": 0,
            "connected_from_pex": 0,
            "pieces_downloaded": 0
        }

    def add_peer(self, ip: str, port: int, source: str = "tracker"):
        """Add a peer to the known peers list."""
        if ip == self.my_ip:
            return  # Don't add ourselves
        
        # Validate IP and port
        try:
            socket.inet_aton(ip)
            if not isinstance(port, int) or port <= 0 or port > 65535:
                return
        except:
            return
        
        is_new = (ip, port) not in self.known_peers
        self.known_peers[(ip, port)] = time.time()
        
        if source == "pex":
            self.pex_peers[(ip, port)] = time.time()
            if is_new:
                self.stats["pex_peers"] += 1
                print(f"Added peer {ip}:{port} from {source}")
        elif is_new:
            self.stats["tracker_peers"] += 1

    async def _connect_peer(self, peer: Peer):
        """Connect to peer and initialize PEX extension."""
        await peer.connect()
        if peer.healthy:
            # Store the connected peer
            self.connected_peers[(peer.ip, peer.port)] = peer

            # Start message processor for this peer
            peer_key = (peer.ip, peer.port)
            self.peer_processors[peer_key] = asyncio.create_task(self.process_peer_messages(peer))

            # Update connection stats
            if hasattr(peer, 'source'):
                if peer.source == "pex":
                    self.stats["connected_from_pex"] += 1
                elif peer.source == "tracker":
                    self.stats["connected_from_tracker"] += 1
            
            # Send extended handshake after a brief delay
            await asyncio.sleep(0.2)  # Short delay before sending extended handshake
            await peer.pex.send_extended_handshake()
            
            return peer
        else:
            print(f"Peer {peer.ip}:{peer.port} not healthy, skipping.")
            return None

    async def add_peer_info(self, peer_info, source="tracker"):
        """Add a peer from a (ip, port) tuple."""
        if peer_info[0] == self.my_ip:
            print("Skipping self.")
            return
        
        # First add to known peers
        self.add_peer(peer_info[0], peer_info[1], source=source)
        
        peer = Peer(
            peer_info[0],
            peer_info[1],
            self.tracker.info_hash,
            self.tracker.peer_id,
            self.piece_manager,
            self
        )
        
        # Set source for tracking
        peer.source = source
        
        connected_peer = await self._connect_peer(peer)
        if connected_peer:
            # Spawn a dedicated task to handle this peer immediately.
            self.tasks.add(asyncio.create_task(self.handle_peer(connected_peer)))

    async def add_peers(self):
        """Connect to all peers from the tracker at once."""
        tasks = []
        for peer_info in self.tracker.peers_list:
            tasks.append(asyncio.create_task(self.add_peer_info(peer_info, source="tracker")))
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def connect_to_pex_peers(self, peers_list):
        """Connect to newly discovered PEX peers."""
        
        # # Dont try to connect to new peers, unless we have less then 40 peers connected
        # if len(self.connected_peers) >= 40:
        #     return
        # Don't try to connect to peers we're already connected to
        unconnected_peers = [(ip, port) for ip, port in peers_list 
                            if (ip, port) not in self.connected_peers]
        
        if not unconnected_peers:
            return
        
        print(f"Attempting to connect to {len(unconnected_peers)} new PEX peers...")
        
        # Connect to all new peers at once
        tasks = []
        for ip, port in unconnected_peers:
            tasks.append(asyncio.create_task(self.add_peer_info((ip, port), source="pex")))
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        connected_count = sum(1 for ip, port in unconnected_peers if (ip, port) in self.connected_peers)
        print(f"Successfully connected to {connected_count}/{len(unconnected_peers)} new PEX peers")
    
    async def share_peers_via_pex(self):
        """Share known peers with all connected peers that support PEX."""
        now = time.time()
        peers_shared = 0
        
        # To reduce overhead, we limit the PEX when we have enough peers
        if len(self.connected_peers) >= 40:
            return
        
        # Create a list of all known peer addresses
        all_peers = list(self.known_peers.keys())
        
        for (peer_ip, peer_port), peer in list(self.connected_peers.items()):
            if hasattr(peer, 'pex') and peer.pex.supports_pex:
                if now - peer.pex.last_pex_sent >= peer.pex.pex_interval:
                    # Don't send a peer its own address
                    peer_list = [(ip, port) for (ip, port) in all_peers 
                                if (ip, port) != (peer_ip, peer_port)]
                    
                    if peer.writer and not peer.writer.is_closing():
                        peer.pex.send_pex_message(peer_list, self.my_ip)
                        peers_shared += 1
        
        if peers_shared > 0:
            print(f"Shared peer lists with {peers_shared} PEX-enabled peers")

    async def remove_peer(self, peer: Peer):
        """
        Remove a peer from tracking and update stats.
        Calls peer's disconnect method to clean up connection.
        """

        peer_key = (peer.ip, peer.port)
        if peer_key in self.peer_processors:
            # Cancel the message processing task
            self.peer_processors[peer_key].cancel()
            del self.peer_processors[peer_key]

        # Disconnect the peer (using the peer's own method)
        await peer.disconnect()
        
        # Remove from tracking and update stats
        peer_key = (peer.ip, peer.port)
        if peer_key in self.connected_peers:
            # Update source-based stats
            source = getattr(peer, 'source', 'unknown')
            if source == "pex":
                self.stats["connected_from_pex"] -= 1
            elif source == "tracker":
                self.stats["connected_from_tracker"] -= 1
            
            # Remove from connected peers
            del self.connected_peers[peer_key]


    async def process_peer_messages(self, peer: Peer):
        """Process incoming messages from a peer."""
        peer_key = (peer.ip, peer.port)

        try:
            while True:
                # Wait for a message from the peer
                msg = await peer.message_queue.get()
                try: 
                    if isinstance(msg, message.Unchoke):
                        peer.handle_unchoke()
                    if isinstance(msg, message.Choke):
                        peer.handle_choke()
                    elif isinstance(msg, message.Bitfield):
                        peer.handle_bitfield(msg)
                    elif isinstance(msg, message.Have):
                        peer.handle_have(msg)
                    elif isinstance(msg, message.Request):
                        # Handle Request messages by sending the requested piece
                        print(f"🔵 Received Request message from {peer.ip}:{peer.port} - piece: {msg.index}, offset: {msg.begin}, length: {msg.length}")
                        await peer.handle_request(msg)
                    elif isinstance(msg, message.Interested):
                        print(f"🔵 Received Interested message from {peer.ip}:{peer.port}")
                        # Handle Interested message
                        await peer.handle_interested()
                    elif isinstance(msg, message.NotInterested):
                        # Update peer state
                        peer.state['peer_interested'] = False
                        print(f"Peer {peer.ip}:{peer.port} is no longer interested in our pieces")

                except Exception as e:
                    print(f"Error processing message from {peer.ip}:{peer.port}: {e}")

                peer.message_queue.task_done()

        except asyncio.CancelledError:
            print(f"Peer {peer.ip}:{peer.port} message processing cancelled")

        finally:
            # Clean up if needed
            if peer_key in self.peer_processors:
                del self.peer_processors[peer_key]
        
    async def handle_peer(self, peer: Peer):
        """
        Handle initialization and piece downloading for a single peer.
        This method can be spawned as an independent task.
        """
        try:
            # Send Interested message
            await peer.send(Interested())
            print(f"Sent 'interested' message to {peer.ip}:{peer.port}")

            # Wait for the peer to unchoke and (optionally) provide a Bitfield.
            start_time = asyncio.get_event_loop().time()
            unchoke_wait_time = 10  # Maximum seconds to wait for unchoke
            
            while asyncio.get_event_loop().time() - start_time < unchoke_wait_time:
                if not peer.is_choking():
                    if peer.bitfield is not None:
                        print(f"Peer {peer.ip}:{peer.port} is ready (Bitfield received).")
                        break
                    elif asyncio.get_event_loop().time() - start_time > 0.5:
                        print(f"Peer {peer.ip}:{peer.port} unchoked but no Bitfield received quickly; assuming full availability.")
                        total = peer.piece_manager.number_of_pieces
                        peer.bitfield = [1] * total
                        break
                
                # Periodically resend interested (some clients need this)
                if asyncio.get_event_loop().time() - start_time > 3:
                    await peer.send(Interested())
                
                await asyncio.sleep(0.5)
            
            # If the peer still hasn't unchoked us, print a warning but continue
            if peer.is_choking():
                print(f"Warning: Peer {peer.ip}:{peer.port} did not unchoke us within {unchoke_wait_time} seconds")
                await self.remove_peer(peer)
                return
                
            # Print bitfield info if available
            if peer.bitfield is not None:
                self.debug_bitfield(peer.bitfield, peer.ip, peer.port)

            # Now start downloading pieces from this peer.
            failure_count = 0
            max_failures = 5  # Increased max failures
            
            while True:
                # Check session state - pause downloading if needed
                if self.session.state == SessionState.PAUSED:
                    await asyncio.sleep(0.5)
                    continue
                elif self.session.state == SessionState.COMPLETED:
                    print("Session completed, stopping peer download.")
                    break
                elif self.session.state == SessionState.STOPPED:
                    print("Session stopped, breaking out of peer download loop.")
                    break


                # If the peer is choking us, we can't download
                if peer.is_choking():
                    print(f"Peer {peer.ip}:{peer.port} is choking us, waiting...")
                    await peer.send(Interested())  # Resend interested
                    await asyncio.sleep(3)
                    continue
                
                # Use PieceManager to select the next piece
                next_piece = self.piece_manager.choose_next_piece(peer.bitfield)
                if next_piece is None:
                    print(f"No available piece for peer {peer.ip}:{peer.port}")
                    print(f"State of session: {self.session.state}")
                    await asyncio.sleep(5)  # Wait before checking again
                    continue
                    
                expected_length = peer.piece_manager.pieces[next_piece].piece_length
                print(f"Starting download of piece {next_piece} from {peer.ip}:{peer.port}")
                
                success = await self.async_download_piece(peer, next_piece, expected_length)
                if success:
                    print(f"✅ Successfully downloaded piece {next_piece} from {peer.ip}:{peer.port}")
                    self.stats["pieces_downloaded"] += 1
                    failure_count = 0  # Reset on success.
                else:
                    print(f"❌ Failed to download piece {next_piece} from {peer.ip}:{peer.port}")
                    self.piece_manager.release_piece(next_piece)
                    failure_count += 1
                    if failure_count >= max_failures:
                        print(f"Peer {peer.ip}:{peer.port} has failed too many times. Giving up on this peer.")
                        break
                    else:
                        print(f"Retrying with peer {peer.ip}:{peer.port}. Failure count: {failure_count}")
                        await asyncio.sleep(1)  # Give the peer a short break.
                await asyncio.sleep(0.1)
        except Exception as e:
            print(f"Error with peer {peer.ip}:{peer.port}: {e}")
            traceback.print_exc()
        finally:
            # Clean up if the peer is no longer usable
            if (peer.ip, peer.port) in self.connected_peers:
                # Update stats before removing
                if hasattr(peer, 'source'):
                    if peer.source == "pex":
                        self.stats["connected_from_pex"] -= 1
                    elif peer.source == "tracker":
                        self.stats["connected_from_tracker"] -= 1
                del self.connected_peers[(peer.ip, peer.port)]
                print(f"Removed peer {peer.ip}:{peer.port} from connected peers")

    async def async_download_piece(self, peer: Peer, piece_index: int, piece_length: int) -> bool:
        """
        Download a single piece from a peer with robust error handling and validation.
        Less verbose output - focusing only on overall piece progress.
        """
        # Check session state before starting - don't download if paused
        if self.session and self.session.state == SessionState.PAUSED:
            return False
        
        # Validate piece length
        if piece_length <= 0:
            print(f"Error: Cannot download piece {piece_index} with invalid length {piece_length}")
            
            # Try to get correct piece length from piece manager
            try:
                correct_length = peer.piece_manager.pieces[piece_index].piece_length
                if correct_length > 0:
                    print(f"Using correct piece length from piece manager: {correct_length}")
                    piece_length = correct_length
                else:
                    print(f"Piece manager also has invalid length for piece {piece_index}")
                    return False
            except Exception as e:
                print(f"Could not get correct piece length: {e}")
                return False
        
        block_size = BLOCK_SIZE
        cur_piece_length = 0
        MAX_RETRIES = 3
        
        # Download the piece block by block - with minimal output
        for offset in range(0, piece_length, block_size):
            # Check session state again before each block - allow pausing mid-piece
            if self.session and self.session.state == SessionState.PAUSED:
                return False
                
            length = min(block_size, piece_length - offset)
            retries = 0
            
            while retries < MAX_RETRIES:
                try:
                    # No output for every block request to reduce verbosity
                    await peer.request_piece(piece_index, offset, length)
                    
                    # Wait for the matching Piece message
                    piece_msg = await peer.get_piece_message(piece_index, offset, timeout=5)
                    
                    # Process the received block
                    if piece_msg and piece_msg.block:
                        block_length = len(piece_msg.block)
                        
                        # Only output for empty blocks
                        if block_length == 0:
                            print(f"Warning: Received empty block from {peer.ip}:{peer.port}, retrying")
                            retries += 1
                            continue
                        
                        await peer.handle_piece(piece_msg)
                        cur_piece_length += block_length
                        
                        # Only show piece progress once in a while
                        if offset % (block_size * 10) == 0 or offset + length >= piece_length:
                            progress_percent = (cur_piece_length/piece_length)*100
                            print(f"\rPiece {piece_index}: {progress_percent:.1f}%", end="", flush=True)
                        
                        break  # Success, move to next block
                    else:
                        retries += 1
                except asyncio.TimeoutError:
                    retries += 1
                except Exception as e:
                    print(f"Error downloading block at offset {offset}: {e}")
                    retries += 1
                    
            if retries == MAX_RETRIES:
                print(f"\nFailed to download block at offset {offset} after {MAX_RETRIES} retries")
                return False

        # Print newline after piece completion for cleaner output
        print()

        # Verify we got all the data
        if cur_piece_length < piece_length:
            print(f"Piece {piece_index} incomplete (downloaded {cur_piece_length} of {piece_length}).")
            return False

        if piece_index in peer.piece_manager.completed_pieces:
            return True
        
        return False
        
    def debug_bitfield(self, bitfield, peer_ip, peer_port):
        """Print detailed information about a peer's bitfield."""
        if bitfield is None:
            print(f"Peer {peer_ip}:{peer_port} has no bitfield!")
            return
        
        total_pieces = len(bitfield)
        available_pieces = sum(1 for bit in bitfield if bit)
        percentage = (available_pieces / total_pieces) * 100 if total_pieces > 0 else 0
        
        print(f"Peer {peer_ip}:{peer_port} bitfield stats:")
        print(f"  - Total pieces: {total_pieces}")
        print(f"  - Available pieces: {available_pieces} ({percentage:.2f}%)")