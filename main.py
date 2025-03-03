import asyncio
import os
import sys
import random
import time
import socket
import bencodepy
import traceback
import hashlib
from torrent import Torrent
from tracker import TrackerHandler
from peer import Peer
from piece_manager import PieceManager
import message
from requests import get

# Connection settings
CONNECTION_TIMEOUT = 10  # Seconds to wait for connection
MAX_CONCURRENT_CONNECTIONS = 50  # Increased for parallel connections

# Download settings
BLOCK_SIZE = 16 * 1024  # 16 KB blocks

# Retrieve your public IP (synchronously for now)
try:
    MY_IP = get('https://api.ipify.org').content.decode('utf8')
    print(f"My IP: {MY_IP}")
except Exception as e:
    print(f"Warning: Could not determine public IP - {e}")
    MY_IP = "127.0.0.1"  # Fallback to localhost

def debug_bitfield(bitfield, peer_ip, peer_port):
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
    
    # Print a sample of the bitfield (first 20 pieces)
    sample_size = min(20, total_pieces)
    sample = bitfield[:sample_size]
    print(f"  - First {sample_size} pieces: {sample}")

# Improved implementation of async_download_piece function
async def async_download_piece(peer: Peer, piece_index: int, piece_length: int) -> bool:
    """
    Download a single piece from a peer with robust error handling and validation.
    """
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

    # Create directory for piece files if it doesn't exist
    os.makedirs("file_pieces", exist_ok=True)
    
    piece_path = f"file_pieces/{piece_index}.part"
    if os.path.exists(piece_path):
        os.remove(piece_path)
    
    # Pre-allocate the file with correct size
    with open(piece_path, "wb") as f:
        f.write(b"\x00" * piece_length)

    # Calculate correct number of blocks
    num_blocks = (piece_length + block_size - 1) // block_size
    print(f"Downloading piece {piece_index} ({piece_length} bytes) in {num_blocks} blocks")
    
    # Download the piece block by block
    for offset in range(0, piece_length, block_size):
        length = min(block_size, piece_length - offset)
        retries = 0
        
        while retries < MAX_RETRIES:
            try:
                print(f"  - Requesting block at offset {offset}/{piece_length} (length: {length})")
                await peer.request_piece(piece_index, offset, length)
                
                # Wait for the matching Piece message
                print(f"  - Waiting for piece message (timeout: 5s)")
                piece_msg = await peer.get_piece_message(piece_index, offset, timeout=5)
                
                # Process the received block
                if piece_msg and piece_msg.block:
                    block_length = len(piece_msg.block)
                    print(f"  - Received block with length {block_length}")
                    
                    # Ensure block is not empty
                    if block_length == 0:
                        print(f"  - Warning: Received empty block, retrying")
                        retries += 1
                        continue
                    
                    await peer.handle_piece(piece_msg)
                    cur_piece_length += block_length
                    progress_percent = (cur_piece_length/piece_length)*100
                    print(f"  - Progress: {cur_piece_length}/{piece_length} bytes ({progress_percent:.1f}%)")
                    break  # Success, move to next block
                else:
                    print(f"  - Received invalid piece message, retrying")
                    retries += 1
            except asyncio.TimeoutError:
                print(f"  - Timeout waiting for block at offset {offset}")
                retries += 1
            except Exception as e:
                print(f"  - Error downloading block at offset {offset}: {e}")
                retries += 1
                
        if retries == MAX_RETRIES:
            print(f"Failed to download block at offset {offset} after {MAX_RETRIES} retries")
            try:
                os.remove(piece_path)
            except OSError:
                pass
            return False

    # Verify we got all the data
    if cur_piece_length < piece_length:
        print(f"Piece {piece_index} incomplete (downloaded {cur_piece_length} of {piece_length}). Removing file.")
        try:
            os.remove(piece_path)
        except OSError:
            pass
        return False

    # Validate hash
    print(f"Validating piece {piece_index}...")
    piece_hash = peer.piece_manager.pieces[piece_index].piece_hash
    
    with open(piece_path, "rb") as f:
        piece_data = f.read()
        actual_hash = hashlib.sha1(piece_data).digest()
        
        if actual_hash != piece_hash:
            print(f"Hash validation failed for piece {piece_index}")
            print(f"Expected: {piece_hash.hex()}")
            print(f"Actual:   {actual_hash.hex()}")
            try:
                os.remove(piece_path)
            except OSError:
                pass
            return False
    
    print(f"Hash validation successful for piece {piece_index}")
    return True

class AsyncPeerManager:
    def __init__(self, tracker: TrackerHandler, piece_manager: PieceManager, my_ip):
        self.tracker = tracker
        self.piece_manager = piece_manager
        self.my_ip = my_ip
        
        # Add PEX-specific attributes
        self.known_peers = {}  # (ip, port) -> timestamp
        self.pex_peers = {}    # (ip, port) -> timestamp
        self.connected_peers = {}  # (ip, port) -> peer
        
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
        """Connect to peer and initialize PEX."""
        await peer.connect()
        if peer.healthy:
            # Set this peer's PEX manager reference
            peer.pex_manager = self
            
            # Store the connected peer
            self.connected_peers[(peer.ip, peer.port)] = peer
            
            # Update connection stats
            if hasattr(peer, 'source'):
                if peer.source == "pex":
                    self.stats["connected_from_pex"] += 1
                elif peer.source == "tracker":
                    self.stats["connected_from_tracker"] += 1
            
            # Send extended handshake after a brief delay
            await asyncio.sleep(0.2)  # Short delay before sending extended handshake
            await peer.send_extended_handshake()
            
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
            self.piece_manager
        )
        
        # Set source for tracking
        peer.source = source
        
        connected_peer = await self._connect_peer(peer)
        if connected_peer:
            # Spawn a dedicated task to handle this peer immediately.
            asyncio.create_task(self.handle_peer(connected_peer))

    async def add_peers(self):
        """Connect to all peers from the tracker at once."""
        tasks = []
        for peer_info in self.tracker.peers_list:
            tasks.append(asyncio.create_task(self.add_peer_info(peer_info, source="tracker")))
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def connect_to_pex_peers(self, peers_list):
        """Connect to newly discovered PEX peers."""
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
        
        # Create a list of all known peer addresses
        all_peers = list(self.known_peers.keys())
        
        for (peer_ip, peer_port), peer in list(self.connected_peers.items()):
            if peer.supports_pex:
                if now - peer.last_pex_sent >= peer.pex_interval:
                    # Don't send a peer its own address
                    peer_list = [(ip, port) for (ip, port) in all_peers 
                                if (ip, port) != (peer_ip, peer_port)]
                    
                    await peer.send_pex_message(peer_list, self.my_ip)
                    peers_shared += 1
        
        if peers_shared > 0:
            print(f"Shared peer lists with {peers_shared} PEX-enabled peers")

    async def handle_peer(self, peer: Peer):
        """
        Handle initialization and piece downloading for a single peer.
        This method can be spawned as an independent task.
        """
        try:
            # Send Interested message
            await peer.send(message.Interested())
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
                    await peer.send(message.Interested())
                
                await asyncio.sleep(0.5)
            
            # If the peer still hasn't unchoked us, print a warning but continue
            if peer.is_choking():
                print(f"Warning: Peer {peer.ip}:{peer.port} did not unchoke us within {unchoke_wait_time} seconds")
                
            # Print bitfield info if available
            if peer.bitfield is not None:
                debug_bitfield(peer.bitfield, peer.ip, peer.port)

            # Now start downloading pieces from this peer.
            failure_count = 0
            max_failures = 5  # Increased max failures
            
            while True:
                # If the peer is choking us, we can't download
                if peer.is_choking():
                    print(f"Peer {peer.ip}:{peer.port} is choking us, waiting...")
                    await peer.send(message.Interested())  # Resend interested
                    await asyncio.sleep(3)
                    continue
                
                # Use PieceManager to select the next piece
                next_piece = self.piece_manager.choose_next_piece(peer.bitfield)
                if next_piece is None:
                    print(f"No available piece for peer {peer.ip}:{peer.port}")
                    await asyncio.sleep(5)  # Wait before checking again
                    continue
                    
                expected_length = peer.piece_manager.pieces[next_piece].piece_length
                print(f"\nStarting download of piece {next_piece} from {peer.ip}:{peer.port}")
                
                success = await async_download_piece(peer, next_piece, expected_length)
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

async def async_main(torrent_path: str):
    # Create directory for file pieces
    os.makedirs("file_pieces", exist_ok=True)
    
    # Load torrent metadata and display info
    tor = Torrent()
    tor.load_file(torrent_path)
    tor.display_info()

    # Initialize tracker and piece manager
    tracker = TrackerHandler(tor)
    piece_manager = PieceManager(tor)

    # Send tracker request (in a thread since requests is blocking)
    await asyncio.to_thread(tracker.send_request)
    print(f"Tracker response: {tracker.response}\n")

    # Create the peer manager and add peers from the tracker.
    peer_manager = AsyncPeerManager(tracker, piece_manager, MY_IP)
    await peer_manager.add_peers()

    # Print initial connection stats
    print(f"\n=== Initial Connection Status ===")
    print(f"Connected to {len(peer_manager.connected_peers)} peers total")
    print(f"  - {peer_manager.stats['connected_from_tracker']} from tracker")
    print(f"  - {peer_manager.stats['connected_from_pex']} from PEX")

    # Main loop for PEX operations
    pex_round = 0
    while True:
        pex_round += 1
        print(f"\n=== PEX Round {pex_round} ===")
        
        # Clean up disconnected peers
        for (ip, port), peer in list(peer_manager.connected_peers.items()):
            if peer.writer is None or peer.writer.is_closing():
                print(f"Removing disconnected peer {ip}:{port}")
                source = getattr(peer, 'source', 'unknown')
                if source == "pex":
                    peer_manager.stats["connected_from_pex"] -= 1
                elif source == "tracker":
                    peer_manager.stats["connected_from_tracker"] -= 1
                del peer_manager.connected_peers[(ip, port)]
        
        # Share peers via PEX
        await peer_manager.share_peers_via_pex()
        
        # Periodically refresh tracker to get new peers if connection count is low
        if len(peer_manager.connected_peers) < 5 and pex_round % 5 == 0:
            print("Low connection count, refreshing tracker...")
            try:
                await asyncio.to_thread(tracker.send_request)
                print(f"Refreshed tracker response with {len(tracker.peers_list)} peers")
                
                # Connect to new peers from tracker
                tasks = []
                for peer_info in tracker.peers_list:
                    if (peer_info[0], peer_info[1]) not in peer_manager.connected_peers:
                        tasks.append(asyncio.create_task(
                            peer_manager.add_peer_info(peer_info, source="tracker")
                        ))
                        
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                print(f"Error refreshing tracker: {e}")
        
        # Print status report
        print("\n=== PEX Status Report ===")
        print(f"Connected peers: {len(peer_manager.connected_peers)} total")
        print(f"  - {peer_manager.stats['connected_from_tracker']} from tracker")
        print(f"  - {peer_manager.stats['connected_from_pex']} from PEX")
        print(f"Known peers: {len(peer_manager.known_peers)} total")
        print(f"  - {peer_manager.stats['tracker_peers']} from tracker")
        print(f"  - {peer_manager.stats['pex_peers']} from PEX")
        print(f"Pieces downloaded: {peer_manager.stats['pieces_downloaded']}")
        
        # Print details of connected peers
        if peer_manager.connected_peers:
            print("\nActive connections:")
            for (ip, port), peer in list(peer_manager.connected_peers.items()):
                supports_pex = "with PEX" if peer.supports_pex else "no PEX"
                source = getattr(peer, 'source', 'unknown')
                print(f"  - {ip}:{port} ({supports_pex}, from {source})")
        else:
            print("\nNo active connections")
        
        await asyncio.sleep(10)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <torrent_file>")
        sys.exit(1)
    
    # Make sure the Extended message type is registered in Message's message map
    if message.Message._message_map is None:
        message.Message._build_message_map()
    
    print("Starting Enhanced BitTorrent Client with PEX support")
    print("---------------------------------------------------")
    
    # Run the main function
    try:
        asyncio.run(async_main(sys.argv[1]))
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Error in main loop: {e}")
        traceback.print_exc()