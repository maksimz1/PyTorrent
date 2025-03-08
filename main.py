import asyncio
import os
import sys
import traceback
import time
from requests import get

# Import from our reorganized modules
from core.torrent import Torrent
from core.tracker import TrackerHandler
from core.piece_manager import PieceManager
from network.peer_manager import AsyncPeerManager
from protocol.message import Message, Extended

# Retrieve your public IP (synchronously for now)
try:
    MY_IP = get('https://api.ipify.org').content.decode('utf8')
    print(f"My IP: {MY_IP}")
except Exception as e:
    print(f"Warning: Could not determine public IP - {e}")
    MY_IP = "127.0.0.1"  # Fallback to localhost

# Custom progress bar function
def print_progress_bar(percentage, width=50):
    """Display a text-based progress bar"""
    completed = int(width * percentage / 100)
    remaining = width - completed
    bar = '█' * completed + '░' * remaining
    print(f"\r[{bar}] {percentage:.2f}%", end='', flush=True)

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

    # Variable to track last progress display time
    last_progress_update = time.time()
    
    # Last progress percentage to avoid repeated printing of the same value
    last_progress_percentage = -1
    
    # Main loop for PEX operations
    pex_round = 0
    completed = False
    
    while not completed:
        pex_round += 1
        print(f"\n\n=== PEX Round {pex_round} ===")
        
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
        
        # Check if download is complete
        if piece_manager.is_complete():
            print("\n\n🎉 Download completed! 🎉")
            print(f"All {piece_manager.stats['pieces_completed']} pieces downloaded successfully.")
            
            # Assemble the final file
            output_path = assemble_file(piece_manager, tor)
            print(f"\nFile assembled and saved to: {output_path}")
            
            # Send completed event to tracker
            print("Sending completed event to tracker...")
            await asyncio.to_thread(lambda: tracker.send_request("completed"))
            
            completed = True
            break
            
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
        
        # Update progress more frequently than PEX status
        current_time = time.time()
        if current_time - last_progress_update >= 2:  # Update every 2 seconds
            progress = piece_manager.get_progress()
            
            # Only print if progress changed
            if abs(progress - last_progress_percentage) > 0.01:
                print("\nDownload Progress:")
                print_progress_bar(progress)
                print(f"\nPieces: {len(piece_manager.completed_pieces)}/{piece_manager.number_of_pieces}")
                last_progress_percentage = progress
                
            last_progress_update = current_time
        
        # Print status report less frequently
        print("\n\n=== PEX Status Report ===")
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
                supports_pex = "with PEX" if hasattr(peer, 'pex') and peer.pex.supports_pex else "no PEX"
                source = getattr(peer, 'source', 'unknown')
                print(f"  - {ip}:{port} ({supports_pex}, from {source})")
        else:
            print("\nNo active connections")
        
        await asyncio.sleep(10)

    # Clean shutdown
    print("\nPerforming clean shutdown...")
    
    # Close all peer connections
    for (ip, port), peer in list(peer_manager.connected_peers.items()):
        if peer.writer:
            try:
                peer.writer.close()
                await peer.writer.wait_closed()
                print(f"Closed connection to {ip}:{port}")
            except Exception as e:
                print(f"Error closing connection to {ip}:{port}: {e}")
    
    # Final tracker update with 'stopped' event
    try:
        print("Sending stopped event to tracker...")
        await asyncio.to_thread(lambda: tracker.send_request("stopped"))
    except Exception as e:
        print(f"Error sending stopped event to tracker: {e}")
    
    print("Shutdown complete. Goodbye!")

def assemble_file(piece_manager, torrent):
    """Assemble the final file from the downloaded pieces."""
    # Determine output path
    output_path = torrent.name
    
    print(f"Assembling file from {piece_manager.number_of_pieces} pieces...")
    
    # Create output directory for multi-file torrents
    if len(torrent.files) > 1:
        os.makedirs(output_path, exist_ok=True)
        
        # TODO: Handle multi-file torrents
        print("Multi-file torrents not fully implemented yet")
        return output_path
    
    # For single file torrents
    with open(output_path, 'wb') as output_file:
        for i in range(piece_manager.number_of_pieces):
            piece_path = f"file_pieces/{i}.part"
            
            if not os.path.exists(piece_path):
                print(f"Warning: Piece {i} is missing, output may be incomplete")
                continue
                
            with open(piece_path, 'rb') as piece_file:
                output_file.write(piece_file.read())
    
    return output_path

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <torrent_file>")
        sys.exit(1)
    
    # Make sure the Extended message type is registered in Message's message map
    if Message._message_map is None:
        Message._build_message_map()
    Message._message_map[20] = Extended
    
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