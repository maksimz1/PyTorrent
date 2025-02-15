from torrent import Torrent
from tracker import TrackerHandler
from peer import Peer
from piece_manager import PieceManager
import socket
import bitstring
import message
import traceback
from requests import get
import os
import sys

ip = get('https://api.ipify.org').content.decode('utf8')
print(f"My IP: {ip}")

MY_IP = ip

def download_piece(peer: Peer, piece_index: int, piece_length: int) -> bool:
    """
    Download a single piece from a peer. Returns True if successful, False otherwise.
    """
    block_size = 16 * 1024  # Request blocks of 16 KB
    cur_piece_length = 0
    MAX_RETRIES = 3
    
    piece_path = f"file_pieces/{piece_index}.part"
    if not os.path.exists(piece_path):
        with open(piece_path, "wb") as f:
            f.write(b"\x00" * piece_length)
            
    for offset in range(0, piece_length, block_size):
        length = min(block_size, piece_length - offset)
        retries = 0
        
        while retries < MAX_RETRIES:
            try:
                peer.request_piece(piece_index, offset, length)
                response = peer.recv()
                
                if not response:
                    print(f"No response for block at offset {offset}.")
                    retries += 1
                    continue

                piece = message.Message.deserialize(response)

                if isinstance(piece, message.Piece):
                    peer.handle_piece(piece)
                    cur_piece_length += len(piece.block)
                    break
                else:
                    print(f"Got unexpected response type {type(piece)}, retrying...")
                    retries += 1
                    
            except socket.error as e:
                print(f"Socket error while downloading: {e}")
                return False
                
        if retries == MAX_RETRIES:
            print(f"Failed to download block at offset {offset}")
            return False
            
    return cur_piece_length >= piece_length

def initialize_peer(peer: Peer) -> bool:
    """
    Initialize connection with a peer. Returns True if successful, False otherwise.
    Simplified to only send interested message since we're just leeching.
    """
    try:
        print(f"Initializing connection with {peer.ip}:{peer.port}")
        
        # Send interested message right away
        peer.send(message.Interested())
        print(f"Sent interested message")

        # Wait for unchoke
        while True:
            response = peer.recv()
            response_message = message.Message.deserialize(response)
            
            if isinstance(response_message, message.Unchoke):
                print("Peer unchoked us. Ready to request pieces.")
                return True
            elif isinstance(response_message, message.Bitfield):
                print("Got bitfield, continuing to wait for unchoke")
                peer.bitfield = response_message.bitfield
                continue
            elif isinstance(response_message, message.Have):
                print("Got Have message, continuing to wait for unchoke")
                continue
            else:
                print(f"Got unexpected message: {type(response_message)}")
                continue
                
    except socket.error as e:
        print(f"Connection error during initialization: {e}")
        return False

def main(torrent_path: str) -> None:
    # Create an instance of the Torrent class
    tor = Torrent()
    tor.load_file(torrent_path)
    tor.display_info()
    print()

    # Initialize torrent components
    tracker_h = TrackerHandler(tor)
    piece_manager = PieceManager(tor)

    tracker_h.send_request()
    print(f'Tracker response: {tracker_h.response}')
    print()

    # Connect to peers and initialize them only once
    healthy_peers = []
    for peer in tracker_h.peers_list:
        if peer[0] == MY_IP:
            print("Skipping self")
            continue

        try:
            print(f"Connecting to {peer[0]}:{peer[1]}")
            my_peer = Peer(peer[0], peer[1], tracker_h.info_hash, tracker_h.peer_id, piece_manager=piece_manager)
            my_peer.connect()
            if not my_peer.healthy:
                continue
            # Initialize the peer connection once
            if not initialize_peer(my_peer):
                continue
            healthy_peers.append(my_peer)
            my_peer.sock.settimeout(5)
        except (socket.error, ValueError) as e:
            print(f"Connection to {peer[0]}:{peer[1]} failed: {e}")
            continue

    if not healthy_peers:
        print("No healthy peers found. Exiting.")
        return

    # Determine the starting piece (based on what already exists on disk)
    start_piece = len(os.listdir("file_pieces"))

    # Download pieces sequentially using the already-initialized peers
    for piece_index in range(start_piece, tor.total_pieces):
        print(f"\nStarting download of piece {piece_index}")
        piece_downloaded = False

        # Iterate over healthy peers without reinitializing
        for peer in healthy_peers[:]:
            if download_piece(peer, piece_index, tor.piece_length):
                piece_downloaded = True
                print(f"✅ Successfully downloaded piece {piece_index}")
                break
            else:
                print(f"Failed to download piece {piece_index} from {peer.ip}:{peer.port}")
                healthy_peers.remove(peer)

        if not piece_downloaded:
            print(f"❌ Failed to download piece {piece_index} from any peer")
            if not healthy_peers:
                print("No more healthy peers available. Exiting.")
                break

    print("\nDownload complete!")
    print(f"Attempted to download {piece_index + 1} out of {tor.total_pieces} pieces")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <torrent_file>")
        sys.exit(1)
    main(sys.argv[1])