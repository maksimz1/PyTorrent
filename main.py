from torrent import Torrent
from tracker import TrackerHandler
from peer import Peer
from piece_manager import PieceManager
import socket
import message
from requests import get
import os
import sys
import time

# Get our public IP for filtering out self-connection
ip = get('https://api.ipify.org').content.decode('utf8')
print(f"My IP: {ip}")
MY_IP = ip

def download_piece(peer: Peer, piece_index: int, piece_length: int) -> bool:
    """
    Download a single piece from a peer.
    Returns True if the piece was successfully downloaded,
    False otherwise.
    """
    block_size = 16 * 1024  # 16 KB blocks
    cur_piece_length = 0
    MAX_RETRIES = 3

    piece_path = f"file_pieces/{piece_index}.part"
    # Pre-create the file with null data if it doesn't exist
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

                piece_msg = message.Message.deserialize(response)

                if isinstance(piece_msg, message.Piece):
                    peer.handle_piece(piece_msg)
                    cur_piece_length += len(piece_msg.block)
                    break  # Successfully received block, move to next block
                else:
                    print(f"Got unexpected message type {type(piece_msg)}; retrying...")
                    retries += 1

            except socket.error as e:
                print(f"Socket error while downloading: {e}")
                retries += 1

        if retries == MAX_RETRIES:
            print(f"Failed to download block at offset {offset}")
            try:
                os.remove(piece_path)
            except OSError:
                pass
            return False

    if cur_piece_length < piece_length:
        print(f"Piece {piece_index} incomplete (downloaded {cur_piece_length} of {piece_length}). Removing file.")
        try:
            os.remove(piece_path)
        except OSError:
            pass
        return False

    return True

def initialize_peer(peer: Peer) -> bool:
    """
    Initialize connection with a peer.
    Returns True if initialization (e.g. unchoking) is successful.
    """
    try:
        print(f"Initializing connection with {peer.ip}:{peer.port}")
        # Send interested message immediately
        peer.send(message.Interested())
        print("Sent interested message")

        # Wait for an Unchoke or a Bitfield
        while True:
            response = peer.recv()
            response_message = message.Message.deserialize(response)
            
            if isinstance(response_message, message.Unchoke):
                print("Peer unchoked us. Ready to request pieces.")
                return True
            elif isinstance(response_message, message.Bitfield):
                print("Received Bitfield; waiting for unchoke.")
                peer.bitfield = response_message.bitfield
                continue
            elif isinstance(response_message, message.Have):
                print("Received Have message; still waiting for unchoke.")
                continue
            else:
                print(f"Unexpected message: {type(response_message)}")
                continue
    except socket.error as e:
        print(f"Connection error during initialization: {e}")
        return False

def main(torrent_path: str) -> None:
    # Load torrent metadata and display info
    tor = Torrent()
    tor.load_file(torrent_path)
    tor.display_info()
    print()

    # Initialize tracker and PieceManager components
    tracker_h = TrackerHandler(tor)
    piece_manager = PieceManager(tor)

    tracker_h.send_request()
    print(f"Tracker response: {tracker_h.response}\n")

    # Establish connections with peers
    real_peers = []
    for peer_info in tracker_h.peers_list:
        if peer_info[0] == MY_IP:
            print("Skipping self")
            continue
        try:
            print(f"Connecting to {peer_info[0]}:{peer_info[1]}")
            my_peer = Peer(peer_info[0], peer_info[1], tracker_h.info_hash, tracker_h.peer_id, piece_manager=piece_manager)
            my_peer.connect()
            if not my_peer.healthy:
                continue
            real_peers.append(my_peer)
            my_peer.sock.settimeout(5)
        except (socket.error, ValueError) as e:
            print(f"Connection to {peer_info[0]}:{peer_info[1]} failed: {e}")
            continue

    if not real_peers:
        print("No healthy peers found. Exiting.")
        return

    healthy_peers = []
    for peer in real_peers:
        if not initialize_peer(peer):
            continue
        healthy_peers.append(peer)

    # Main download loop: assign pieces to peers dynamically
    while True:
        assigned = False
        # Iterate over a copy of the healthy peers list so we can remove peers if needed
        for peer in healthy_peers[:]:
            # Use the peer's bitfield if available, otherwise assume the peer has all pieces
            peer_bitfield = peer.bitfield if hasattr(peer, "bitfield") and peer.bitfield is not None else None
            next_piece = piece_manager.choose_next_piece(peer_bitfield)
            if next_piece is not None:
                assigned = True
                expected_length = piece_manager.pieces[next_piece].piece_length
                print(f"\nStarting download of piece {next_piece} (expected length: {expected_length}) from {peer.ip}:{peer.port}")
                if download_piece(peer, next_piece, expected_length):
                    print(f"✅ Successfully downloaded piece {next_piece}")
                else:
                    print(f"❌ Failed to download piece {next_piece} from {peer.ip}:{peer.port}")
                    healthy_peers.remove(peer)
            else:
                print(f"No available piece for peer {peer.ip}:{peer.port}")

        # If no assignment was made across all peers, break the loop
        if not assigned:
            break
        time.sleep(1)  # small delay to avoid a tight busy loop

    print("\nDownload complete!")
    print(f"Attempted to download all {tor.total_pieces} pieces")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <torrent_file>")
        sys.exit(1)
    main(sys.argv[1])
