import asyncio
import os
import sys
from torrent import Torrent
from tracker import TrackerHandler
from peer import Peer
from piece_manager import PieceManager
import message
from requests import get

# Retrieve your public IP (synchronously for now)
MY_IP = get('https://api.ipify.org').content.decode('utf8')
print(f"My IP: {MY_IP}")

async def async_download_piece(peer: Peer, piece_index: int, piece_length: int) -> bool:
    block_size = 16 * 1024  # 16 KB blocks
    cur_piece_length = 0
    MAX_RETRIES = 3

    piece_path = f"file_pieces/{piece_index}.part"
    if os.path.exists(piece_path):
        os.remove(piece_path)
    with open(piece_path, "wb") as f:
        f.write(b"\x00" * piece_length)

    for offset in range(0, piece_length, block_size):
        length = min(block_size, piece_length - offset)
        retries = 0
        while retries < MAX_RETRIES:
            try:
                await peer.request_piece(piece_index, offset, length)
                # Wait for the matching Piece message from the continuous listener queue
                piece_msg = await peer.get_piece_message(piece_index, offset, timeout=3)
                await peer.handle_piece(piece_msg)
                cur_piece_length += len(piece_msg.block)
                break  # Move on to the next block
            except Exception as e:
                print(f"Error downloading block at offset {offset}: {e}")
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

class AsyncPeerManager:
    def __init__(self, tracker: TrackerHandler, piece_manager: PieceManager, my_ip):
        self.tracker = tracker
        self.piece_manager = piece_manager
        self.my_ip = my_ip
        self.peers = []

    async def _connect_peer(self, peer: Peer):
        await peer.connect()
        if peer.healthy:
            return peer
        else:
            print(f"Peer {peer.ip}:{peer.port} not healthy, skipping.")
            return None

    async def add_peers(self):
        tasks = []
        for peer_info in self.tracker.peers_list:
            if peer_info[0] == self.my_ip:
                print("Skipping self.")
                continue
            peer = Peer(
                peer_info[0],
                peer_info[1],
                self.tracker.info_hash,
                self.tracker.peer_id,
                self.piece_manager
            )
            tasks.append(asyncio.create_task(self._connect_peer(peer)))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        self.peers = [p for p in results if p is not None and not isinstance(p, Exception)]
        print(f"Connected peers: {[f'{p.ip}:{p.port}' for p in self.peers]}")

    async def initialize_and_download_peer(self, peer: Peer):
        """
        For a single peer, send the Interested message, wait briefly for an unchoke,
        and then start downloading pieces immediately—even if no Bitfield is received quickly.
        Peers are given multiple chances before being dropped.
        """
        try:
            # Start initialization by sending Interested.
            await peer.send(message.Interested())
            start_time = asyncio.get_event_loop().time()
            # Wait until unchoked; if Bitfield doesn't arrive quickly, assume full availability.
            while True:
                if not peer.is_choking():
                    if peer.bitfield is not None:
                        print(f"Peer {peer.ip}:{peer.port} is ready (Bitfield received).")
                        break
                    elif asyncio.get_event_loop().time() - start_time > 0.5:
                        print(f"Peer {peer.ip}:{peer.port} unchoked but no Bitfield received quickly; assuming full availability.")
                        total = peer.piece_manager.number_of_pieces
                        peer.bitfield = [1] * total
                        break
                await asyncio.sleep(0.1)
            
            # Allow the peer several chances before giving up.
            failure_count = 0
            max_failures = 3  # Allow up to 3 consecutive failures.
            while True:
                next_piece = self.piece_manager.choose_next_piece(peer.bitfield)
                if next_piece is None:
                    print(f"No available piece for peer {peer.ip}:{peer.port}")
                    break
                expected_length = self.piece_manager.pieces[next_piece].piece_length
                print(f"\nStarting download of piece {next_piece} from {peer.ip}:{peer.port}")
                success = await async_download_piece(peer, next_piece, expected_length)
                if success:
                    print(f"✅ Successfully downloaded piece {next_piece}")
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



    
    async def start_downloads(self):
        """
        Launch initialization and download tasks for each connected peer concurrently.
        """
        tasks = [asyncio.create_task(self.initialize_and_download_peer(peer)) for peer in self.peers]
        await asyncio.gather(*tasks, return_exceptions=True)

async def async_main(torrent_path: str):
    # Load torrent metadata and display info
    tor = Torrent()
    tor.load_file(torrent_path)
    tor.display_info()

    # Initialize tracker and piece manager
    tracker = TrackerHandler(tor)
    piece_manager = PieceManager(tor)

    # Run tracker request in a thread (since requests is blocking)
    await asyncio.to_thread(tracker.send_request)
    print(f"Tracker response: {tracker.response}\n")

    # Use AsyncPeerManager to add peers and start downloads as soon as each peer is ready.
    peer_manager = AsyncPeerManager(tracker, piece_manager, MY_IP)
    await peer_manager.add_peers()
    if not peer_manager.peers:
        print("Exiting because no healthy peers are available.")
        return
    await peer_manager.start_downloads()

    print(f"Attempted to download all {tor.total_pieces} pieces")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <torrent_file>")
        sys.exit(1)
    asyncio.run(async_main(sys.argv[1]))
