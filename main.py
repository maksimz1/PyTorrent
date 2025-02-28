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

    async def _connect_peer(self, peer: Peer):
        await peer.connect()
        if peer.healthy:
            return peer
        else:
            print(f"Peer {peer.ip}:{peer.port} not healthy, skipping.")
            return None

    async def add_peer(self, peer_info):
        if peer_info[0] == self.my_ip:
            print("Skipping self.")
            return
        peer = Peer(
            peer_info[0],
            peer_info[1],
            self.tracker.info_hash,
            self.tracker.peer_id,
            self.piece_manager
        )
        connected_peer = await self._connect_peer(peer)
        if connected_peer:
            # Spawn a dedicated task to handle this peer immediately.
            asyncio.create_task(self.handle_peer(connected_peer))

    async def add_peers(self):
        tasks = []
        for peer_info in self.tracker.peers_list:
            tasks.append(asyncio.create_task(self.add_peer(peer_info)))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def handle_peer(self, peer: Peer):
        """
        Handle initialization and piece downloading for a single peer.
        This method can be spawned as an independent task.
        """
        try:
            # Send Interested message and negotiate extensions
            await peer.send(message.Interested())
            # await peer.send_extended_handshake()

            start_time = asyncio.get_event_loop().time()
            # Wait for the peer to unchoke and (optionally) provide a Bitfield.
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

            # Now start downloading pieces from this peer.
            failure_count = 0
            max_failures = 3
            while True:
                next_piece = self.piece_manager.choose_next_piece(peer.bitfield)
                if next_piece is None:
                    print(f"No available piece for peer {peer.ip}:{peer.port}")
                    break
                expected_length = peer.piece_manager.pieces[next_piece].piece_length
                print(f"\nStarting download of piece {next_piece} from {peer.ip}:{peer.port}")
                success = await async_download_piece(peer, next_piece, expected_length)
                if success:
                    print(f"✅ Successfully downloaded piece {next_piece} from {peer.ip}:{peer.port}")
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

async def async_main(torrent_path: str):
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

    # Optionally, keep the main loop running so new peers (e.g., from PEX) can be handled.
    while True:
        await asyncio.sleep(10)
        # Here you might check for new peers from PEX and call peer_manager.add_peer(new_peer_info)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <torrent_file>")
        sys.exit(1)
    asyncio.run(async_main(sys.argv[1]))
