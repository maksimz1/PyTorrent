import tracker
from parser import Torrent
from tracker import TrackerHandler
import hashlib
from peer import Peer

def main():
    # Create an instance of the Torrent class
    tor = Torrent()

    # Load and process the .torrent file
    tor.load_file("bee.torrent")
    # tor.display_info()

    tracker_h = TrackerHandler(tor)

    tracker_h.send_request()

    first_peer = tracker_h.peers_list[0]
    print(f'Tracker response: {tracker_h.response}')
    my_peer = Peer(first_peer[0], first_peer[1] , tracker_h.info_hash, tracker_h.peer_id)
    print(f"Trying to communicate with {first_peer[0]}:{first_peer[1]}")
    my_peer.connect()
    

main()