import tracker
from parser import Torrent
from tracker import TrackerHandler
import hashlib
from peer import Peer
import socket

MY_IP = "77.127.104.161"

def main():
    # Create an instance of the Torrent class
    tor = Torrent()

    # Load and process the .torrent file
    tor.load_file("torrents/bee.torrent")
    # tor.display_info()

    tracker_h = TrackerHandler(tor)

    tracker_h.send_request()
    print(f'Tracker response: {tracker_h.response}')

    for peer in tracker_h.peers_list: 
        # first_peer = tracker_h.peers_list[0]
        if peer[0] == MY_IP:
            print("Skipping self")
            continue
        first_peer = peer
        try:
            my_peer = Peer(first_peer[0], first_peer[1] , tracker_h.info_hash, tracker_h.peer_id)
            print(f"Trying to connect to {first_peer[0]}:{first_peer[1]}")
            my_peer.connect()
        except socket.error as e:
            print(f"Connection to {first_peer[0]}:{first_peer[1]} failed")
            continue
    

main()