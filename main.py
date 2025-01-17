
from parser import Torrent
from tracker import TrackerHandler
from peer import Peer
import socket
import bitstring
import message

from requests import get

ip = get('https://api.ipify.org').content.decode('utf8')
print(f"My IP: {ip}")

MY_IP = ip

def main():
    """
    Steps of torrent:

    1. Parse the torrent file
    2. Send a request to the tracker
    3. Connect to the peers 
    4. Send and recieve a bitfield message(Indicating what pieces we have)
    5. Send an unchoke message to the peer
    6. Send a request message to the peer
    7. Recieve the piece from the peer
    8. Repeat steps 6 and 7 until all pieces are downloaded
    """
    # Create an instance of the Torrent class
    tor = Torrent()

    # Load and process the .torrent file
    tor.load_file("torrents/bee.torrent")
    tor.display_info()
    print()

    # Bitfield to store the pieces we have
    bitfield = bitstring.BitArray(length=tor.total_pieces)

    tracker_h = TrackerHandler(tor)

    tracker_h.send_request()
    print(f'Tracker response: {tracker_h.response}')
    print()

    healthy_peers = []
    for peer in tracker_h.peers_list: 
        # first_peer = tracker_h.peers_list[0]
        if peer[0] == MY_IP:
            print("Skipping self")
            continue
        first_peer = peer
        try:
            print(f"Connecting to {first_peer[0]}:{first_peer[1]}")
            my_peer = Peer(first_peer[0], first_peer[1] , tracker_h.info_hash, tracker_h.peer_id)
            my_peer.connect()
            
            
            if my_peer.healthy:
                healthy_peers.append(my_peer)

                my_peer.send(message.Bitfield(bitfield))
                response = my_peer.recv()
                # print(f"Response: {response}")
                response = message.Message.deserialize(response)
                print(f"Deserialized: {response}")



        except socket.error as e:
            print(f"Connection to {first_peer[0]}:{first_peer[1]} failed")
            continue
    

if __name__ == "__main__":
    main()
    # clss = message.Message.__subclasses__()
    # for cls in clss:
    #     cls()