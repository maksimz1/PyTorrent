
from torrent import Torrent
from tracker import TrackerHandler
from peer import Peer
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

def main(torrent_path : str, piece_index : int) -> None:
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
    tor.load_file(torrent_path)
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
                my_peer.sock.settimeout(5)

        except socket.error as e:
            print(f"Connection to {first_peer[0]}:{first_peer[1]} failed")
            traceback.exec_print()
            continue

        except ValueError as e:
            print(f"Invalid handshake response: {e}")
            traceback.print_exc()
            continue

    print('\n'*5)

    # piece_index = bitfield.find('0b0')[0]
    MAX_RETRIES = 3 # Maximum number of retries for unexpected responses

    for peer in healthy_peers:
        try:
            # Send bitfield, Get bitfield
            print(f"Talking to {peer.ip}:{peer.port}")
            peer.send(message.Bitfield(bitfield))
            print(f"Sent bitfield")
            bitfield_response = peer.recv()
            # print(f"Response: {response}")
            bitfield_response : message.Bitfield = message.Message.deserialize(bitfield_response)
            if isinstance(bitfield_response, message.Bitfield):
                print("Got bitfield")
                peer.bitfield = bitfield_response.bitfield
            else:
                print("Failed to get bitfield")
                continue

            
            # Send unchoke, Get Unchoke/Choke
            peer.send(message.Unchoke())
            peer.state['am_choking'] = False
            print(f"Sent unchoke")

            while True:

                response = peer.recv()
                #print(f"Response: {unchoke_response}")
                response_message = message.Message.deserialize(response)
                
                if isinstance(response_message, message.Have):
                    print("Got unsupported Have message, Ignoring.")

                elif isinstance(response_message, message.Unchoke):
                    print("Peer unchoked me. Ready to request pieces.")
                    retries = MAX_RETRIES
                    break

                elif isinstance(response_message, message.Piece):
                    print(f"Unexpected Piece message during initialization. Skipping.")
                    continue

                else:
                    print(f"Unexpected message type: {type(response_message)}. Ignoring.")
                    continue

            # Interested 
            peer.send(message.Interested())
            peer.state['am_interested'] = True
            print(f"Sent interested")
            # response = peer.recv()
            #print(f"Response: {response}")

            # Actual piece downloading
            piece_length = tor.piece_length 
            cur_piece_length = 0
            print(f"Piece length: {piece_length}")
            block_size = 16 * 1024  # Request blocks of 16 KB
            piece_path = f"file_pieces/{piece_index}.part"
            if not os.path.exists(piece_path):
                with open(piece_path, "wb") as f:
                    f.write(b"\x00" * piece_length)
            for offset in range(0, piece_length, block_size):
                
                length = min(block_size, piece_length - offset)
                retries = 0
                while retries < MAX_RETRIES:
                    peer.request_piece(piece_index, offset, length)
                    
                    response = peer.recv()
                    if not response:
                        print(f"No response for block at offset {offset}.")
                        retries += 1
                        continue

                    piece = message.Message.deserialize(response)

                    if isinstance(piece, message.Piece):
                        print(f"Received block at offset {offset}")
                        
                        peer.handle_piece(piece)
                        cur_piece_length += len(piece.block)
                        break

                    elif isinstance(piece, message.Choke):
                        print(f"Peer {peer.ip}:{peer.port} choked us.")
                        peer.state['peer_choking'] = True
                        break
                    else:
                        print(f"Got response of type {type(piece)}, can't handle, Retrying...")
                        retries += 1
                
                if retries == MAX_RETRIES:
                    print(f"Failed to download block at offset {offset}")
                    if peer in healthy_peers:
                        healthy_peers.remove(peer)
            if cur_piece_length >= piece_length:
                break
            
            
            # peer.request_piece(0, 0, piece_length)
            # print(f"Requested piece 0")

            # response = peer.recv()
            # print(f"Piece response: {response}")
            # piece = message.Message.deserialize(response)
            # if isinstance(piece, message.Piece):
            #     print(f"Recieved piece 0")
            #     bitfield[0] = 1
            #     print(bitfield)

        except socket.error as e:
            print(f"Connection to {peer.ip}:{peer.port} failed")
            traceback.print_exc()
            continue

if __name__ == "__main__":
    # main(sys.argv[1], 3)

    # for cls in clss:
    #     cls()

    tor = Torrent()
    import hashlib
    # Load and process the .torrent file
    tor.load_file("torrents/starwars.torrent")
    tor.display_info()
    piece_index = 0 
    original = tor.pieces[piece_index * 20:(piece_index + 1) * 20]
    with open(f"file_pieces/{piece_index}.part", 'rb') as f:
        full_data = f.read()

    actual_hash = hashlib.sha1(full_data).digest()
    
    print(original)
    print(actual_hash)



    # ישתבך שמו לעד, אחרי שעות רבות זה עובד