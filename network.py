import socket
def recv_by_size(sock: socket.socket):
    """Recieve data according to size"""
    # First 4 bytes => size of the message
    size = int.from_bytes(sock.recv(4), 'big')

    # Recieve the message
    data = b""
    while len(data) < size:
        data += sock.recv(size)
    
    return data