import bitstring

class Message:
    # A mapping of all message types to their respective IDs
    _message_map = None

    @classmethod
    def _build_message_map(cls):
        if not cls._message_map:
            cls._message_map = {subclass.message_id : subclass for subclass in cls.__subclasses__()}
        return cls._message_map

    @classmethod
    def deserialize(cls, data):
        """Deserialize the message from the given data"""
        message_id = data[0]
        message_map = cls._build_message_map()
        message_class = message_map.get(message_id)
        print(f"Deserializng message: {message_class}, {message_id}")
        if not message_class:
            raise ValueError(f"Invalid message ID: {message_id}")
        return message_class.deserialize_payload(data[1:])

    def deserialize_payload(self, data):
        raise NotImplementedError

    def __init__(self, data):
        pass

class Handshake:
    def __init__(self, info_hash, peer_id):
        self.pstr = b'BitTorrent protocol' # Define the protocol, 19 bytes
        self.pstrlen = len(self.pstr)
        self.reserved = b'\x00\x00\x00\x00\x00\x00\x00\x00'
        self.info_hash = info_hash
        self.peer_id = peer_id
        
        self.payload = (
            bytes([self.pstrlen]) +
            self.pstr +
            self.reserved +
            self.info_hash + 
            self.peer_id
        )
    def __str__(self):
        return self.payload
    

class KeepAlive:
    def __init__(self):
        pass

class Choke(Message):
    message_id = 0
    def __init__(self):
        pass

class Unchoke(Message):
    message_id = 1
    def __init__(self):
        super().__init__(None)
        se

class Interested(Message):
    message_id = 2
    def __init__(self):
        pass

class NotInterested(Message):
    message_id = 3
    def __init__(self):
        pass

class Have(Message):
    message_id = 4
    def __init__(self):
        pass

class Bitfield(Message):
    message_id = 5
    def __init__(self, bitfield):
        super().__init__(None)
        self.bitfield = bitfield
        self.bitfield_bytes = self.bitfield.tobytes()
        self.bitfield_length = len(self.bitfield_bytes)

        self.total_length = self.bitfield_length + 1
        self.payload = (
            bytes([self.total_length]) +
            bytes([self.message_id]) +
            self.bitfield_bytes
        )

    def serialize(self):
        return self.payload

    @classmethod
    def deserialize_payload(self, data):
        bitfield = bitstring.BitArray(data)
        return Bitfield(bitfield)

    def __str__(self):
        return f"Bitfield: {self.bitfield}"


class Request(Message):
    message_id = 6
    def __init__(self):
        pass

class Piece(Message):
    message_id = 7
    def __init__(self):
        pass

class Cancel(Message):
    message_id = 8
    def __init__(self):
        pass
