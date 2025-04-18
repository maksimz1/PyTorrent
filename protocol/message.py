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
        try:
            """Deserialize the message from the given data"""
            message_id = data[0]
            message_map = cls._build_message_map()
            message_class = message_map.get(message_id)
            # print(f"Deserializng message: {message_class}, {message_id}")
            if not message_class:
                raise ValueError(f"Invalid message ID: {message_id}")
            return message_class.deserialize_payload(data[1:])
        except Exception as e:
            print(f"Error deserializing message: {e}")
    def deserialize_payload(self, data):
        raise NotImplementedError

    def __init__(self, data):
        pass

class Extended(Message):
    """Extension message as defined in BEP 10."""
    message_id = 20  # BitTorrent protocol assigns 20 to extended messages
    
    def __init__(self, ext_id: int, payload: bytes = None):
        super().__init__(None)
        self.ext_id = ext_id
        self.payload = payload or b''
        
    def serialize(self):
        """Serialize the message for transmission."""
        # Length prefix + message_id + extension_id + payload
        length = (1 + 1 + len(self.payload)).to_bytes(4, byteorder='big')
        return length + bytes([self.message_id]) + bytes([self.ext_id]) + self.payload
    
    @classmethod
    def deserialize_payload(cls, data):
        """Deserialize an extended message from raw data."""
        if not data:
            return None
        ext_id = data[0]
        payload = data[1:]
        return cls(ext_id, payload)

class Handshake:
    def __init__(self, info_hash, peer_id):
        self.pstr = b'BitTorrent protocol' # Define the protocol, 19 bytes
        self.pstrlen = len(self.pstr)
        self.reserved = b'\x00\x00\x00\x00\x00\x10\x00\x00'  # Set 5th bit to indicate extension protocol support
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
        super().__init__(None)
        self.length = (1).to_bytes(4, byteorder='big')
        self.payload = self.length + bytes([self.message_id])
    
    def serialize(self):
        return self.payload
    
    @classmethod
    def deserialize_payload(cls, data):
        return Unchoke()


class Unchoke(Message):
    message_id = 1
    def __init__(self):
        super().__init__(None)
        self.length = (1).to_bytes(4, byteorder='big')
        self.payload = self.length + bytes([self.message_id])
    
    def serialize(self):
        return self.payload
    
    @classmethod
    def deserialize_payload(cls, data):
        return Unchoke()

class Interested(Message):
    message_id = 2
    def __init__(self):
        super().__init__(None)
        self.length = (1).to_bytes(4, byteorder='big')
        self.payload = self.length + bytes([self.message_id])
    
    def serialize(self):
        return self.payload
    
    @classmethod
    def deserialize_payload(cls, data):
        return Interested()

class NotInterested(Message):
    message_id = 3
    def __init__(self):
        pass

class Have(Message):
    message_id = 4
    def __init__(self, index: int):
        super().__init__(None)
        self.index = index

        self.length = (5).to_bytes(4, byteorder='big')
        self.payload = self.length + bytes([self.message_id]) + index.to_bytes(4, byteorder='big')

    def serialize(self):
        return self.payload
    
    @classmethod
    def deserialize_payload(cls, data):
        if len(data) != 4:
            raise ValueError("Invalid payload length for Have message")
        index = int.from_bytes(data, 'big')
        return cls(index)

class Bitfield(Message):
    message_id = 5
    def __init__(self, bitfield):
        super().__init__(None)
        self.bitfield = bitfield
        self.bitfield_bytes = self.bitfield.tobytes()
        self.bitfield_length = len(self.bitfield_bytes)

        self.total_length = self.bitfield_length + 1
        
        self.payload = (
            self.total_length.to_bytes(4, 'big') +
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

    def __init__(self, index, begin, length):
        super().__init__(None)
        self.index = index
        self.begin = begin
        self.length = length

    def serialize(self):
        payload = (
            self.index.to_bytes(4, byteorder='big') +
            self.begin.to_bytes(4, byteorder='big') +
            self.length.to_bytes(4, byteorder='big')
        )
        message_length = (1 + len(payload)).to_bytes(4, byteorder='big')

        return message_length + bytes([self.message_id]) + payload

class Piece(Message):
    message_id = 7
    
    def __init__(self, index, begin ,block):
        self.index = index
        self.begin = begin
        self.block = block
    
    @classmethod
    def serialize(self):
        payload = (
            self.index.to_bytes(4, byteorder='big') +
            self.begin.to_bytes(4, byteorder='big') +
            self.block
        )
        message_length = (1 + len(payload)).to_bytes(4, byteorder='big')
        
        return message_length + bytes([self.message_id]) + payload

    @classmethod
    def deserialize_payload(cls, data):

        index = int.from_bytes(data[:4], 'big')
        begin = int.from_bytes(data[4:8], 'big')
        block = data[8:]
        return Piece(index, begin, block)

class Cancel(Message):
    message_id = 8
    def __init__(self):
        pass