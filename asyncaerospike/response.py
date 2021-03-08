from asyncaerospike.bin import Bin
from asyncaerospike.base import Base


class Response:
    def __init__(self, status_code, generation, bins):
        self.status_code = status_code
        self.generation = generation
        self.bins = bins

    @classmethod
    def from_bytes(cls, message_data: bytes):
        base = Base.unpack(message_data)
        resp_data = message_data[Base.ENCODER.size:]

        bins = {}
        for _ in range(base.bins_num):
            b = Bin.unpack(resp_data)
            bins[b.key] = b.data.data
            resp_data = resp_data[len(b):]

        return cls(status_code=base.status_code, generation=base.generation, bins=bins)

    def __repr__(self):
        return str(self.status_code)
