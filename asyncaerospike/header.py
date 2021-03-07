from enum import IntEnum
from dataclasses import dataclass

from construct import BytesInteger, Const, Int8ub, Struct, Container


class RequestType(IntEnum):
    INFO = 1
    ADMIN = 2
    MESSAGE = 3
    COMPRESSED = 4


@dataclass
class Headers:
    """Headers for aerospike request"""

    ENCODER = Struct(
        'version' / Const(2, Int8ub),
        'request_type' / Int8ub,
        'request_length' / BytesInteger(6),
    )
    request_type: RequestType
    request_length: int

    def pack(self):
        """Packs Headers to bytes for request."""
        return self.ENCODER.build(
            Container(request_type=self.request_type, request_length=self.request_length)
        )

    @classmethod
    def unpack(cls, data: bytes):
        """Unpacks Headers from bytes from response"""
        parsed_data = cls.ENCODER.parse(data)
        return cls(
            request_type=parsed_data.request_type, request_length=parsed_data.request_length
        )
