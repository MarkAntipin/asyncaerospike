from dataclasses import dataclass
import struct


@dataclass
class Base:
    """First part of Aerospike request after headers"""

    ENCODER = struct.Struct('!BBBBxBIIIHH')

    info1: int
    info2: int
    info3: int

    fields_num: int
    bins_num: int

    status_code: int = 0
    generation: int = 0
    record_ttl: int = 0
    transaction_ttl: int = 1000

    def pack(self) -> bytes:
        """Packs Base to bytes for request."""

        return self.ENCODER.pack(
            self.ENCODER.size,
            self.info1,
            self.info2,
            self.info3,
            self.status_code,
            self.generation,
            self.record_ttl,
            self.transaction_ttl,
            self.fields_num,
            self.bins_num,
        )

    @classmethod
    def unpack(cls, data: bytes):
        """Unpacks Base from bytes from response"""

        base = cls.ENCODER.unpack(data[:cls.ENCODER.size])
        return cls(
            info1=base[1],
            info2=base[2],
            info3=base[3],
            fields_num=base[8],
            bins_num=base[9],
            status_code=base[4],
            generation=base[5],
        )
