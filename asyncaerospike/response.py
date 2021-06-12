from asyncaerospike.bin import Bin
from asyncaerospike.base import Base
from asyncaerospike.errors import AerospikeError, STATUS_TO_ERROR


class Response:
    def __init__(
            self,
            status_code: int,
            generation: int,
            bins_num: int,
            resp_data: bytes
    ):
        self.status_code = status_code
        self.generation = generation
        self.bins_num = bins_num
        self.resp_data = resp_data

    @classmethod
    def from_bytes(cls, message_data: bytes):
        base = Base.unpack(message_data)
        resp_data = message_data[Base.ENCODER.size:]

        return cls(
            status_code=base.status_code,
            generation=base.generation,
            bins_num=base.bins_num,
            resp_data=resp_data
        )

    @property
    def bins(self) -> [dict, None]:
        """Get bins from response"""
        if self.bins_num == 0:
            return None
        resp_data = self.resp_data
        bins = {}
        for _ in range(self.bins_num):
            b = Bin.unpack(resp_data)
            bins[b.key] = b.data.data
            resp_data = resp_data[len(b):]
        return bins

    def raise_for_status(self):
        """Raises 'AerospikeError', if one occurred."""

        if self.status_code != 0:
            raise AerospikeError(
                status_code=self.status_code,
                message=STATUS_TO_ERROR[self.status_code]
            )

    @property
    def is_ok(self):
        """Check if status is ok"""
        try:
            self.raise_for_status()
        except AerospikeError:
            return False
        return True

    def __repr__(self):
        return f'<Aerospike Response [{STATUS_TO_ERROR[self.status_code]}]>'
