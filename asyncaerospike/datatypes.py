from abc import ABC, abstractmethod
import hashlib
import struct
from enum import IntEnum
from typing import Any


class AerospikeType(IntEnum):
    UNDEF = 0
    INTEGER = 1
    DOUBLE = 2
    STRING = 3
    MAP = 19
    LIST = 20


class AerospikeDataType(ABC):
    """Abstract class for Aerospike data types.

    :param data: python data to transform to Aerospike data type.
    :param set_name: name of Aerospike set. Need to pack data
    """

    TYPE: AerospikeType

    def __init__(self, data: Any = None, set_name: str = None):
        self.data = data
        self.set_name = set_name

    @abstractmethod
    def pack_data(self) -> bytes:
        """Packs self.data

        :return: packed self.data
        """

    @classmethod
    @abstractmethod
    def unpack(cls, data: bytes):
        """Unpacks bytes to data

        :return self.data bytes
        """

    def encode(self) -> bytes:
        """ Packs Aerospike data type

        :return: encoded data type for Aerospike request
        """
        ripe = hashlib.new('ripemd160')
        if self.set_name:
            ripe.update(self.set_name.encode('utf-8'))
        ripe.update(struct.pack('!B', self.TYPE.value) + self.pack_data())
        return ripe.digest()


class AerospikeUndef(AerospikeDataType):
    TYPE = AerospikeType.UNDEF

    def pack_data(self) -> bytes:
        return b''

    @classmethod
    def unpack(cls, data: bytes):  # noqa
        return None

    def __len__(self):
        return 0


class AerospikeString(AerospikeDataType):
    TYPE = AerospikeType.STRING

    def pack_data(self) -> bytes:
        return self.data.encode('utf-8')

    @classmethod
    def unpack(cls, data: bytes):
        return cls(data=data.decode('utf-8'))

    def __len__(self):
        return len(self.data)


PYTHON_TYPE_TO_AEROSPIKE_TYPE = {
    str: AerospikeString,
    type(None): AerospikeUndef
}

AEROSPIKE_TYPE_CODE_TO_AEROSPIKE_TYPE = {
    AerospikeType.STRING: AerospikeString,
    AerospikeType.UNDEF: AerospikeUndef
}
