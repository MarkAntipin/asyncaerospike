from abc import ABC, abstractmethod
import hashlib
import struct
from struct import Struct
from enum import IntEnum
from typing import Any

import msgpack


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
    ENCODER: Struct = None

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
    def unpack(cls, data: bytes):  # noqa
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


class AerospikeInteger(AerospikeDataType):
    TYPE = AerospikeType.INTEGER
    ENCODER: Struct = Struct('!Q')

    def pack_data(self) -> bytes:
        return self.ENCODER.pack(self.data)

    @classmethod
    def unpack(cls, data: bytes):
        return cls(cls.ENCODER.unpack(data)[0])

    def __len__(self):
        return self.ENCODER.size


class AerospikeDouble(AerospikeDataType):
    TYPE = AerospikeType.DOUBLE
    ENCODER: Struct = Struct('!d')

    def pack_data(self) -> bytes:
        return self.ENCODER.pack(self.data)

    @classmethod
    def unpack(cls, data: bytes):
        return cls(cls.ENCODER.unpack(data)[0])

    def __len__(self):
        return self.ENCODER.size




    # @classmethod
    # def parse(cls, data: bytes) -> "AerospikeList":
        raw_values = msgpack.unpackb(data)
        parsed_values = []
        for value in raw_values:
            parsed_values.append(unpack_aerospike(value))
        return cls(parsed_values, len(data))


class AerospikeList(AerospikeDataType):
    TYPE = AerospikeType.LIST

    def __init__(self, data: Any = None, set_name: str = None):
        super().__init__(data=data, set_name=set_name)
        self.size = None

    def pack_data(self) -> bytes:
        data_list = []
        for d in self.data:
            data_list.append(pack_native(d))

        data = msgpack.packb(data_list)
        self.size = len(data)
        return data

    @classmethod
    def unpack(cls, data: bytes):
        data_list = msgpack.unpackb(data)
        parsed_values = []
        for d in data_list:
            parsed_values.append(unpack_aerospike(value))
        return cls(parsed_values, len(data))

    def __len__(self):
        if self.size:
            return self.size
        return len(self.pack_data())


def pack_aerospike(data: Any) -> bytes:
    data = PYTHON_TYPE_TO_AEROSPIKE_TYPE[type(data)](data=data)
    return struct.pack("!B", data.TYPE) + data.pack_data()


def unpack_aerospike(data: bytes) -> AerospikeDataType:
    atype = AEROSPIKE_TYPE_CODE_TO_AEROSPIKE_TYPE[data[0]]
    parsed = parse_raw(atype, data[1:])
    return parsed.value


PYTHON_TYPE_TO_AEROSPIKE_TYPE = {
    str: AerospikeString,
    type(None): AerospikeUndef,
    int: AerospikeInteger,
    float: AerospikeDouble,
    list: AerospikeList,
}

AEROSPIKE_TYPE_CODE_TO_AEROSPIKE_TYPE = {
    AerospikeType.STRING: AerospikeString,
    AerospikeType.UNDEF: AerospikeUndef,
    AerospikeType.INTEGER: AerospikeInteger,
    AerospikeType.DOUBLE: AerospikeDouble,
    AerospikeType.LIST: AerospikeList,
}
