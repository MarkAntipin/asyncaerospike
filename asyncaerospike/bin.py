from struct import Struct
from typing import Any
from enum import IntEnum

from asyncaerospike.datatypes import (
    PYTHON_TYPE_TO_AEROSPIKE_TYPE, AEROSPIKE_TYPE_CODE_TO_AEROSPIKE_TYPE,
    AerospikeDataType
)


class OperationTypes(IntEnum):
    READ = 1
    WRITE = 2
    CDT_READ = 3
    CDT_MODIFY = 4
    INCR = 5
    MAP_READ = 6
    MAP_MODIFY = 7
    APPEND = 9
    PREPEND = 10
    TOUCH = 11
    BIT_READ = 12
    BIT_MODIFY = 13
    DELETE = 14


class Bin:
    """Implements Aerospike bin
    """
    FIELD_ENCODER = Struct('!IB')
    ENCODER = Struct('BBB')

    def __init__(
        self,
        key: str,
        operation_type: OperationTypes,
        data: Any = None,
        version: int = 0,
    ):
        self.key = key
        self.operation_type = operation_type
        self.version = version

        if isinstance(data, AerospikeDataType):
            self.data = data
        else:
            self.data = PYTHON_TYPE_TO_AEROSPIKE_TYPE[type(data)](data=data)


    def pack(self) -> bytes:
        base = self.ENCODER.pack(self.data.TYPE, self.version, len(self.key))
        packed_data = base + self.key.encode('utf-8') + self.data.pack_data()
        length = len(packed_data) + 1
        return self.FIELD_ENCODER.pack(length, self.operation_type) + packed_data

    @classmethod
    def unpack(cls, data: bytes):
        size, operation_type = cls.FIELD_ENCODER.unpack(data[:cls.FIELD_ENCODER.size])
        encoded_data = data[cls.FIELD_ENCODER.size: cls.FIELD_ENCODER.size + size - 1]

        aerospike_type_code, version, key_length = cls.ENCODER.unpack(encoded_data[:cls.ENCODER.size])
        key = encoded_data[cls.ENCODER.size: cls.ENCODER.size + key_length].decode('utf-8')
        encoded_data = encoded_data[cls.ENCODER.size + key_length:]

        bin_data = AEROSPIKE_TYPE_CODE_TO_AEROSPIKE_TYPE[aerospike_type_code].unpack(encoded_data)
        return cls(operation_type=operation_type, data=bin_data, key=key)

    def __len__(self):
        return self.ENCODER.size + + len(self.key) + len(self.data) + self.FIELD_ENCODER.size
