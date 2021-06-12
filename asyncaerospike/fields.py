from abc import ABC, abstractmethod
from struct import Struct
from typing import Any
from enum import IntEnum


from asyncaerospike.datatypes import PYTHON_TYPE_TO_AEROSPIKE_TYPE


class FieldTypes(IntEnum):
    NAMESPACE = 0
    SET = 1
    KEY = 2
    DIGEST = 4


class Field(ABC):
    """Abstract class for Aerospike entities (namespace, set, key).

    :param data: field name.
    """

    ENCODER: Struct
    FIELD_TYPE: FieldTypes

    def __init__(self, data: Any):
        self.data = data

    @abstractmethod
    def pack_data(self) -> bytes:
        """Packs self.data

        :return: packed self.data
        """

    def pack(self) -> bytes:
        """Packs field

        :return: encoded field
        """
        data = self.pack_data()
        length = len(data) + 1
        return self.ENCODER.pack(length, self.FIELD_TYPE) + data

    @classmethod
    def unpack(cls, data: bytes):
        """ Packs field

        :return: encoded field for Aerospike request
        """
        length, field_type = cls.ENCODER.unpack(data[:cls.ENCODER.size])
        data = data[cls.ENCODER.size:length]
        return cls(data=data)


class Namespace(Field):
    """Implements Aerospike namespace."""

    ENCODER = Struct('!IB')
    FIELD_TYPE = FieldTypes.NAMESPACE

    def pack_data(self):
        return self.data.encode('utf-8')


class Set(Namespace):
    """Implements Aerospike set."""

    FIELD_TYPE = FieldTypes.SET


class Key(Field):
    """Implements Aerospike key."""
    ENCODER = Struct('!IB')
    FIELD_TYPE = FieldTypes.DIGEST

    def __init__(self, data: Any, set_name: str = None):
        super().__init__(data=data)
        self.data = PYTHON_TYPE_TO_AEROSPIKE_TYPE[type(data)](data=data, set_name=set_name)

    def pack_data(self):
        return self.data.encode()
