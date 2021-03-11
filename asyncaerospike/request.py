from enum import IntEnum
from typing import List

from asyncaerospike.header import Headers
from asyncaerospike.base import Base
from asyncaerospike.fields import (
    Namespace, Set, Key
)
from asyncaerospike.bin import (
    Bin, OperationTypes, READ_OPERATIONS, WRITE_OPERATIONS
)
from asyncaerospike.info_flags import Info1Flags, Info2Flags, Info3Flags


class RequestType(IntEnum):
    INFO = 1
    ADMIN = 2
    MESSAGE = 3
    COMPRESSED = 4


class Request:
    def __init__(
        self,
        namespace: str,
        key: str,
        info1: int,
        info2: int,
        info3: int,
        set_name: str = None,
        bins: [dict, list] = None,
        operation_bins: List[Bin] = None
    ):

        self.namespace = Namespace(data=namespace)
        self.key = Key(data=key, set_name=set_name)
        if set_name:
            self.set = Set(data=set_name)
        else:
            self.set = None

        self.fields = [f for f in [self.namespace, self.set, self.key] if f]

        if isinstance(bins, dict):
            self.bins = [Bin(data=v, operation_type=OperationTypes.WRITE, key=k) for k, v in bins.items()]
        elif isinstance(bins, list):
            self.bins = [Bin(operation_type=OperationTypes.READ, key=b) for b in bins]
        else:
            self.bins = []

        if operation_bins:
            self.bins = operation_bins

        self.base = Base(
            info1=info1,
            info2=info2,
            info3=info3,
            fields_num=len(self.fields),
            bins_num=len(self.bins)
        )

    def pack(self):
        packed_base = self.base.pack()
        fields_packed = b''.join([field.pack() for field in self.fields])
        bins_packed = b''.join([b.pack() for b in self.bins])
        message_packed = packed_base + fields_packed + bins_packed
        headers = Headers(request_type=RequestType.MESSAGE, request_length=len(message_packed)).pack()
        return headers + message_packed

    def __repr__(self):
        return '<Aerospike Request>'


def put_request(
        namespace: str,
        key: str,
        bins: dict,
        set_name: str = None,
):
    return Request(
        namespace=namespace,
        key=key,
        bins=bins,
        set_name=set_name,
        info1=Info1Flags.EMPTY,
        info2=Info2Flags.WRITE,
        info3=Info3Flags.EMPTY,
    )


def get_request(
        namespace: str,
        key: str,
        set_name: str = None,
):
    return Request(
        namespace=namespace,
        key=key,
        set_name=set_name,
        info1=Info1Flags.READ | Info1Flags.GET_ALL,
        info2=Info2Flags.EMPTY,
        info3=Info3Flags.EMPTY,
    )


def select_request(
        namespace: str,
        key: str,
        bin_names: list,
        set_name: str = None,
):
    return Request(
        namespace=namespace,
        key=key,
        set_name=set_name,
        bins=bin_names,
        info1=Info1Flags.READ,
        info2=Info2Flags.EMPTY,
        info3=Info3Flags.EMPTY,
    )


def delete_request(
        namespace: str,
        key: str,
        set_name: str = None,
):
    return Request(
        namespace=namespace,
        key=key,
        set_name=set_name,
        info1=Info1Flags.EMPTY,
        info2=Info2Flags.DELETE | Info2Flags.WRITE,
        info3=Info3Flags.EMPTY,
    )


def _get_info_flag_for_operations(operation_bins: List[Bin]):
    info1 = Info1Flags.EMPTY
    info2 = Info2Flags.EMPTY
    for op_bin in operation_bins:
        if op_bin.operation_type in READ_OPERATIONS:
            info1 = Info1Flags.READ
            continue

        if op_bin.operation_type in WRITE_OPERATIONS:
            info2 = Info2Flags.WRITE
            continue

    return info1, info2


def operate_request(
        namespace: str,
        key: str,
        operation_bins: List[Bin],
        set_name: str = None,
):
    info1, info2 = _get_info_flag_for_operations(operation_bins)

    return Request(
        namespace=namespace,
        key=key,
        set_name=set_name,
        info1=info1,
        info2=info2,
        info3=Info3Flags.EMPTY,
        operation_bins=operation_bins
    )
