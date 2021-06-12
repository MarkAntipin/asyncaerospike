from enum import IntEnum
from typing import List, Union
from dataclasses import dataclass

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


def _create_request(
        namespace: str,
        key: str,
        info1: int,
        info2: int,
        info3: int,
        set_name: str = None,
        bins: [dict, list] = None,
        operation_bins: List[Bin] = None
):
    namespace = Namespace(data=namespace)
    key = Key(data=key, set_name=set_name)

    set = None
    if set_name:
        set = Set(data=set_name)

    fields = [f for f in [namespace, set, key] if f]

    if isinstance(bins, dict):
        bins = [Bin(data=v, operation_type=OperationTypes.WRITE, key=k) for k, v in bins.items()]
    elif isinstance(bins, list):
        bins = [Bin(operation_type=OperationTypes.READ, key=b) for b in bins]
    else:
        bins = []

    if operation_bins:
        bins = operation_bins

    base = Base(
        info1=info1,
        info2=info2,
        info3=info3,
        fields_num=len(fields),
        bins_num=len(bins)
    )
    return Request(
        namespace=namespace,
        key=key,
        fields=fields,
        bins=bins,
        base=base,
        set=set,
    )


@dataclass
class Request:
    namespace: Namespace
    key: Key
    fields: List[Union[Namespace, Key, Key]]
    bins: List[Union[None, Bin]]
    base: Base
    set: [Set, None] = None

    def pack(self):
        packed_base = self.base.pack()
        fields_packed = b''.join([f.pack() for f in self.fields])
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
    return _create_request(
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
    return _create_request(
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
    return _create_request(
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
    return _create_request(
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

    return _create_request(
        namespace=namespace,
        key=key,
        set_name=set_name,
        info1=info1,
        info2=info2,
        info3=Info3Flags.EMPTY,
        operation_bins=operation_bins
    )
