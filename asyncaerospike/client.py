import asyncio
from functools import wraps

from asyncaerospike.request import (
    Request, put_request, get_request,
    select_request, delete_request
)
from asyncaerospike.response import Response
from asyncaerospike.header import Headers


def require_connection(func):
    @wraps(func)
    async def wrapper(self: "Client", *args, **kwargs):
        if not self.is_connected:
            raise ConnectionError()
        return await func(self, *args, **kwargs)
    return wrapper


class Client:
    """ Aerospike client. Provides all database queries.

    :param str host: Aerospike host.
    :param int port: Aerospike port.
    """
    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port

        self._reader = None
        self._writer = None
        self._is_connected = False

    async def connect(self):
        """Connect to Aerospike within self.host and self.port"""
        self._reader, self._writer = await asyncio.open_connection(
            self.host, self.port
        )
        self._is_connected = True

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def is_connected(self):
        return self._is_connected

    async def _send(self, request: Request):
        packed_request = request.pack()
        self._writer.write(packed_request)
        await self._writer.drain()

    async def _get_response(self) -> Response:
        header_data = await self._reader.readexactly(Headers.ENCODER.sizeof())
        parsed_header = Headers.ENCODER.parse(header_data)
        message_data = await self._reader.readexactly(parsed_header.request_length)

        return Response.from_bytes(message_data)

    @require_connection
    async def put(
        self,
        namespace: str,
        key: str,
        bins: dict,
        set_name: str = None,
    ):
        request = put_request(
            namespace=namespace,
            key=key,
            bins=bins,
            set_name=set_name
        )
        request.pack()
        await self._send(request)
        return await self._get_response()

    @require_connection
    async def get(
        self,
        namespace: str,
        key: str,
        set_name: str = None,
    ):
        request = get_request(
            namespace=namespace,
            key=key,
            set_name=set_name
        )
        request.pack()
        await self._send(request)
        return await self._get_response()

    async def select(
            self,
            namespace: str,
            key: str,
            bin_names: list,
            set_name: str = None,
    ):
        request = select_request(
            namespace=namespace,
            key=key,
            set_name=set_name,
            bin_names=bin_names
        )
        request.pack()
        await self._send(request)
        return await self._get_response()

    async def delete(
            self,
            namespace: str,
            key: str,
            set_name: str = None,
    ):
        request = delete_request(
            namespace=namespace,
            key=key,
            set_name=set_name
        )
        request.pack()
        await self._send(request)
        return await self._get_response()


async def connection(
    host: str,
    port: int,
) -> Client:
    client = Client(
        host=host,
        port=port
    )
    await client.connect()
    return client
