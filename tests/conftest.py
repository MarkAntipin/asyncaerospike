import asyncio

import pytest

import asyncaerospike


NAMESPACE = 'test'
SET = 'test'


@pytest.fixture(scope='module')
async def client():
    client = await asyncaerospike.connection(host='127.0.0.1', port=3000)
    yield client
    await client.close()


@pytest.fixture(scope='module')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()
