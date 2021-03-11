import pytest

import asyncaerospike


NAMESPACE = 'test'
SET = 'test'


@pytest.fixture(scope='module')
async def client():
    client = await asyncaerospike.connection(host='127.0.0.1', port=3000)
    yield client
    await client.close()

