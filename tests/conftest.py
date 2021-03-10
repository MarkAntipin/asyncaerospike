import pytest

import asyncaerospike


NAMESPASE = 'test'
SET = 'test'


@pytest.fixture(scope='module')
async def client():
    client = await asyncaerospike.connection(host='127.0.0.1', port=3000)
    return client
