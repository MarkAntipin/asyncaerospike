import pytest

import asyncaerospike


@pytest.fixture(scope='module')
async def client():
    client = asyncaerospike.connection(host= '127.0.0.1', port=3000)
    return client
