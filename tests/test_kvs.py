import asyncio
import pytest

from tests.conftest import NAMESPACE, SET


@pytest.fixture(scope='module')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.mark.asyncio
async def test_put_get_delete(client):
    r = await client.put(
        namespace=NAMESPACE,
        key='test',
        bins={'hello': 'hey'}
    )
    assert r.is_ok is True

    r = await client.get(
        namespace=NAMESPACE,
        key='test',
    )
    assert r.bins == {'hello': 'hey'}

    r = await client.select(
        namespace=NAMESPACE,
        key='test',
        bin_names=['hello']
    )
    assert r.bins == {'hello': 'hey'}

    r = await client.select(
        namespace=NAMESPACE,
        key='test',
        bin_names=['hello1']
    )
    assert r.bins is None

    r = await client.delete(
        namespace=NAMESPACE,
        key='test',
    )
    assert r.is_ok is True

    r = await client.get(
        namespace=NAMESPACE,
        key='test',
    )
    assert r.bins is None


@pytest.mark.asyncio
async def test_put_get_delete_with_set(client):
    r = await client.put(
        namespace=NAMESPACE,
        key='test',
        set_name=SET,
        bins={'hello': 'hey'}
    )
    assert r.is_ok is True

    r = await client.get(
        namespace=NAMESPACE,
        key='test',
        set_name=SET,
    )
    assert r.bins == {'hello': 'hey'}

    r = await client.select(
        namespace=NAMESPACE,
        key='test',
        set_name=SET,
        bin_names=['hello']
    )
    assert r.bins == {'hello': 'hey'}

    r = await client.select(
        namespace=NAMESPACE,
        key='test',
        set_name=SET,
        bin_names=['hello1']
    )
    assert r.bins is None

    r = await client.delete(
        namespace=NAMESPACE,
        key='test',
        set_name=SET
    )
    assert r.is_ok is True

    r = await client.get(
        namespace=NAMESPACE,
        key='test',
        set_name=SET,
    )
    assert r.bins is None
