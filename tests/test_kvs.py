import pytest

from tests.conftest import NAMESPASE, SET


@pytest.mark.asyncio
async def test_put_get_delete(client):
    r = await client.put(
        namespace=NAMESPASE,
        set_name=SET,
        key='test',
        bins={'hello': 'hey'}
    )

    # r = await client.get(namespace, set_name, key)
    # assert result["bin_to_delete"] == "test_bin_value_to_delete"
    #
    # await client.delete_key(namespace, set_name, key)
    #
    # result = await client.get_key(namespace, set_name, key)
    # assert len(result) == 0
