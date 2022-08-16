import pytest

from ray import serve


@pytest.mark.asyncio
async def test_execute_cached_object_ref():
    @serve.deployment
    def f():
        return 1

    @serve.deployment
    class g:
        def __init__(self, _):
            pass

        def run(self):
            return 2

    f_node = f.bind()
    g_instance = g.bind(f_node)
    dag = g_instance.run.bind()

    dag.execute()
    assert (
        await (await dag.get_object_ref_from_last_execute(f_node.get_stable_uuid()))
        == 1
    )
    assert (
        await (await dag.get_object_ref_from_last_execute(dag.get_stable_uuid())) == 2
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
