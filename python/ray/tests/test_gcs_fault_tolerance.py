import sys

import ray


def test_gcs_server_restart():
    ray.init()

    @ray.remote
    class Increase:
        def method(self, x):
            return x + 2

    @ray.remote
    def increase(x):
        return x + 1

    actor1 = Increase.remote()
    result = ray.get(actor1.method.remote(1))
    assert result == 3

    ray.worker._global_node.kill_gcs_server()
    ray.worker._global_node.start_gcs_server()

    result = ray.get(actor1.method.remote(7))
    assert result == 9

    actor2 = Increase.remote()
    result = ray.get(actor2.method.remote(2))
    assert result == 4

    result = ray.get(increase.remote(1))
    assert result == 2
    ray.shutdown()


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
