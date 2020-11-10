import pytest
import ray.experimental.client.server as ray_client_server
import ray.experimental.client as ray


def test_put_get(ray_start_regular_shared):
    server = ray_client_server.serve("localhost:50051")
    ray.connect("localhost:50051")

    objectref = ray.put("hello world")
    print(objectref)

    retval = ray.get(objectref)
    assert retval == "hello world"
    ray.disconnect()
    server.stop(0)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
