import sys
import pytest
import ray


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="PSUtil does not work the same on windows.")
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25001 --port 0"],
    indirect=True)
def test_multi_cli_basic(call_ray_start):
    ray.init("ray://localhost:25001")
    cli1 = ray.init("ray://localhost:25001", allow_multiple=True)
    cli2 = ray.init("ray://localhost:25001", allow_multiple=True)
    with cli1:
        a = ray.put(10)

    with cli2:
        b = ray.put(20)

    with pytest.raises(
            ValueError, match="ClientObjectRef is not from this Ray client"):
        ray.get(a)

    with pytest.raises(
            ValueError,
            match="ClientObjectRef is not from this Ray client"), cli2:
        ray.get(a)

    with pytest.raises(
            ValueError,
            match="ClientObjectRef is not from this Ray client"), cli1:
        ray.get(b)

    c = ray.put(30)

    with cli1:
        assert 10 == ray.get(a)

    with cli2:
        assert 20 == ray.get(b)

    with pytest.raises(
            ValueError,
            match="ClientObjectRef is not from this Ray client"), cli1:
        ray.get(c)

    with pytest.raises(
            ValueError,
            match="ClientObjectRef is not from this Ray client"), cli2:
        ray.get(c)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="PSUtil does not work the same on windows.")
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25001 --port 0"],
    indirect=True)
def test_multi_cli_init(call_ray_start):
    cli1 = ray.init("ray://localhost:25001", allow_multiple=True)  # noqa
    with pytest.raises(
            ValueError,
            match="The client has already connected to the cluster "
            "with allow_multiple=True. Please set allow_multiple=True"
            " to proceed"):
        ray.init("ray://localhost:25001")
    cli2 = ray.init("ray://localhost:25001", allow_multiple=True)  # noqa

    cli1.disconnect()
    cli2.disconnect()

    ray.init("ray://localhost:25001")
    cli1 = ray.init("ray://localhost:25001", allow_multiple=True)  # noqa


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="PSUtil does not work the same on windows.")
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25001 --port 0"],
    indirect=True)
def test_multi_cli_func(call_ray_start):
    @ray.remote
    def hello():
        return "world"

    cli1 = ray.init("ray://localhost:25001", allow_multiple=True)
    cli2 = ray.init("ray://localhost:25001", allow_multiple=True)

    with pytest.raises(Exception, match="Ray Client is not connected"):
        ray.get(hello.remote())

    with cli1:
        o1 = hello.remote()
        assert "world" == ray.get(o1)

    with cli2:
        o2 = hello.remote()
        assert "world" == ray.get(o2)

    with pytest.raises(
            ValueError,
            match="ClientObjectRef is not from this Ray client"), cli1:
        ray.get(o2)

    with pytest.raises(
            ValueError,
            match="ClientObjectRef is not from this Ray client"), cli2:
        ray.get(o1)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="PSUtil does not work the same on windows.")
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25001 --port 0"],
    indirect=True)
def test_multi_cli_actor(call_ray_start):
    @ray.remote
    class Actor:
        def __init__(self, v):
            self.v = v

        def double(self):
            return self.v * 2

    cli1 = ray.init("ray://localhost:25001", allow_multiple=True)
    cli2 = ray.init("ray://localhost:25001", allow_multiple=True)

    with pytest.raises(Exception, match="Ray Client is not connected"):
        a = Actor.remote(10)
        ray.get(a.double.remote())

    with cli1:
        a1 = Actor.remote(10)
        o1 = a1.double.remote()
        assert 20 == ray.get(o1)

    with cli2:
        a2 = Actor.remote(20)
        o2 = a2.double.remote()
        assert 40 == ray.get(o2)

    with pytest.raises(
            ValueError,
            match="ClientActorHandle is not from this Ray client"), cli1:
        a2.double.remote()

    with pytest.raises(
            ValueError,
            match="ClientObjectRef is not from this Ray client"), cli1:
        ray.get(o2)

    with pytest.raises(
            ValueError,
            match="ClientActorHandle is not from this Ray client"), cli2:
        a1.double.remote()

    with pytest.raises(
            ValueError,
            match="ClientActorHandle is not from this Ray client"), cli2:
        ray.kill(a1)

    with pytest.raises(
            ValueError,
            match="ClientObjectRef is not from this Ray client"), cli2:
        ray.get(o1)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="PSUtil does not work the same on windows.")
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25001 --port 0"],
    indirect=True)
def test_multi_cli_threading(call_ray_start):
    import threading
    b = threading.Barrier(2)
    ret = [None, None]

    def get(idx):
        cli = ray.init("ray://localhost:25001", allow_multiple=True)
        with cli:
            a = ray.put(idx)
            b.wait()
            v = ray.get(a)
            assert idx == v
            b.wait()
            ret[idx] = v

    t1 = threading.Thread(target=get, args=(0, ))
    t2 = threading.Thread(target=get, args=(1, ))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert ret == [0, 1]


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="PSUtil does not work the same on windows.")
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25001 --port 0"],
    indirect=True)
def test_multi_cli_count_and_get(call_ray_start):
    cli1 = ray.init("ray://localhost:25001", allow_multiple=True)
    id1 = cli1.id
    assert ray.util.client.num_connected_contexts() == 1

    cli2 = ray.init("ray://localhost:25001", allow_multiple=True)
    id2 = cli2.id
    assert ray.util.client.num_connected_contexts() == 2
    assert id1 != id2

    with ray.init("ray://localhost:25001", allow_multiple=True) as cli3:
        id3 = cli3.id
        assert ray.util.client.num_connected_contexts() == 3
        assert id1 != id3

    @ray.remote
    class Actor:
        def __init__(self, v):
            self.v = v

        def double(self):
            return self.v * 2

    with ray.util.client.context_from_client_id(id1):
        a1 = Actor.remote(3)

    with ray.util.client.context_from_client_id(id2):
        a2 = Actor.remote(11)

    with ray.util.client.context_from_client_id(id1):
        with pytest.raises(
                ValueError,
                match="ClientActorHandle is not from this Ray client"):
            a2.double.remote()

        v1 = a1.double.remote()

    with ray.util.client.context_from_client_id(id2):
        with pytest.raises(
                ValueError,
                match="ClientActorHandle is not from this Ray client"):
            a1.double.remote()

        v2 = a2.double.remote()

    with ray.util.client.context_from_client_id(id1):
        with pytest.raises(
                ValueError,
                match="ClientObjectRef is not from this Ray client"):
            ray.get(v2)

        assert ray.get(v1) == 6

    with ray.util.client.context_from_client_id(id2):
        with pytest.raises(
                ValueError,
                match="ClientObjectRef is not from this Ray client"):
            ray.get(v1)

        assert ray.get(v2) == 22

    assert ray.util.client.num_connected_contexts() == 3

    with ray.util.client.context_from_client_id(id2):
        ray.shutdown()
    assert ray.util.client.num_connected_contexts() == 2

    with ray.util.client.context_from_client_id(id1):
        ray.shutdown()
    assert ray.util.client.num_connected_contexts() == 1

    with pytest.raises(ValueError, match="It may have been shut down"):
        with ray.util.client.context_from_client_id(id1):
            pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
