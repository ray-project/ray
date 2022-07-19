import os
import sys
import pytest
import ray


@pytest.mark.skipif(
    sys.platform == "win32", reason="PSUtil does not work the same on windows."
)
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25001 --port 0"],
    indirect=True,
)
def test_multi_cli_basic(call_ray_start):
    ray.init("ray://localhost:25001")
    cli1 = ray.init("ray://localhost:25001", allow_multiple=True)
    cli2 = ray.init("ray://localhost:25001", allow_multiple=True)
    with cli1:
        a = ray.put(10)

    with cli2:
        b = ray.put(20)

    # TODO better error message.
    # Right now, it's EOFError actually
    with pytest.raises(Exception):
        ray.get(a)

    with pytest.raises(Exception), cli2:
        ray.get(a)

    with pytest.raises(Exception), cli1:
        ray.get(b)

    c = ray.put(30)

    with cli1:
        assert 10 == ray.get(a)

    with cli2:
        assert 20 == ray.get(b)

    with pytest.raises(Exception), cli1:
        ray.get(c)

    with pytest.raises(Exception), cli2:
        ray.get(c)


@pytest.mark.skipif(
    sys.platform == "win32", reason="PSUtil does not work the same on windows."
)
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25001 --port 0"],
    indirect=True,
)
def test_multi_cli_init(call_ray_start):
    cli1 = ray.init("ray://localhost:25001", allow_multiple=True)  # noqa
    with pytest.raises(
        ValueError,
        match="The client has already connected to the cluster "
        "with allow_multiple=True. Please set allow_multiple=True"
        " to proceed",
    ):
        ray.init("ray://localhost:25001")
    cli2 = ray.init("ray://localhost:25001", allow_multiple=True)  # noqa

    cli1.disconnect()
    cli2.disconnect()

    ray.init("ray://localhost:25001")
    cli1 = ray.init("ray://localhost:25001", allow_multiple=True)  # noqa


@pytest.mark.skipif(
    sys.platform == "win32", reason="PSUtil does not work the same on windows."
)
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25001 --port 0"],
    indirect=True,
)
def test_multi_cli_func(call_ray_start):
    @ray.remote
    def hello():
        return "world"

    cli1 = ray.init("ray://localhost:25001", allow_multiple=True)
    cli2 = ray.init("ray://localhost:25001", allow_multiple=True)

    # TODO better error message.
    # Right now, it's EOFError actually
    with pytest.raises(Exception):
        ray.get(hello.remote())

    with cli1:
        o1 = hello.remote()
        assert "world" == ray.get(o1)

    with cli2:
        o2 = hello.remote()
        assert "world" == ray.get(o2)

    with pytest.raises(Exception), cli1:
        ray.get(o2)

    with pytest.raises(Exception), cli2:
        ray.get(o1)


@pytest.mark.skipif(
    sys.platform == "win32", reason="PSUtil does not work the same on windows."
)
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25001 --port 0"],
    indirect=True,
)
def test_multi_cli_actor(call_ray_start):
    @ray.remote
    class Actor:
        def __init__(self, v):
            self.v = v

        def double(self):
            return self.v * 2

    cli1 = ray.init("ray://localhost:25001", allow_multiple=True)
    cli2 = ray.init("ray://localhost:25001", allow_multiple=True)

    # TODO better error message.
    # Right now, it's EOFError actually
    with pytest.raises(Exception):
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

    with pytest.raises(Exception), cli1:
        ray.get(a2.double.remote())

    with pytest.raises(Exception), cli1:
        ray.get(o2)

    with pytest.raises(Exception), cli2:
        ray.get(a1.double.remote())

    with pytest.raises(Exception), cli2:
        ray.get(o1)


@pytest.mark.skipif(
    sys.platform == "win32", reason="PSUtil does not work the same on windows."
)
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25001 --port 0"],
    indirect=True,
)
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

    t1 = threading.Thread(target=get, args=(0,))
    t2 = threading.Thread(target=get, args=(1,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert ret == [0, 1]


if __name__ == "__main__":
    # The following should be removed after
    # https://github.com/ray-project/ray/issues/20355
    # is fixed.
    os.environ["RAY_ENABLE_AUTO_CONNECT"] = "0"
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
