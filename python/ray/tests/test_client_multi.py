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
    cli1 = ray.init("ray://localhost:25001", allow_multiple=True)
    cli2 = ray.init("ray://localhost:25001", allow_multiple=True)
    with cli1:
        a = ray.put(10)
    with cli2:
        b = ray.put(20)

    with pytest.raises(Exception):
        ray.put(30)

    with pytest.raises(Exception):
        ray.get(a)

    with pytest.raises(Exception), cli2:
        ray.get(a)

    with pytest.raises(Exception), cli1:
        ray.get(b)

    with cli1:
        assert 10 == ray.get(a)

    with cli2:
        assert 20 == ray.get(b)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="PSUtil does not work the same on windows.")
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25001 --port 0"],
    indirect=True)
def test_multi_cli_error_1(call_ray_start):
    cli1 = ray.init("ray://localhost:25001", allow_multiple=True)  # noqa
    with pytest.raises(RuntimeError):
        cli2 = ray.init("ray://localhost:25001")  # noqa


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="PSUtil does not work the same on windows.")
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25001 --port 0"],
    indirect=True)
def test_multi_cli_error_2(call_ray_start):
    cli1 = ray.init("ray://localhost:25001")  # noqa
    with pytest.raises(RuntimeError):
        cli2 = ray.init("ray://localhost:25001", allow_multiple=True)  # noqa


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

    with pytest.raises(Exception):
        ray.get(hello.remote())

    with cli1:
        o1 = hello.remote()
        assert "world" == ray.get(o1)

    with cli2:
        o2 = hello.remote()
        assert "world" == ray.get(o2)

    # TODO better error message.
    # Right now, it's EOFError actually
    with pytest.raises(Exception), cli1:
        ray.get(o2)

    with pytest.raises(Exception), cli2:
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
