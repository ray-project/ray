import os

import pytest

import ray
from ray import serve


def test_basic_get(serve_instance):
    name = "test"

    @serve.deployment(name=name, version="1")
    def d(*args):
        return "1", os.getpid()

    with pytest.raises(KeyError):
        serve.get_deployment(name=name)

    handle = serve.run(d.bind())
    val1, pid1 = ray.get(handle.remote())
    assert val1 == "1"

    del d

    d2 = serve.get_deployment(name=name)
    val2, pid2 = ray.get(d2.get_handle().remote())
    assert val2 == "1"
    assert pid2 == pid1


def test_get_after_delete(serve_instance):
    name = "test"

    @serve.deployment(name=name, version="1")
    def d(*args):
        return "1", os.getpid()

    d.deploy()
    del d

    d2 = serve.get_deployment(name=name)
    d2.delete()
    del d2

    with pytest.raises(KeyError):
        serve.get_deployment(name=name)


def test_deploy_new_version(serve_instance):
    name = "test"

    @serve.deployment(name=name, version="1")
    def d(*args):
        return "1", os.getpid()

    d.deploy()
    val1, pid1 = ray.get(d.get_handle().remote())
    assert val1 == "1"

    del d

    d2 = serve.get_deployment(name=name)
    d2.options(version="2").deploy()
    val2, pid2 = ray.get(d2.get_handle().remote())
    assert val2 == "1"
    assert pid2 != pid1


def test_deploy_empty_version(serve_instance):
    name = "test"

    @serve.deployment(name=name)
    def d(*args):
        return "1", os.getpid()

    d.deploy()
    val1, pid1 = ray.get(d.get_handle().remote())
    assert val1 == "1"

    del d

    d2 = serve.get_deployment(name=name)
    d2.deploy()
    val2, pid2 = ray.get(d2.get_handle().remote())
    assert val2 == "1"
    assert pid2 != pid1


def test_init_args(serve_instance):
    name = "test"

    @serve.deployment(name=name)
    class D:
        def __init__(self, val):
            self._val = val

        def __call__(self, *arg):
            return self._val, os.getpid()

    D.deploy("1")
    val1, pid1 = ray.get(D.get_handle().remote())
    assert val1 == "1"

    del D

    D2 = serve.get_deployment(name=name)
    D2.deploy()
    val2, pid2 = ray.get(D2.get_handle().remote())
    assert val2 == "1"
    assert pid2 != pid1

    D2 = serve.get_deployment(name=name)
    D2.deploy("2")
    val3, pid3 = ray.get(D2.get_handle().remote())
    assert val3 == "2"
    assert pid3 != pid2


def test_init_kwargs(serve_instance):
    name = "test"

    @serve.deployment(name=name)
    class D:
        def __init__(self, *, val=None):
            assert val is not None
            self._val = val

        def __call__(self, *arg):
            return self._val, os.getpid()

    D.deploy(val="1")
    val1, pid1 = ray.get(D.get_handle().remote())
    assert val1 == "1"

    del D

    D2 = serve.get_deployment(name=name)
    D2.deploy()
    val2, pid2 = ray.get(D2.get_handle().remote())
    assert val2 == "1"
    assert pid2 != pid1

    D2 = serve.get_deployment(name=name)
    D2.deploy(val="2")
    val3, pid3 = ray.get(D2.get_handle().remote())
    assert val3 == "2"
    assert pid3 != pid2


def test_scale_replicas(serve_instance):
    name = "test"

    @serve.deployment(name=name)
    def d(*args):
        return os.getpid()

    def check_num_replicas(num):
        handle = serve.get_deployment(name=name).get_handle()
        assert len(set(ray.get([handle.remote() for _ in range(50)]))) == num

    d.deploy()
    check_num_replicas(1)
    del d

    d2 = serve.get_deployment(name=name)
    d2.options(num_replicas=2).deploy()
    check_num_replicas(2)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
