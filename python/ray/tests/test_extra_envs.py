import os
import pytest
import sys

import ray


def test_normal_remote_task(ray_start_2_cpus):
    @ray.remote(extra_envs={"key1": "value1"})
    def f1(key):
        return os.environ.get(key, "error")

    @ray.remote(extra_envs={"key2": "value2"})
    def f2(key):
        return os.environ.get(key, "error")

    assert ray.get(f1.remote("key1")) == "value1"
    assert ray.get(f1.remote("key3")) == "error"
    v = ray.get(f1.options(extra_envs={"key1": "value3"}).remote("key1"))
    assert v == "value3"
    v = ray.get(f1.options(extra_envs={}).remote("key1"))
    assert v == "error"
    assert ray.get(f2.remote("key2")) == "value2"
    assert ray.get(f2.remote("key1")) == "error"

    with pytest.raises(ValueError) as excinfo:
        f1.options(extra_envs={"CUDA_VISIBLE_DEVICES": "1"}).remote("key")
    assert str(excinfo.value) == \
        '"CUDA_VISIBLE_DEVICES" should not be set by user.'

    with pytest.raises(ValueError) as excinfo:
        f1.options(extra_envs={"key": 1.0}).remote("key")
    assert str(excinfo.value) == \
        "Extra envs key and value must be str."


def test_actor(ray_start_2_cpus):
    @ray.remote(extra_envs={"actor1": "value1"})
    class Actor1:
        def get(self, key):
            return os.environ.get(key, "error")

    @ray.remote(extra_envs={"actor2": "value2"})
    class Actor2:
        def get(self, key):
            return os.environ.get(key, "error")

    @ray.remote(extra_envs={"actor3": "value3"})
    class Actor3:
        def get(self, key):
            return os.environ.get(key, "error")

    actor1 = Actor1.remote()
    actor2 = Actor2.remote()

    assert ray.get(actor1.get.remote("actor1")) == "value1"
    assert ray.get(actor1.get.remote("actor3")) == "error"
    assert ray.get(actor1.get.remote("actor1")) == "value1"

    assert ray.get(actor2.get.remote("actor2")) == "value2"
    assert ray.get(actor2.get.remote("actor3")) == "error"
    assert ray.get(actor2.get.remote("actor2")) == "value2"

    del actor1

    actor3 = Actor3.remote()
    assert ray.get(actor3.get.remote("actor3")) == "value3"
    assert ray.get(actor3.get.remote("actor4")) == "error"
    assert ray.get(actor3.get.remote("actor3")) == "value3"

    del actor2

    with pytest.raises(ValueError) as excinfo:
        Actor1.options(extra_envs={"CUDA_VISIBLE_DEVICES": "1"}).remote()
    assert str(excinfo.value) == \
        '"CUDA_VISIBLE_DEVICES" should not be set by user.'

    with pytest.raises(ValueError) as excinfo:
        Actor1.options(extra_envs={"key": 1.0}).remote()
    assert str(excinfo.value) == \
        "Extra envs key and value must be str."


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
