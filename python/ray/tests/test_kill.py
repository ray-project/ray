import ray
import time
import pytest
from ray.exceptions import RayCancellationError


def test_basic_kill(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    def micro_sleep_for(t):
        for _ in range(t * 1000):
            time.sleep(1 / 1000)
        return t

    obj1 = micro_sleep_for.remote(100)
    ray.kill(obj1)
    with pytest.raises(RayCancellationError):
        ray.get(obj1, 10)
    obj2 = micro_sleep_for.remote(100)
    obj3 = micro_sleep_for.remote(100)
    ray.kill(obj3)
    with pytest.raises(RayCancellationError):
        ray.get(obj3, 10)
    ray.kill(obj2)
    with pytest.raises(RayCancellationError):
        ray.get(obj2, 10)
    assert ray.get(micro_sleep_for.remote(0)) == 0


def test_cascading_kill(ray_start_regular):
    @ray.remote
    def wait_for(x):
        for i in range(100 * x):
            time.sleep(1 / 100)
        return x

    first_wait = wait_for.remote(100)
    second_wait = wait_for.remote(first_wait)
    ray.kill(first_wait)
    with pytest.raises(RayCancellationError):
        ray.get(second_wait, 10)


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))