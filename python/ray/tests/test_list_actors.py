import pytest
import sys

import ray
from ray._private.test_utils import wait_for_condition


def test_list_named_actors_basic(ray_start_regular):
    @ray.remote
    class A:
        pass

    a = A.remote()
    assert not ray.util.list_named_actors()

    a = A.options(name="hi").remote()
    assert len(ray.util.list_named_actors()) == 1
    assert "hi" in ray.util.list_named_actors()

    b = A.options(name="hi2").remote()
    assert len(ray.util.list_named_actors()) == 2
    assert "hi" in ray.util.list_named_actors()
    assert "hi2" in ray.util.list_named_actors()

    def one_actor():
        actors = ray.util.list_named_actors()
        return actors == ["hi2"]

    del a
    wait_for_condition(one_actor)

    del b
    wait_for_condition(lambda: not ray.util.list_named_actors())


@pytest.mark.parametrize("ray_start_regular", [{"local_mode": True}], indirect=True)
def test_list_named_actors_basic_local_mode(ray_start_regular):
    @ray.remote
    class A:
        pass

    a = A.remote()
    assert not ray.util.list_named_actors()

    a = A.options(name="hi").remote()  # noqa: F841
    assert len(ray.util.list_named_actors()) == 1
    assert "hi" in ray.util.list_named_actors()

    b = A.options(name="hi2").remote()  # noqa: F841
    assert len(ray.util.list_named_actors()) == 2
    assert "hi" in ray.util.list_named_actors()
    assert "hi2" in ray.util.list_named_actors()


if __name__ == "__main__":
    import os

    # Test suite is timing out. Disable on windows for now.
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
