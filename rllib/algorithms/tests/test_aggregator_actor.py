import sys
import time

import pytest

import ray
from ray.rllib.algorithms.utils import _collect_episodes


def _good_ref(steps: int) -> ray.ObjectRef:
    return ray.remote(lambda: [bytearray(steps)]).remote()


def _stuck_ref() -> ray.ObjectRef:
    """Ref that never resolves and never raises.

    This models a lost owner that is not surfaced by Ray Core.
    """

    def _hang() -> list:
        time.sleep(10**9)
        return [bytearray(0)]

    return ray.remote(_hang).remote()


@pytest.mark.parametrize(
    "make_refs, expected_env_steps, expected_drops",
    [
        pytest.param(lambda: [_good_ref(5), _good_ref(5)], 10, 0, id="all-good"),
        pytest.param(
            lambda: [_good_ref(10), _stuck_ref(), _good_ref(7)],
            17,
            1,
            id="one-stuck-dropped",
        ),
        pytest.param(lambda: [_stuck_ref() for _ in range(3)], 0, 3, id="all-stuck"),
    ],
)
def test_collect_episodes_drops_unfetchable_refs(
    make_refs, expected_env_steps: int, expected_drops: int
):
    episodes, dropped = _collect_episodes(
        make_refs(), bulk_timeout_s=2.0, per_ref_timeout_s=1.0
    )

    assert sum(len(e) for e in episodes) == expected_env_steps
    assert dropped == expected_drops


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
