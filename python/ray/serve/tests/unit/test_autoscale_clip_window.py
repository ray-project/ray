"""Unit tests for the autoscaling window clip (RAY_SERVE_AUTOSCALE_CLIP_WINDOW_S, D1).

The clip caps the aggregation ``window_start`` to the most recent ``clip_window_s``
seconds so a stale free-autoscaling ramp overshoot transient is not time-averaged
into the request total. ``clip_window_s <= 0`` is a no-op (today's behavior).
"""
import sys

import pytest

from ray.serve._private.autoscaling_state import _clip_window_start


def test_disabled_is_noop():
    assert _clip_window_start(None, 100.0, 130.0, 0.0) is None
    assert _clip_window_start(105.0, 100.0, 130.0, 0.0) == 105.0
    assert _clip_window_start(105.0, 100.0, 130.0, -1.0) == 105.0


def test_clips_start_up_to_recent_window():
    # end_ts=130, clip=5 -> floor at 125; earlier starts (incl. None->first_ts) pulled up.
    assert _clip_window_start(None, 100.0, 130.0, 5.0) == 125.0
    assert _clip_window_start(110.0, 100.0, 130.0, 5.0) == 125.0


def test_never_moves_start_earlier():
    # A start already inside the clip window is kept (max, never moved back).
    assert _clip_window_start(128.0, 100.0, 130.0, 5.0) == 128.0
    # A clip wider than the data returns None (no effective clip on an unset start).
    assert _clip_window_start(None, 100.0, 130.0, 100.0) is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
