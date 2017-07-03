import numpy as np

from ray.rllib.dqn.common.schedules import ConstantSchedule, PiecewiseSchedule


def test_piecewise_schedule():
    ps = PiecewiseSchedule([(-5, 100), (5, 200), (10, 50), (100, 50), (200, -50)], outside_value=500)

    assert np.isclose(ps.value(-10), 500)
    assert np.isclose(ps.value(0), 150)
    assert np.isclose(ps.value(5), 200)
    assert np.isclose(ps.value(9), 80)
    assert np.isclose(ps.value(50), 50)
    assert np.isclose(ps.value(80), 50)
    assert np.isclose(ps.value(150), 0)
    assert np.isclose(ps.value(175), -25)
    assert np.isclose(ps.value(201), 500)
    assert np.isclose(ps.value(500), 500)

    assert np.isclose(ps.value(200 - 1e-10), -50)


def test_constant_schedule():
    cs = ConstantSchedule(5)
    for i in range(-100, 100):
        assert np.isclose(cs.value(i), 5)
