import unittest

from ray.rllib.utils.schedules import ConstantSchedule, \
    LinearSchedule, ExponentialSchedule, PiecewiseSchedule
from ray.rllib.utils import check, try_import_tf
from ray.rllib.utils.from_config import from_config

tf = try_import_tf()


class TestSchedules(unittest.TestCase):
    """
    Tests all time-step/time-percentage dependent Schedule classes.
    """

    def test_constant_schedule(self):
        value = 2.3
        ts = [100, 0, 10, 2, 3, 4, 99, 56, 10000, 23, 234, 56]

        for fw in ["tf", "torch", None]:
            constant = from_config(ConstantSchedule,
                                   dict(value=value, framework=fw))
            for t in ts:
                out = constant(t)
                check(out, value)

    def test_linear_schedule(self):
        ts = [0, 50, 10, 100, 90, 2, 1, 99, 23]
        for fw in ["tf", "torch", None]:
            linear = from_config(
                LinearSchedule, {
                    "schedule_timesteps": 100,
                    "initial_p": 2.1,
                    "final_p": 0.6,
                    "framework": fw
                })
            if fw == "tf":
                tf.enable_eager_execution()
            for t in ts:
                out = linear(t)
                check(out, 2.1 - (t / 100) * (2.1 - 0.6), decimals=4)

    def test_polynomial_schedule(self):
        ts = [0, 5, 10, 100, 90, 2, 1, 99, 23]
        for fw in ["tf", "torch", None]:
            polynomial = from_config(
                dict(
                    type="ray.rllib.utils.schedules.polynomial_schedule."
                    "PolynomialSchedule",
                    schedule_timesteps=100,
                    initial_p=2.0,
                    final_p=0.5,
                    power=2.0,
                    framework=fw))
            if fw == "tf":
                tf.enable_eager_execution()
            for t in ts:
                out = polynomial(t)
                check(out, 0.5 + (2.0 - 0.5) * (1.0 - t / 100)**2, decimals=4)

    def test_exponential_schedule(self):
        ts = [0, 5, 10, 100, 90, 2, 1, 99, 23]
        for fw in ["tf", "torch", None]:
            exponential = from_config(
                ExponentialSchedule,
                dict(
                    initial_p=2.0,
                    decay_rate=0.99,
                    schedule_timesteps=100,
                    framework=fw))
            for t in ts:
                out = exponential(t)
                check(out, 2.0 * 0.99**(t / 100), decimals=4)

    def test_piecewise_schedule(self):
        piecewise = from_config(
            PiecewiseSchedule,
            dict(
                endpoints=[(0, 50.0), (25, 100.0), (30, 200.0)],
                outside_value=14.5))
        ts = [0, 5, 10, 100, 90, 2, 1, 99, 27]
        expected = [50.0, 60.0, 70.0, 14.5, 14.5, 54.0, 52.0, 14.5, 140.0]
        for t, e in zip(ts, expected):
            out = piecewise(t)
            check(out, e, decimals=4)
