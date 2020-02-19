from tensorflow.python.eager.context import eager_mode
import unittest

from ray.rllib.utils.schedules import ConstantSchedule, \
    LinearSchedule, ExponentialSchedule, PiecewiseSchedule
from ray.rllib.utils import check, try_import_tf
from ray.rllib.utils.from_config import from_config

tf = try_import_tf()


class TestSchedules(unittest.TestCase):
    """
    Tests all time-step dependent Schedule classes.
    """

    def test_constant_schedule(self):
        value = 2.3
        ts = [100, 0, 10, 2, 3, 4, 99, 56, 10000, 23, 234, 56]

        config = {"value": value}

        for fw in ["tf", "torch", None]:
            constant = from_config(ConstantSchedule, config, framework=fw)
            for t in ts:
                out = constant(t)
                check(out, value)

        # Test eager as well.
        with eager_mode():
            constant = from_config(ConstantSchedule, config, framework="tf")
            for t in ts:
                out = constant(t)
                check(out, value)

    def test_linear_schedule(self):
        ts = [0, 50, 10, 100, 90, 2, 1, 99, 23]
        config = {"schedule_timesteps": 100, "initial_p": 2.1, "final_p": 0.6}
        for fw in ["tf", "torch", None]:
            linear = from_config(LinearSchedule, config, framework=fw)
            for t in ts:
                out = linear(t)
                check(out, 2.1 - (t / 100) * (2.1 - 0.6), decimals=4)

        # Test eager as well.
        with eager_mode():
            linear = from_config(LinearSchedule, config, framework="tf")
            for t in ts:
                out = linear(t)
                check(out, 2.1 - (t / 100) * (2.1 - 0.6), decimals=4)

    def test_polynomial_schedule(self):
        ts = [0, 5, 10, 100, 90, 2, 1, 99, 23]
        config = dict(
            type="ray.rllib.utils.schedules.polynomial_schedule."
            "PolynomialSchedule",
            schedule_timesteps=100,
            initial_p=2.0,
            final_p=0.5,
            power=2.0)
        for fw in ["tf", "torch", None]:
            config["framework"] = fw
            polynomial = from_config(config)
            for t in ts:
                out = polynomial(t)
                check(out, 0.5 + (2.0 - 0.5) * (1.0 - t / 100)**2, decimals=4)

        # Test eager as well.
        with eager_mode():
            config["framework"] = "tf"
            polynomial = from_config(config)
            for t in ts:
                out = polynomial(t)
                check(out, 0.5 + (2.0 - 0.5) * (1.0 - t / 100)**2, decimals=4)

    def test_exponential_schedule(self):
        ts = [0, 5, 10, 100, 90, 2, 1, 99, 23]
        config = dict(initial_p=2.0, decay_rate=0.99, schedule_timesteps=100)
        for fw in ["tf", "torch", None]:
            config["framework"] = fw
            exponential = from_config(ExponentialSchedule, config)
            for t in ts:
                out = exponential(t)
                check(out, 2.0 * 0.99**(t / 100), decimals=4)

        # Test eager as well.
        with eager_mode():
            config["framework"] = "tf"
            exponential = from_config(ExponentialSchedule, config)
            for t in ts:
                out = exponential(t)
                check(out, 2.0 * 0.99**(t / 100), decimals=4)

    def test_piecewise_schedule(self):
        ts = [0, 5, 10, 100, 90, 2, 1, 99, 27]
        expected = [50.0, 60.0, 70.0, 14.5, 14.5, 54.0, 52.0, 14.5, 140.0]
        config = dict(
            endpoints=[(0, 50.0), (25, 100.0), (30, 200.0)],
            outside_value=14.5)
        for fw in ["tf", "torch", None]:
            config["framework"] = fw
            piecewise = from_config(PiecewiseSchedule, config)
            for t, e in zip(ts, expected):
                out = piecewise(t)
                check(out, e, decimals=4)

        # Test eager as well.
        with eager_mode():
            config["framework"] = "tf"
            piecewise = from_config(PiecewiseSchedule, config)
            for t, e in zip(ts, expected):
                out = piecewise(t)
                check(out, e, decimals=4)


if __name__ == "__main__":
    import unittest
    unittest.main(verbosity=1)
