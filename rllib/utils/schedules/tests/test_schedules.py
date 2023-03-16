import unittest

from ray.rllib.utils.schedules import (
    ConstantSchedule,
    LinearSchedule,
    ExponentialSchedule,
    PiecewiseSchedule,
)
from ray.rllib.utils import check, framework_iterator, try_import_tf, try_import_torch
from ray.rllib.utils.from_config import from_config

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class TestSchedules(unittest.TestCase):
    """Tests all time-step dependent Schedule classes."""

    def test_constant_schedule(self):
        value = 2.3
        ts = [100, 0, 10, 2, 3, 4, 99, 56, 10000, 23, 234, 56]

        config = {"value": value}

        for fw in framework_iterator(frameworks=["tf2", "tf", "torch", None]):
            constant = from_config(ConstantSchedule, config, framework=fw)
            for t in ts:
                out = constant(t)
                check(out, value)

            ts_as_tensors = self._get_framework_tensors(ts, fw)
            for t in ts_as_tensors:
                out = constant(t)
                assert fw != "tf" or isinstance(out, tf.Tensor)
                check(out, value, decimals=4)

    def test_linear_schedule(self):
        ts = [0, 50, 10, 100, 90, 2, 1, 99, 23, 1000]
        expected = [2.1 - (min(t, 100) / 100) * (2.1 - 0.6) for t in ts]
        config = {"schedule_timesteps": 100, "initial_p": 2.1, "final_p": 0.6}

        for fw in framework_iterator(frameworks=["tf2", "tf", "torch", None]):
            linear = from_config(LinearSchedule, config, framework=fw)
            for t, e in zip(ts, expected):
                out = linear(t)
                check(out, e, decimals=4)

            ts_as_tensors = self._get_framework_tensors(ts, fw)
            for t, e in zip(ts_as_tensors, expected):
                out = linear(t)
                assert fw != "tf" or isinstance(out, tf.Tensor)
                check(out, e, decimals=4)

    def test_polynomial_schedule(self):
        ts = [0, 5, 10, 100, 90, 2, 1, 99, 23, 1000]
        expected = [0.5 + (2.0 - 0.5) * (1.0 - min(t, 100) / 100) ** 2 for t in ts]
        config = dict(
            type="ray.rllib.utils.schedules.polynomial_schedule.PolynomialSchedule",
            schedule_timesteps=100,
            initial_p=2.0,
            final_p=0.5,
            power=2.0,
        )

        for fw in framework_iterator(frameworks=["tf2", "tf", "torch", None]):
            polynomial = from_config(config, framework=fw)
            for t, e in zip(ts, expected):
                out = polynomial(t)
                check(out, e, decimals=4)

            ts_as_tensors = self._get_framework_tensors(ts, fw)
            for t, e in zip(ts_as_tensors, expected):
                out = polynomial(t)
                assert fw != "tf" or isinstance(out, tf.Tensor)
                check(out, e, decimals=4)

    def test_exponential_schedule(self):
        decay_rate = 0.2
        ts = [0, 5, 10, 100, 90, 2, 1, 99, 23]
        expected = [2.0 * decay_rate ** (t / 100) for t in ts]
        config = dict(initial_p=2.0, decay_rate=decay_rate, schedule_timesteps=100)

        for fw in framework_iterator(frameworks=["tf2", "tf", "torch", None]):
            exponential = from_config(ExponentialSchedule, config, framework=fw)
            for t, e in zip(ts, expected):
                out = exponential(t)
                check(out, e, decimals=4)

            ts_as_tensors = self._get_framework_tensors(ts, fw)
            for t, e in zip(ts_as_tensors, expected):
                out = exponential(t)
                assert fw != "tf" or isinstance(out, tf.Tensor)
                check(out, e, decimals=4)

    def test_piecewise_schedule(self):
        ts = [0, 5, 10, 100, 90, 2, 1, 99, 27]
        expected = [50.0, 60.0, 70.0, 14.5, 14.5, 54.0, 52.0, 14.5, 140.0]
        config = dict(
            endpoints=[(0, 50.0), (25, 100.0), (30, 200.0)], outside_value=14.5
        )

        for fw in framework_iterator(frameworks=["tf2", "tf", "torch", None]):
            piecewise = from_config(PiecewiseSchedule, config, framework=fw)
            for t, e in zip(ts, expected):
                out = piecewise(t)
                check(out, e, decimals=4)

            ts_as_tensors = self._get_framework_tensors(ts, fw)
            for t, e in zip(ts_as_tensors, expected):
                out = piecewise(t)
                assert fw != "tf" or isinstance(out, tf.Tensor)
                check(out, e, decimals=4)

    @staticmethod
    def _get_framework_tensors(ts, fw):
        if fw == "torch":
            ts = [torch.tensor(t, dtype=torch.int32) for t in ts]
        elif fw is not None and "tf" in fw:
            ts = [tf.constant(t, dtype=tf.int32) for t in ts]
        return ts


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
