import numpy as np
import unittest

from ray.rllib.utils.schedules import Schedule, ConstantSchedule, \
    LinearSchedule, ExponentialSchedule
from ray.rllib.utils import check


# TODO(sven): Fix these test cases for framework-agnostic Schedule-components.
class TestSchedules(unittest.TestCase):
    """
    Tests all time-step/time-percentage dependent Schedule classes.
    """
    def test_constant_schedule(self):
        value = 2.3
        constant = ConstantSchedule.from_config(value)
        # Time-percentages.
        input_ = np.array([0.5, 0.1, 1.0, 0.9, 0.02, 0.01, 0.99, 0.23])
        out = constant(input_)
        check(out, [value] * len(input_))

    def test_linear_schedule(self):
        linear = LinearSchedule.from_config({
            "initial_value": 2.1,
            "final_value": 0.6
        })
        # Time-percentages.
        input_ = np.array([0.5, 0.1, 1.0, 0.9, 0.02, 0.01, 0.99, 0.23])
        out = linear(input_)
        check(out, 2.1 - input_ * (2.1 - 0.6))

    def test_linear_schedule_with_step_function(self):
        linear = LinearSchedule.from_config({
            "initial_value": 2.0,
            "final_value": 0.5,
            "begin_time_percentage": 0.5,
            "end_time_percentage": 0.6
        })
        # Time-percentages.
        input_ = np.array([
            0.5, 0.1, 1.0, 0.9, 0.02, 0.01, 0.99, 0.23, 0.51, 0.52, 0.55, 0.59
        ])
        out = linear(input_)
        check(
            out,
            np.array([
                2.0, 2.0, 0.5, 0.5, 2.0, 2.0, 0.5, 2.0, 1.85, 1.7, 1.25, 0.65
            ]))

    def test_linear_parameter_using_global_time_step(self):
        max_time_steps = 100
        linear = Schedule.from_config(
            "linear", initial_value=2.0, final_value=0.5, max_t=max_time_steps)
        # Call without any parameters -> force component to use GLOBAL_STEP,
        # which should be 0 right now -> no decay.
        for time_step in range(30):
            out = linear()
            check(out, 2.0 - (time_step / max_time_steps) * (2.0 - 0.5))

    def test_polynomial_schedule(self):
        polynomial = Schedule.from_config(
            type="polynomial", initial_value=2.0, final_value=0.5, power=2.0)
        input_ = np.array([0.5, 0.1, 1.0, 0.9, 0.02, 0.01, 0.99, 0.23])
        out = polynomial(input_)
        check(out, (2.0 - 0.5) * (1.0 - input_)**2 + 0.5)

    def test_polynomial_schedule_using_global_time_step(self):
        max_time_steps = 10
        polynomial = Schedule.from_config(
            "polynomial",
            initial_value=3.0,
            final_value=0.5,
            max_t=max_time_steps)
        # Call without any parameters -> force component to use internal
        # `current_time_step`.
        # Go over the max time steps and expect time_percentage to be capped
        # at 1.0.
        for time_step in range(50):
            out = polynomial()
            check(out,
                  (3.0 - 0.5) * (1.0 - min(time_step / max_time_steps, 1.0))**2
                  + 0.5)

    def test_exponential_schedule(self):
        exponential = Schedule.from_config(
            type="exponential",
            initial_value=2.0,
            final_value=0.5,
            decay_rate=0.5)
        input_ = np.array([0.5, 0.1, 1.0, 0.9, 0.02, 0.01, 0.99, 0.23])
        out = exponential(input_)
        check(out, 0.5 + (2.0 - 0.5) * 0.5**input_)

    def test_exponential_schedule_using_global_time_step(self):
        max_time_steps = 10
        decay_rate = 0.1
        exponential = ExponentialSchedule.from_config(
            initial_value=3.0,
            final_value=0.5,
            max_t=max_time_steps,
            decay_rate=decay_rate)
        # Call without any parameters -> force component to use internal
        # `current_time_step`.
        # Go over the max time steps and expect time_percentage to be capped
        # at 1.0.
        for time_step in range(100):
            out = exponential()
            check(
                out, 0.5 +
                (3.0 - 0.5) * decay_rate**min(time_step / max_time_steps, 1.0))
