from unittest.mock import patch

import pytest

from ray.data._internal.average_calculator import TimeWindowAverageCalculator


@pytest.fixture
def current_time():
    class MutableInt:
        def __init__(self, value: int = 0):
            self.value = value

        def __repr__(self):
            return f"MutableInt({self.value})"

        def increment(self):
            self.value += 1

        def get_value(self) -> int:
            return self.value

    _current_time = MutableInt()

    def time():
        return _current_time.get_value()

    with patch("time.time", time):
        yield _current_time


def test_calcuate_time_window_average(current_time):
    """Test TimeWindowAverageCalculator."""
    window_s = 10
    values_to_report = [i + 1 for i in range(20)]

    calculator = TimeWindowAverageCalculator(window_s)
    assert calculator.get_average() is None

    for value in values_to_report:
        # Report values, test `get_average`.
        # and proceed the time by 1 second each time.
        calculator.report(value)
        avg = calculator.get_average()
        values_in_window = values_to_report[
            max(current_time.get_value() - 10, 0) : current_time.get_value() + 1
        ]
        expected = sum(values_in_window) / len(values_in_window)
        assert avg == expected, current_time.get_value()
        current_time.increment()

    for _ in range(10):
        # Keep proceeding the time, and test `get_average`.
        avg = calculator.get_average()
        values_in_window = values_to_report[max(current_time.get_value() - 10, 0) : 20]
        expected = sum(values_in_window) / len(values_in_window)
        assert avg == expected, current_time.get_value()
        current_time.increment()

    # Now no values in the time window, `get_average` should return None.
    assert calculator.get_average() is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
