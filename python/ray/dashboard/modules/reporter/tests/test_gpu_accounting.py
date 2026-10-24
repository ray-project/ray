import math
import sys

import pytest

from ray.dashboard.modules.reporter.gpu_accounting import WindowAccountant


class FakeClock:
    def __init__(self, now=100.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


# Each row: a sequence of (clock advance, action, argument) and the expected
# sequence of credits returned by `credit`.
CASES = {
    "first read establishes the window and credits nothing": (
        [(0, "credit", 50)],
        [None],
    ),
    "second read credits percent x span": (
        [(0, "credit", 50), (5, "credit", 40)],
        [None, 2.0],
    ),
    "failed read keeps the window open so the next read covers the gap": (
        [(0, "credit", 50), (5, "fail", None), (5, "credit", 100)],
        [None, None, 10.0],
    ),
    "invalid value credits nothing and restarts the window": (
        [(0, "credit", 50), (5, "credit", None), (5, "credit", 100)],
        [None, None, 5.0],
    ),
    "NaN is an invalid value": (
        [(0, "credit", 50), (5, "credit", math.nan), (5, "credit", 100)],
        [None, None, 5.0],
    ),
    "percent is clamped to 0-100": (
        [(0, "credit", 0), (4, "credit", 150), (4, "credit", -3)],
        [None, 4.0, 0.0],
    ),
    "a stalled poll credits the whole gap on the next read": (
        [(0, "credit", 50), (60, "credit", 10)],
        [None, 6.0],
    ),
    "a clock that did not move credits zero": (
        [(0, "credit", 50), (0, "credit", 100)],
        [None, 0.0],
    ),
}


@pytest.mark.parametrize("steps, expected", CASES.values(), ids=CASES.keys())
def test_window_accountant(steps, expected):
    clock = FakeClock()
    accountant = WindowAccountant(clock=clock)
    credits = []
    for advance, action, argument in steps:
        clock.advance(advance)
        if action == "credit":
            credits.append(accountant.credit(argument))
        elif action == "fail":
            credits.append(accountant.fail())
    assert credits == expected


def test_window_seconds():
    clock = FakeClock()
    accountant = WindowAccountant(clock=clock)
    assert accountant.window_seconds() is None
    accountant.credit(10)
    clock.advance(4)
    assert accountant.window_seconds() == 4.0
    accountant.fail()
    clock.advance(3)
    assert accountant.window_seconds() == 7.0


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
