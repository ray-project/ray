import time

import pytest

import ray


@pytest.fixture(autouse=True)
def disallow_ray_init(monkeypatch):
    def raise_on_init(*args, **kwargs):
        raise RuntimeError("Unit tests should not depend on Ray being initialized.")

    monkeypatch.setattr(ray, "init", raise_on_init)


@pytest.fixture(autouse=True)
def disallow_time_sleep(monkeypatch):
    def raise_on_sleep(seconds):
        raise RuntimeError(
            f"Unit tests should not use time.sleep({seconds}). "
            "Unit tests should be fast and deterministic."
        )

    monkeypatch.setattr(time, "sleep", raise_on_sleep)
