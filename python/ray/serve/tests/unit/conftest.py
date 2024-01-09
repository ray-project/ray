import pytest

import ray


@pytest.fixture(autouse=True)
def disallow_ray_init(monkeypatch):
    def raise_on_init():
        raise RuntimeError("Unit tests should not depend on Ray being initialized.")

    monkeypatch.setattr(ray, "init", raise_on_init)
