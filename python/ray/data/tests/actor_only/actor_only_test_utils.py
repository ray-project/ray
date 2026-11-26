"""Fakes that several actor-only backend test files share."""

from types import SimpleNamespace
from typing import Any, Dict, Iterable


class FakeClock:
    """A clock that moves only when a test moves it. Pass it wherever the
    code takes a ``clock`` callable."""

    def __init__(self, now: float = 0.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def fake_pg(id_hex: str, bundle_specs: Iterable[Dict[str, float]] = ()) -> Any:
    """A placement-group stand-in with the two fields the actor-only backend
    reads: ``id.hex()`` and ``bundle_specs``."""
    return SimpleNamespace(
        id=SimpleNamespace(hex=lambda: id_hex), bundle_specs=list(bundle_specs)
    )
