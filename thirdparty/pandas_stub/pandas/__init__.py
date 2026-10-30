"""Minimal pandas stub for offline Ray Data benchmarks."""

__all__ = ["DataFrame", "Series", "__version__"]
__version__ = "0.0.0-stub"


class _UnavailableObject:
    def __init__(self, *args, **kwargs):  # pragma: no cover - guard rail
        raise RuntimeError(
            "The lightweight pandas stub only satisfies 'import pandas'. "
            "Install the real pandas distribution to use pandas functionality."
        )


class DataFrame(_UnavailableObject):
    """Placeholder that immediately errors if instantiated."""


class Series(_UnavailableObject):
    """Placeholder that immediately errors if instantiated."""


def __getattr__(name):  # pragma: no cover - guard rail
    raise AttributeError(
        f"pandas stub does not provide attribute '{name}'. Install pandas for full support."
    )
