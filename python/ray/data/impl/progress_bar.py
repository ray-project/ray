from typing import List, Any

import ray
from ray.ray_constants import env_integer
from ray.types import ObjectRef
from ray.util.annotations import PublicAPI

try:
    import tqdm

    needs_warning = False
except ImportError:
    tqdm = None
    needs_warning = True

# Whether progress bars are enabled in this process.
_enabled: bool = not bool(env_integer("RAY_DATA_DISABLE_PROGRESS_BARS", 0))


@PublicAPI
def set_progress_bars(enabled: bool) -> bool:
    """Set whether progress bars are enabled.

    The default behavior is controlled by the
    ``RAY_DATA_DISABLE_PROGRESS_BARS`` environment variable. By default,
    it is set to "0". Setting it to "1" will disable progress bars, unless
    they are reenabled by this method.

    Returns:
        Whether progress bars were previously enabled.
    """
    global _enabled
    old_value = _enabled
    _enabled = enabled
    return old_value


class ProgressBar:
    """Thin wrapper around tqdm to handle soft imports."""

    def __init__(self, name: str, total: int, position: int = 0):
        if not _enabled:
            self._bar = None
        elif tqdm:
            self._bar = tqdm.tqdm(total=total, position=position)
            self._bar.set_description(name)
        else:
            global needs_warning
            if needs_warning:
                print(
                    "[dataset]: Run `pip install tqdm` to enable " "progress reporting."
                )
                needs_warning = False
            self._bar = None

    def block_until_complete(self, remaining: List[ObjectRef]) -> None:
        while remaining:
            done, remaining = ray.wait(remaining, fetch_local=False)
            self.update(len(done))

    def fetch_until_complete(self, refs: List[ObjectRef]) -> List[Any]:
        ref_to_result = {}
        remaining = refs
        while remaining:
            done, remaining = ray.wait(remaining, fetch_local=True)
            for ref, result in zip(done, ray.get(done)):
                ref_to_result[ref] = result
            self.update(len(done))
        return [ref_to_result[ref] for ref in refs]

    def set_description(self, name: str) -> None:
        if self._bar:
            self._bar.set_description(name)

    def update(self, i: int) -> None:
        if self._bar:
            self._bar.update(i)

    def close(self):
        if self._bar:
            self._bar.close()
            self._bar = None

    def __del__(self):
        self.close()
