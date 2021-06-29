from typing import List

import ray
from ray.experimental.data.impl.block import ObjectRef

try:
    import tqdm
    needs_warning = False
except ImportError:
    tqdm = None
    needs_warning = True


class ProgressBar:
    """Thin wrapper around tqdm to handle soft imports."""

    def __init__(self, name: str, total: int, position: int = 0):
        if tqdm:
            self._bar = tqdm.tqdm(total=total, position=position)
            self._bar.set_description(name)
        else:
            global needs_warning
            if needs_warning:
                print("[dataset]: Run `pip install tqdm` to enable "
                      "progress reporting.")
                needs_warning = False
            self._bar = None

    def block_until_complete(self, remaining: List[ObjectRef]) -> None:
        while remaining:
            done, remaining = ray.wait(remaining, fetch_local=False)
            self.update(len(done))

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
