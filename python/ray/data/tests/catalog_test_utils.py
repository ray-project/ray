"""Shared catalog test doubles.

These live in an importable module rather than inside a test file on purpose.
``write_delta`` pickles the ``Catalog`` to its write tasks so a worker can
re-resolve credentials itself, and a class defined in a pytest module can't be
imported back by name in a Ray worker process (``ModuleNotFoundError: No module
named 'test_catalog'``). Defining the double here makes it picklable, which is
what lets the catalog-backed write paths be tested end-to-end at all.
"""

from typing import List, Union

from ray.data.catalog import Catalog, CatalogAccessMode, ReaderFormat, ResolvedSource


class FakeCatalog(Catalog):
    """Returns pre-baked ``ResolvedSource``s; records ``(table, reader, mode)``.

    Accepts either a single ``ResolvedSource`` or a list. With a list, each
    ``resolve()`` returns the next entry and the last one repeats once
    exhausted -- which is how a credential *refresh* is simulated: the first
    call vends one set of values, the second a different set.

    Note when used across processes: ``calls`` only records resolutions made in
    the process holding that instance. A worker gets an unpickled copy, so
    driver-side assertions on ``calls`` never see worker-side resolutions.
    """

    def __init__(self, resolved: Union[ResolvedSource, List[ResolvedSource]]):
        self._resolved: List[ResolvedSource] = (
            list(resolved) if isinstance(resolved, list) else [resolved]
        )
        self.calls: List[tuple] = []

    def resolve(
        self,
        table: str,
        *,
        reader: ReaderFormat,
        mode: CatalogAccessMode = CatalogAccessMode.READ,
    ) -> ResolvedSource:
        self.calls.append((table, reader, mode))
        idx = min(len(self.calls) - 1, len(self._resolved) - 1)
        return self._resolved[idx]
