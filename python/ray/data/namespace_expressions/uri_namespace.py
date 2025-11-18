"""
URI namespace for expression operations on URI‑typed columns.

Currently provides a ``download()`` method that returns a
``DownloadExpr`` by delegating to the existing top‑level
`download()` function.  An ``upload()`` method is stubbed for future
support.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ray.data.expressions import download as _download

if TYPE_CHECKING:
    from ray.data.expressions import Expr

@dataclass
class _UriNamespace:
    """Namespace for URI operations on expression columns."""
    _expr: "Expr"

    def download(self) -> "Expr":
        name = getattr(self._expr, "name", None)
        if name is None:
            raise ValueError(
                "download() can only be used on simple column expressions"
            )
        return _download(name)

    def upload(self, *args, **kwargs) -> "Expr":
        raise NotImplementedError(
            "upload() is not yet implemented; see issue #58005 for details"
        )
