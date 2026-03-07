"""URI namespace for expression operations on URI-typed columns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

import pyarrow.fs

if TYPE_CHECKING:
    from ray.data.expressions import Expr, DownloadExpr


@dataclass
class _URINamespace:
    """Namespace for URI operations on expression columns.

    Example:
        >>> from ray.data.expressions import col
        >>> # Download content from URI column
        >>> expr = col("uri").uri.download()
    """

    _expr: Expr

    def download(
        self, *, filesystem: Optional[pyarrow.fs.FileSystem] = None
    ) -> "DownloadExpr":
        """Download content from URI column.

        Args:
            filesystem: PyArrow filesystem to use for reading remote files.
                If None, the filesystem is auto-detected from the path scheme.

        Returns:
            A DownloadExpr that will download content from the URI column.
        """
        from ray.data.expressions import download, ColumnExpr

        if not isinstance(self._expr, ColumnExpr):
            raise TypeError(
                "download() can only be called on a column expression, "
                f"but got {type(self._expr).__name__}"
            )

        return download(self._expr.name, filesystem=filesystem)
