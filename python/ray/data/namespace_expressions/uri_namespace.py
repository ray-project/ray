# python/ray/data/namespace_expressions/uri_namespace.py
"""
URI namespace for expression operations on URI-typed columns.

Currently provides a ``download()`` method that returns a ``DownloadExpr``
by delegating to the existing top-level :func:`ray.data.expressions.download`
function.

An ``upload()`` method is stubbed for future support (see issue #58005).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ray.data.expressions import download as _download

if TYPE_CHECKING:
    from ray.data.expressions import Expr


@dataclass
class _UriNamespace:
    """Namespace for URI operations on expression columns.

    This namespace is accessed via :meth:`Expr.uri`. For example:

    .. code-block:: python

        from ray.data.expressions import col

        # \"uri\" is a column containing URIs (e.g., s3://..., gs://..., etc.)
        expr = col(\"uri\").uri.download()

    """
    _expr: "Expr"

    def download(self) -> "Expr":
        """Create a :class:`DownloadExpr` from this URI column.

        Returns
        -------
        Expr
            A ``DownloadExpr`` that will download bytes for each URI in the
            underlying column.

        Raises
        ------
        ValueError
            If this namespace is not attached to a simple column expression.
        """
        # We only support simple column references like col("uri").
        name = getattr(self._expr, "name", None)
        if name is None:
            raise ValueError(
                "uri.download() can only be used on simple column expressions, "
                "for example: col('uri').uri.download()."
            )
        return _download(name)

    def upload(self, *args, **kwargs) -> "Expr":
        """Placeholder for a future upload expression.

        Notes
        -----
        The upload expression is tracked in issue #58005 and is not yet
        implemented.
        """
        raise NotImplementedError(
            "upload() is not yet implemented; see issue #58005 for details."
        )
