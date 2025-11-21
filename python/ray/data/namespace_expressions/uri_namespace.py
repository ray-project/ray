"""
URI namespace for expression operations on URI-typed columns.

This module defines the ``_UriNamespace`` class, which is exposed as the
:pyattr:`ray.data.expressions.Expr.uri` attribute on expressions.

It currently provides a :meth:`_UriNamespace.download` method that returns a
:class:`~ray.data.expressions.DownloadExpr` by delegating to the top-level
:func:`ray.data.expressions.download` function.
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

    This namespace is accessed via
    :pyattr:`ray.data.expressions.Expr.uri`.

    Examples
    --------
    >>> from ray.data.expressions import col
    >>> expr = col("uri").uri.download()  # "uri" is a column of URIs
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
            If this namespace is not attached to a named column expression,
            for example ``col("uri")`` or ``col("uri").alias("my_uri")``.
        """
        # We only support simple, named column expressions like
        # col("uri") or col("uri").alias("my_uri").  These are the
        # expressions that expose a ``name`` attribute.
        name = getattr(self._expr, "name", None)
        if name is None:
            raise ValueError(
                "uri.download() can only be used on named column expressions, "
                "for example: col('uri').uri.download() or "
                "col('uri').alias('my_uri').uri.download()."
            )

        # Delegate to the existing top-level helper, which builds a DownloadExpr.
        return _download(name)
