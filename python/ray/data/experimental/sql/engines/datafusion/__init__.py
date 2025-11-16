"""
Apache DataFusion optimizer engine for Ray Data SQL.

This engine uses DataFusion for advanced cost-based query optimization.

Features:
- Cost-based optimizer (CBO) with table statistics
- Advanced join reordering based on cardinality
- Predicate and projection pushdown
- Expression optimization
- Native Arrow integration
"""

from ray.data.experimental.sql.engines.datafusion.datafusion_backend import (
    DataFusionBackend,
)

__all__ = ["DataFusionBackend"]
