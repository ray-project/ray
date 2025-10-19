"""
Engine factory for selecting SQL optimizer backends.

Provides automatic engine selection based on configuration and availability.
"""

from ray.data import DataContext
from ray.data.experimental.sql.engines.base import OptimizerBackend
from ray.data.experimental.sql.engines.datafusion import DataFusionBackend
from ray.data.experimental.sql.engines.sqlglot import SQLGlotBackend


def get_optimizer_engine() -> OptimizerBackend:
    """
    Get the appropriate optimizer engine based on configuration.

    Selection logic:
    1. If ctx.sql_use_datafusion=True AND DataFusion available → use DataFusion
    2. Otherwise → use SQLGlot (always available)

    This makes the core SQL engine completely agnostic to which optimizer
    is being used. Engines can be swapped by changing configuration.

    Returns:
        OptimizerBackend instance (DataFusion or SQLGlot).
    """
    try:
        ctx = DataContext.get_current()

        # Try DataFusion if enabled
        if ctx.sql_use_datafusion:
            datafusion_engine = DataFusionBackend()
            if datafusion_engine.is_available():
                return datafusion_engine

        # Fallback to SQLGlot (always available)
        return SQLGlotBackend()

    except Exception:
        # Ultimate fallback
        return SQLGlotBackend()


def get_available_engines() -> list:
    """
    Get list of available optimizer engines.

    Returns:
        List of engine names that are currently available.
    """
    available = []

    # Check SQLGlot (always available)
    sqlglot = SQLGlotBackend()
    if sqlglot.is_available():
        available.append(sqlglot.get_engine_name())

    # Check DataFusion
    datafusion = DataFusionBackend()
    if datafusion.is_available():
        available.append(datafusion.get_engine_name())

    return available
