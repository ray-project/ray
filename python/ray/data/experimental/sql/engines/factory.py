"""DataFusion optimizer engine factory."""

from ray.data import DataContext
from ray.data.experimental.sql.engines.base import OptimizerBackend
from ray.data.experimental.sql.engines.datafusion import DataFusionBackend

def get_optimizer_engine() -> OptimizerBackend:
    """Get DataFusion optimizer engine if available and enabled."""
    try:
        ctx = DataContext.get_current()
        if ctx.sql_use_datafusion:
            engine = DataFusionBackend()
            if engine.is_available():
                return engine
    except Exception:
        pass
    return None

def get_available_engines() -> list:
    """Get list of available optimizer engines."""
    available = []
    datafusion = DataFusionBackend()
    if datafusion.is_available():
        available.append(datafusion.get_engine_name())
    return available
