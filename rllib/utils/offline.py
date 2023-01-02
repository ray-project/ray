import logging
import os

from ray.rllib.utils.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI
def try_import_arrow(error: bool = False):
    """Tries importing Arrow and returns the module.

    Args:
        error: Whether to raise an error if pyarrow cannot be
               imported.

    Returns:
        The pyarrow module

    Raises:
        ImportError: If error=True and pyarrow is not insntalled.
    """
    if "RLLIB_TEST_NO_ARROW_IMPORT" in os.environ:
        logger.warning("Not importing Arrow for test purposes.")
        return None

    try:
        # TODO: Might need pyarrow==6.0.1
        import pyarrow as pa

        return pa
    except ImportError as e:
        if error:
            raise e
        return None
