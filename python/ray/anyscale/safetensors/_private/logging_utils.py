"""Configures the default logger to be used by all anytensor code.

Example:

    from ray.anyscale.safetensors._private.logging_utils import logger

    logger.info("A highly important message!")

The log level can be set via the ANYTENSOR_LOG_LEVEL env var.
"""

import logging

from ray.anyscale.safetensors._private.env import LOG_LEVEL

logger = logging.getLogger("anytensor")
logger.setLevel(LOG_LEVEL)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
