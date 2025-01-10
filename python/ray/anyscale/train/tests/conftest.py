import logging

import pytest


@pytest.fixture(autouse=True)
def setup_logging():
    logger = logging.getLogger("ray.train")
    orig_level = logger.getEffectiveLevel()
    logger.setLevel(logging.INFO)
    yield
    logger.setLevel(orig_level)
