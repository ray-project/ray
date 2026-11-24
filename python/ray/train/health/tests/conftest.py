import logging

import pytest


@pytest.fixture
def propagate_logs():
    """Propagate Ray's logs to the root logger, where ``caplog`` reads them."""
    logging.getLogger("ray").propagate = True
    yield
    logging.getLogger("ray").propagate = False
