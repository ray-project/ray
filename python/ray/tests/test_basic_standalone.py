# coding: utf-8
import logging
import sys

import pytest


logger = logging.getLogger(__name__)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
