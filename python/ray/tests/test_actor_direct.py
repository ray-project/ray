"""Wrapper script that sets RAY_FORCE_DIRECT."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import sys
import os

if __name__ == "__main__":
    os.environ["RAY_FORCE_DIRECT"] = "1"
    sys.exit(
        pytest.main(
            ["-v",
             os.path.join(os.path.dirname(__file__), "test_actor.py")]))
