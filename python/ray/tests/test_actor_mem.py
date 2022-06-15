import random
import pytest
import numpy as np
import os
from ray import cloudpickle as pickle
from ray import ray_constants
from ray.actor import ActorClassInheritanceException

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None
import sys
import tempfile
import datetime

from ray._private.test_utils import (
    client_test_enabled,
    wait_for_condition,
    wait_for_pid_to_exit,
)
from ray.tests.client_test_utils import create_remote_signal_actor
import ray

# NOTE: We have to import setproctitle after ray because we bundle setproctitle
# with ray.
import setproctitle  # noqa


@pytest.mark.parametrize("set_enable_auto_connect", ["1", "0"], indirect=True)
def test_caching_actors(shutdown_only, set_enable_auto_connect):
    # Test defining actors before ray.init() has been called.

    @ray.remote
    class Foo:
        def __init__(self):
            pass

        def get_val(self):
            return 3

    if set_enable_auto_connect == "0":
        # Check that we can't actually create actors before ray.init() has
        # been called.
        with pytest.raises(Exception):
            f = Foo.remote()

        ray.init(num_cpus=1)
    else:
        # Actor creation should succeed here because ray.init() auto connection
        # is (by default) enabled.
        f = Foo.remote()

    f = Foo.remote()

    assert ray.get(f.get_val.remote()) == 3
