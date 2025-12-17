import time
from unittest.mock import patch

import pytest

from ray.util.multiprocessing import Pool


def test_pool_timeout_success():
    """Test that Pool waits for resources when cluster_resources_timeout is set."""
    with patch("ray.init"), \
         patch("ray.is_initialized", return_value=True), \
         patch("ray._private.state.cluster_resources") as mock_resources, \
         patch("ray.util.multiprocessing.pool.Pool._start_actor_pool"):

        # Simulate resources becoming available after a few calls
        # 1st call: empty
        # 2nd call: empty
        # 3rd call: has CPU
        mock_resources.side_effect = [{}, {}, {"CPU": 4}]

        start = time.time()
        # Should succeed within timeout
        Pool(processes=2, cluster_resources_timeout=2.0)
        duration = time.time() - start

        # Verify it waited (called more than once)
        assert mock_resources.call_count == 3
        # Should be fast enough
        assert duration < 2.0

def test_pool_timeout_wait_for_sufficient_cpus():
    """Test that Pool waits until ENOUGH resources are available."""
    with patch("ray.init"), \
         patch("ray.is_initialized", return_value=True), \
         patch("ray._private.state.cluster_resources") as mock_resources, \
         patch("ray.util.multiprocessing.pool.Pool._start_actor_pool"):

        # 1st call: 2 CPUs (insufficient for 4 processes)
        # 2nd call: 4 CPUs (sufficient)
        mock_resources.side_effect = [{"CPU": 2}, {"CPU": 4}]

        start = time.time()
        # Should succeed within timeout
        Pool(processes=4, cluster_resources_timeout=2.0)
        duration = time.time() - start

        # Verify it waited
        assert mock_resources.call_count == 2
        assert duration < 2.0


def test_pool_timeout_failure():
    """Test that Pool raises ValueError when timeout expires."""
    with patch("ray.init"), \
         patch("ray.is_initialized", return_value=True), \
         patch("ray._private.state.cluster_resources") as mock_resources, \
         patch("ray.util.multiprocessing.pool.Pool._start_actor_pool"):

        # Always return empty resources
        mock_resources.return_value = {}

        with pytest.raises(ValueError, match="Insufficient CPU resources found"):
            Pool(processes=2, cluster_resources_timeout=0.5)

def test_pool_default_backward_compatibility():
    """Test that default behavior (timeout=0) raises error immediately if no resources."""
    with patch("ray.init"), \
         patch("ray.is_initialized", return_value=True), \
         patch("ray._private.state.cluster_resources") as mock_resources, \
         patch("ray.util.multiprocessing.pool.Pool._start_actor_pool"):

        mock_resources.return_value = {}

        # Default is timeout=0
        with pytest.raises(ValueError, match="No CPU resources found"):
            Pool(processes=2)

        # Verify it was called only once for immediate check
        assert mock_resources.call_count == 1

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
