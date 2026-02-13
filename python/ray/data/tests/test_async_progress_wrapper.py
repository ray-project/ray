import sys

import pytest

import ray


@pytest.mark.timeout(20)
def test_async_progress_updater_non_blocking():
    """Test that slow tqdm updates don't block the streaming executor.

    If tqdm.update or tqdm.close blocks indefinitely, this test will timeout after 5 seconds.
    If the async progress updater works (including cleanup), the test completes quickly.
    """
    import time
    from unittest.mock import patch

    def blocking_tqdm_operation(self, *args, **kwargs):
        """Simulate completely blocked terminal I/O"""
        time.sleep(999)  # Sleep indefinitely to simulate blocked terminal
        pass

    try:
        import tqdm

        # Mock update to block indefinitely
        with patch.object(tqdm.tqdm, "update", blocking_tqdm_operation):
            # Create and process a dataset that will trigger progress updates
            ds = ray.data.range(100, override_num_blocks=10)
            result = ds.map_batches(lambda batch: batch).take_all()

            # If we reach this point, the async progress updater worked for both
            # execution and cleanup
            assert len(result) == 100

    except ImportError:
        # Skip test if tqdm not available
        pytest.skip("tqdm not available")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
