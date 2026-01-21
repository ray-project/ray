import time

import pytest

import ray
from ray.data import DataContext
from ray.data.exceptions import DatasetJobTimeoutError


class TestJobTimeout:
    """Test suite for Ray Data job-level timeout functionality."""

    def test_timeout_disabled_by_default(self, ray_start_regular_shared):
        """Test that timeout is disabled by default."""
        ctx = DataContext.get_current()
        assert ctx.job_timeout_s == -1

        # Job should complete normally without timeout
        ds = ray.data.range(100)
        result = ds.map(lambda x: {"id": x["id"] * 2}).take_all()
        assert len(result) == 100

    def test_timeout_configuration(self, ray_start_regular_shared):
        """Test timeout configuration in DataContext."""
        ctx = DataContext.get_current()

        # Test setting timeout
        ctx.job_timeout_s = 30
        assert ctx.job_timeout_s == 30

        # Test setting to 0 (disabled)
        ctx.job_timeout_s = 0
        assert ctx.job_timeout_s == 0

        # Test setting to -1 (disabled)
        ctx.job_timeout_s = -1
        assert ctx.job_timeout_s == -1

    def test_timeout_raised_on_slow_job(self, ray_start_regular_shared):
        """Test that timeout is raised when job exceeds limit."""
        ctx = DataContext.get_current()
        original_timeout = ctx.job_timeout_s

        try:
            # Set a very short timeout
            ctx.job_timeout_s = 1

            # Create a job that will take longer than timeout
            ds = ray.data.range(100)

            def slow_map(batch):
                time.sleep(0.2)  # Sleep 200ms per batch
                return batch

            # This should raise DatasetJobTimeoutError
            with pytest.raises(DatasetJobTimeoutError) as exc_info:
                ds.map_batches(slow_map, batch_size=10).take_all()

            # Verify exception message contains useful information
            error_msg = str(exc_info.value)
            assert "exceeded the configured timeout" in error_msg
            assert "1s" in error_msg or "1.0s" in error_msg
            assert "Elapsed time:" in error_msg

        finally:
            # Restore original timeout
            ctx.job_timeout_s = original_timeout

    def test_timeout_not_raised_for_fast_job(self, ray_start_regular_shared):
        """Test that timeout is not raised when job completes quickly."""
        ctx = DataContext.get_current()
        original_timeout = ctx.job_timeout_s

        try:
            # Set a generous timeout
            ctx.job_timeout_s = 30

            # Create a fast job
            ds = ray.data.range(100)
            result = ds.map(lambda x: {"id": x["id"] * 2}).take_all()

            # Job should complete successfully
            assert len(result) == 100

        finally:
            ctx.job_timeout_s = original_timeout

    def test_timeout_per_job_isolation(self, ray_start_regular_shared):
        """Test that timeout configuration is isolated per job."""
        ctx = DataContext.get_current()
        original_timeout = ctx.job_timeout_s

        try:
            # First job with no timeout
            ctx.job_timeout_s = -1
            ds1 = ray.data.range(50)
            result1 = ds1.take_all()
            assert len(result1) == 50

            # Second job with timeout enabled
            ctx.job_timeout_s = 10
            ds2 = ray.data.range(50)
            result2 = ds2.take_all()
            assert len(result2) == 50

        finally:
            ctx.job_timeout_s = original_timeout

    def test_timeout_with_transformations(self, ray_start_regular_shared):
        """Test timeout with multiple transformations."""
        ctx = DataContext.get_current()
        original_timeout = ctx.job_timeout_s

        try:
            ctx.job_timeout_s = 5

            ds = ray.data.range(1000)
            result = (
                ds.map(lambda x: {"id": x["id"] * 2})
                .filter(lambda x: x["id"] % 2 == 0)
                .map(lambda x: {"id": x["id"] + 1})
                .take_all()
            )

            # Should complete within timeout
            assert len(result) == 1000

        finally:
            ctx.job_timeout_s = original_timeout

    def test_timeout_exception_type(self, ray_start_regular_shared):
        """Test that the correct exception type is raised."""
        ctx = DataContext.get_current()
        original_timeout = ctx.job_timeout_s

        try:
            ctx.job_timeout_s = 1

            ds = ray.data.range(100)

            def slow_fn(batch):
                time.sleep(0.5)
                return batch

            with pytest.raises(DatasetJobTimeoutError):
                ds.map_batches(slow_fn, batch_size=5).take_all()

        finally:
            ctx.job_timeout_s = original_timeout

    def test_timeout_zero_disables(self, ray_start_regular_shared):
        """Test that setting timeout to 0 disables it."""
        ctx = DataContext.get_current()
        original_timeout = ctx.job_timeout_s

        try:
            ctx.job_timeout_s = 0

            # Job should complete without timeout even if slow
            ds = ray.data.range(10)

            def slow_fn(batch):
                time.sleep(0.1)
                return batch

            result = ds.map_batches(slow_fn, batch_size=2).take_all()
            assert len(result) == 10

        finally:
            ctx.job_timeout_s = original_timeout

    def test_timeout_with_read_operations(self, ray_start_regular_shared, tmp_path):
        """Test timeout with read operations."""
        ctx = DataContext.get_current()
        original_timeout = ctx.job_timeout_s

        try:
            # Create test data
            import pandas as pd

            df = pd.DataFrame({"value": range(100)})
            path = tmp_path / "test.parquet"
            df.to_parquet(path)

            ctx.job_timeout_s = 10

            # Read should complete within timeout
            ds = ray.data.read_parquet(str(path))
            result = ds.take_all()
            assert len(result) == 100

        finally:
            ctx.job_timeout_s = original_timeout

    def test_timeout_propagates_through_execution(self, ray_start_regular_shared):
        """Test that timeout is properly checked throughout execution."""
        ctx = DataContext.get_current()
        original_timeout = ctx.job_timeout_s

        try:
            ctx.job_timeout_s = 2

            ds = ray.data.range(1000)

            def very_slow_fn(batch):
                time.sleep(1.0)  # Each batch takes 1 second
                return batch

            # Should timeout after ~2 seconds
            with pytest.raises(DatasetJobTimeoutError):
                ds.map_batches(very_slow_fn, batch_size=100).take_all()

        finally:
            ctx.job_timeout_s = original_timeout


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
