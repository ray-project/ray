import asyncio
import sys
import time

import pytest

from ray._common.test_utils import async_wait_for_condition, wait_for_condition


class TestWaitForCondition:
    """Tests for the synchronous wait_for_condition function."""

    def test_immediate_true_condition(self):
        """Test that function returns immediately when condition is already true."""

        def always_true():
            return True

        wait_for_condition(always_true, timeout=5)

    def test_condition_becomes_true(self):
        """Test waiting for a condition that becomes true after some time."""
        counter = {"value": 0}

        def condition():
            counter["value"] += 1
            return counter["value"] >= 3

        wait_for_condition(condition, timeout=5, retry_interval_ms=50)

        assert counter["value"] >= 3

    def test_timeout_raises_runtime_error(self):
        """Test that timeout raises RuntimeError with appropriate message."""

        def always_false():
            return False

        with pytest.raises(RuntimeError) as exc_info:
            wait_for_condition(always_false, timeout=0.2, retry_interval_ms=50)

        assert "condition wasn't met before the timeout expired" in str(exc_info.value)

    def test_condition_with_kwargs(self):
        """Test passing kwargs to the condition predictor."""

        def condition_with_args(target, current=0):
            return current >= target

        wait_for_condition(condition_with_args, timeout=1, target=5, current=10)

        # Should not raise an exception since current >= target

    def test_exception_handling_default(self):
        """Test that exceptions are caught by default and timeout occurs."""

        def failing_condition():
            raise ValueError("Test exception")

        with pytest.raises(RuntimeError) as exc_info:
            wait_for_condition(failing_condition, timeout=0.2, retry_interval_ms=50)

        error_msg = str(exc_info.value)
        assert "condition wasn't met before the timeout expired" in error_msg
        assert "Last exception:" in error_msg
        assert "ValueError: Test exception" in error_msg

    def test_exception_handling_raise_true(self):
        """Test that exceptions are raised when raise_exceptions=True."""

        def failing_condition():
            raise ValueError("Test exception")

        with pytest.raises(ValueError) as exc_info:
            wait_for_condition(failing_condition, timeout=1, raise_exceptions=True)

        assert "Test exception" in str(exc_info.value)

    def test_custom_retry_interval(self):
        """Test that custom retry intervals are respected."""
        call_times = []

        def condition():
            call_times.append(time.time())
            return len(call_times) >= 3

        wait_for_condition(condition, timeout=5, retry_interval_ms=200)

        # Verify that calls were spaced approximately 200ms apart
        if len(call_times) >= 2:
            interval = call_times[1] - call_times[0]
            assert 0.15 <= interval <= 0.25  # Allow some tolerance

    def test_condition_with_mixed_results(self):
        """Test condition that fails initially then succeeds."""
        attempts = {"count": 0}

        def intermittent_condition():
            attempts["count"] += 1
            # Succeed on the 4th attempt
            return attempts["count"] >= 4

        wait_for_condition(intermittent_condition, timeout=2, retry_interval_ms=100)
        assert attempts["count"] >= 4


class TestAsyncWaitForCondition:
    """Tests for the asynchronous async_wait_for_condition function."""

    @pytest.mark.asyncio
    async def test_immediate_true_condition(self):
        """Test that function returns immediately when condition is already true."""

        def always_true():
            return True

        await async_wait_for_condition(always_true, timeout=5)

    @pytest.mark.asyncio
    async def test_async_condition_becomes_true(self):
        """Test waiting for an async condition that becomes true after some time."""
        counter = {"value": 0}

        async def async_condition():
            counter["value"] += 1
            await asyncio.sleep(0.01)  # Small async operation
            return counter["value"] >= 3

        await async_wait_for_condition(async_condition, timeout=5, retry_interval_ms=50)

        assert counter["value"] >= 3

    @pytest.mark.asyncio
    async def test_sync_condition_becomes_true(self):
        """Test waiting for a sync condition in async context."""
        counter = {"value": 0}

        def sync_condition():
            counter["value"] += 1
            return counter["value"] >= 3

        await async_wait_for_condition(sync_condition, timeout=5, retry_interval_ms=50)
        assert counter["value"] >= 3

    @pytest.mark.asyncio
    async def test_timeout_raises_runtime_error(self):
        """Test that timeout raises RuntimeError with appropriate message."""

        def always_false():
            return False

        with pytest.raises(RuntimeError) as exc_info:
            await async_wait_for_condition(
                always_false, timeout=0.2, retry_interval_ms=50
            )

        assert "condition wasn't met before the timeout expired" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_condition_with_kwargs(self):
        """Test passing kwargs to the condition predictor."""

        def condition_with_args(target, current=0):
            return current >= target

        await async_wait_for_condition(
            condition_with_args, timeout=1, target=5, current=10
        )

        # Should not raise an exception since current >= target

    @pytest.mark.asyncio
    async def test_async_condition_with_kwargs(self):
        """Test passing kwargs to an async condition predictor."""

        async def async_condition_with_args(target, current=0):
            await asyncio.sleep(0.01)
            return current >= target

        await async_wait_for_condition(
            async_condition_with_args, timeout=1, target=5, current=10
        )

        # Should not raise an exception since current >= target

    @pytest.mark.asyncio
    async def test_exception_handling(self):
        """Test that exceptions are caught and timeout occurs."""

        def failing_condition():
            raise ValueError("Test exception")

        with pytest.raises(RuntimeError) as exc_info:
            await async_wait_for_condition(
                failing_condition, timeout=0.2, retry_interval_ms=50
            )

        error_msg = str(exc_info.value)
        assert "condition wasn't met before the timeout expired" in error_msg
        assert "Last exception:" in error_msg

    @pytest.mark.asyncio
    async def test_async_exception_handling(self):
        """Test that exceptions from async conditions are caught."""

        async def async_failing_condition():
            await asyncio.sleep(0.01)
            raise ValueError("Async test exception")

        with pytest.raises(RuntimeError) as exc_info:
            await async_wait_for_condition(
                async_failing_condition, timeout=0.2, retry_interval_ms=50
            )

        error_msg = str(exc_info.value)
        assert "condition wasn't met before the timeout expired" in error_msg
        assert "Last exception:" in error_msg

    @pytest.mark.asyncio
    async def test_custom_retry_interval(self):
        """Test that custom retry intervals are respected."""
        call_times = []

        def condition():
            call_times.append(time.time())
            return len(call_times) >= 3

        await async_wait_for_condition(condition, timeout=5, retry_interval_ms=200)

        # Verify that calls were spaced approximately 200ms apart
        if len(call_times) >= 2:
            interval = call_times[1] - call_times[0]
            assert 0.15 <= interval <= 0.25  # Allow some tolerance

    @pytest.mark.asyncio
    async def test_mixed_sync_async_conditions(self):
        """Test that both sync and async conditions work in the same test."""
        sync_counter = {"value": 0}
        async_counter = {"value": 0}

        def sync_condition():
            sync_counter["value"] += 1
            return sync_counter["value"] >= 2

        async def async_condition():
            async_counter["value"] += 1
            await asyncio.sleep(0.01)
            return async_counter["value"] >= 2

        # Test sync condition
        await async_wait_for_condition(sync_condition, timeout=2, retry_interval_ms=50)
        assert sync_counter["value"] >= 2

        # Test async condition
        await async_wait_for_condition(async_condition, timeout=2, retry_interval_ms=50)
        assert async_counter["value"] >= 2


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_zero_timeout(self):
        """Test behavior with zero timeout."""

        def slow_condition():
            time.sleep(0.1)
            return True

        with pytest.raises(RuntimeError):
            wait_for_condition(slow_condition, timeout=0, retry_interval_ms=50)

    @pytest.mark.asyncio
    async def test_async_zero_timeout(self):
        """Test async behavior with zero timeout."""

        async def slow_condition():
            await asyncio.sleep(0.1)
            return True

        with pytest.raises(RuntimeError):
            await async_wait_for_condition(
                slow_condition, timeout=0, retry_interval_ms=50
            )

    def test_very_small_retry_interval(self):
        """Test with very small retry interval."""
        counter = {"value": 0}

        def condition():
            counter["value"] += 1
            return counter["value"] >= 5

        start_time = time.time()
        wait_for_condition(condition, timeout=1, retry_interval_ms=1)
        elapsed = time.time() - start_time

        # Should complete quickly due to small retry interval
        assert elapsed < 0.5
        assert counter["value"] >= 5

    @pytest.mark.asyncio
    async def test_async_very_small_retry_interval(self):
        """Test async version with very small retry interval."""
        counter = {"value": 0}

        def condition():
            counter["value"] += 1
            return counter["value"] >= 5

        start_time = time.time()
        await async_wait_for_condition(condition, timeout=1, retry_interval_ms=1)
        elapsed = time.time() - start_time

        # Should complete quickly due to small retry interval
        assert elapsed < 0.5
        assert counter["value"] >= 5


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
