import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ray.serve._private.queue_monitor import QueueMonitorConfig


class TestQueueMonitorConfig:
    """Unit tests for QueueMonitorConfig class."""

    def test_config_stores_values(self):
        """Test config stores broker_url, queue_name, and rabbitmq_http_url."""
        config = QueueMonitorConfig(
            broker_url="redis://localhost:6379/0",
            queue_name="test_queue",
        )

        assert config.broker_url == "redis://localhost:6379/0"
        assert config.queue_name == "test_queue"
        assert config.rabbitmq_http_url == "http://guest:guest@localhost:15672/api/"

    def test_config_custom_rabbitmq_http_url(self):
        """Test config stores custom rabbitmq_http_url."""
        config = QueueMonitorConfig(
            broker_url="amqp://guest:guest@localhost:5672//",
            queue_name="my_queue",
            rabbitmq_http_url="http://user:pass@localhost:15672/api/",
        )

        assert config.broker_url == "amqp://guest:guest@localhost:5672//"
        assert config.queue_name == "my_queue"
        assert config.rabbitmq_http_url == "http://user:pass@localhost:15672/api/"


class TestQueueMonitorCaching:
    """Unit tests for QueueMonitor caching behavior using mocks."""

    @pytest.mark.asyncio
    async def test_returns_cached_value_on_broker_error(self):
        """Test that get_queue_length returns cached value when broker raises error."""
        from ray.serve._private.queue_monitor import QueueMonitorActor

        config = QueueMonitorConfig(
            broker_url="redis://localhost:6379/0",
            queue_name="test_queue",
        )

        # Create a mock broker
        mock_broker = MagicMock()

        with patch("ray.serve._private.queue_monitor.Broker", return_value=mock_broker):
            # Create the actor's underlying class (not as a Ray actor for unit testing)
            actor = QueueMonitorActor.__ray_actor_class__(config)

            # First call succeeds and returns 50
            mock_broker.queues = AsyncMock(
                return_value=[{"name": "test_queue", "messages": 50}]
            )
            length = await actor.get_queue_length()
            assert length == 50

            # Second call raises an error - should return cached value
            mock_broker.queues = AsyncMock(side_effect=Exception("Connection failed"))
            length = await actor.get_queue_length()
            assert length == 50  # Returns cached value

    @pytest.mark.asyncio
    async def test_updates_cache_on_successful_query(self):
        """Test that cache is updated on successful queries."""
        from ray.serve._private.queue_monitor import QueueMonitorActor

        config = QueueMonitorConfig(
            broker_url="redis://localhost:6379/0",
            queue_name="test_queue",
        )

        mock_broker = MagicMock()

        with patch("ray.serve._private.queue_monitor.Broker", return_value=mock_broker):
            actor = QueueMonitorActor.__ray_actor_class__(config)

            # First call returns 50
            mock_broker.queues = AsyncMock(
                return_value=[{"name": "test_queue", "messages": 50}]
            )
            length = await actor.get_queue_length()
            assert length == 50

            # Second call returns 100 - cache should update
            mock_broker.queues = AsyncMock(
                return_value=[{"name": "test_queue", "messages": 100}]
            )
            length = await actor.get_queue_length()
            assert length == 100

            # Third call fails - should return latest cached value (100)
            mock_broker.queues = AsyncMock(side_effect=Exception("Connection failed"))
            length = await actor.get_queue_length()
            assert length == 100


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
