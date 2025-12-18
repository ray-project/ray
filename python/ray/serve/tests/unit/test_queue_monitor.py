import sys
from unittest.mock import MagicMock, patch

import pytest

from ray.serve._private.queue_monitor import (
    QueueMonitor,
    QueueMonitorConfig,
)


class TestQueueMonitorConfig:
    """Tests for QueueMonitorConfig class."""

    def test_redis_broker_type(self):
        """Test Redis broker type detection."""
        config = QueueMonitorConfig(
            broker_url="redis://localhost:6379/0",
            queue_name="test_queue",
        )
        assert config.broker_type == "redis"

    def test_redis_with_password_broker_type(self):
        """Test Redis with password broker type detection."""
        config = QueueMonitorConfig(
            broker_url="rediss://user:password@localhost:6379/0",
            queue_name="test_queue",
        )
        assert config.broker_type == "redis"

    def test_rabbitmq_amqp_broker_type(self):
        """Test RabbitMQ AMQP broker type detection."""
        config = QueueMonitorConfig(
            broker_url="amqp://guest:guest@localhost:5672//",
            queue_name="test_queue",
        )
        assert config.broker_type == "rabbitmq"

    def test_rabbitmq_pyamqp_broker_type(self):
        """Test RabbitMQ pyamqp broker type detection."""
        config = QueueMonitorConfig(
            broker_url="pyamqp://guest:guest@localhost:5672//",
            queue_name="test_queue",
        )
        assert config.broker_type == "rabbitmq"

    def test_unknown_broker_type(self):
        """Test unknown broker type detection."""
        config = QueueMonitorConfig(
            broker_url="some://unknown/broker",
            queue_name="test_queue",
        )
        assert config.broker_type == "unknown"

    def test_config_stores_values(self):
        """Test config stores broker_url and queue_name."""
        config = QueueMonitorConfig(
            broker_url="amqp://guest:guest@localhost:5672//",
            queue_name="my_queue",
        )
        assert config.broker_url == "amqp://guest:guest@localhost:5672//"
        assert config.queue_name == "my_queue"


class TestQueueMonitor:
    """Tests for QueueMonitor class."""

    @pytest.fixture
    def redis_config(self):
        """Provides a Redis QueueMonitorConfig."""
        return QueueMonitorConfig(
            broker_url="redis://localhost:6379/0",
            queue_name="test_queue",
        )

    @pytest.fixture
    def rabbitmq_config(self):
        """Provides a RabbitMQ QueueMonitorConfig."""
        return QueueMonitorConfig(
            broker_url="amqp://guest:guest@localhost:5672//",
            queue_name="test_queue",
        )

    @patch("redis.from_url")
    def test_get_redis_queue_length(self, mock_from_url, redis_config):
        """Test Redis queue length returns pending tasks."""
        mock_client = MagicMock()
        mock_client.llen.return_value = 30
        mock_from_url.return_value = mock_client

        monitor = QueueMonitor(redis_config)
        monitor.initialize()
        length = monitor.get_queue_length()

        assert length == 30
        mock_client.llen.assert_called_with("test_queue")

    @patch("pika.BlockingConnection")
    @patch("pika.URLParameters")
    def test_get_rabbitmq_queue_length(
        self, mock_url_params, mock_blocking_connection, rabbitmq_config
    ):
        """Test RabbitMQ queue length retrieval via AMQP."""
        mock_params = MagicMock()
        mock_url_params.return_value = mock_params

        # Mock connection and channel - connection is reused across calls
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_result = MagicMock()
        mock_result.method.message_count = 25

        # Ensure connection appears open so it's not recreated
        mock_connection.is_closed = False
        mock_channel.is_closed = False

        mock_connection.channel.return_value = mock_channel
        mock_channel.queue_declare.return_value = mock_result
        mock_blocking_connection.return_value = mock_connection

        monitor = QueueMonitor(rabbitmq_config)
        monitor.initialize()
        length = monitor.get_queue_length()

        assert length == 25
        # Connection is established once during initialization
        mock_blocking_connection.assert_called_once()
        mock_channel.queue_declare.assert_called_with(
            queue="test_queue",
            passive=True,
        )

    @patch("redis.from_url")
    def test_get_queue_length_returns_cached_on_error(
        self, mock_from_url, redis_config
    ):
        """Test get_queue_length returns cached value on error."""
        mock_client = MagicMock()
        mock_client.llen.return_value = 50
        mock_from_url.return_value = mock_client

        monitor = QueueMonitor(redis_config)
        monitor.initialize()

        # First successful query
        length = monitor.get_queue_length()
        assert length == 50

        # Now make queries fail
        mock_client.llen.side_effect = Exception("Connection lost")

        # Should return cached value
        length = monitor.get_queue_length()
        assert length == 50


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
