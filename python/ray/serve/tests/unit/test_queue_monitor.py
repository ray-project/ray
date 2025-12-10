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

    def test_sqs_broker_type(self):
        """Test SQS broker type detection."""
        config = QueueMonitorConfig(
            broker_url="sqs://...",
            queue_name="test_queue",
        )
        assert config.broker_type == "sqs"

    def test_unknown_broker_type(self):
        """Test unknown broker type detection."""
        config = QueueMonitorConfig(
            broker_url="some://unknown/broker",
            queue_name="test_queue",
        )
        assert config.broker_type == "unknown"

    def test_config_stores_values(self):
        """Test config stores provided values."""
        config = QueueMonitorConfig(
            broker_url="redis://localhost:6379",
            queue_name="my_queue",
        )
        assert config.broker_url == "redis://localhost:6379"
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

    @patch("ray.serve._private.queue_monitor.redis")
    def test_get_redis_queue_length(self, mock_redis_module, redis_config):
        """Test Redis queue length retrieval."""
        mock_client = MagicMock()
        mock_client.llen.return_value = 42
        mock_redis_module.from_url.return_value = mock_client

        monitor = QueueMonitor(redis_config)
        monitor.initialize()
        length = monitor.get_queue_length()

        assert length == 42
        mock_client.llen.assert_called_with("test_queue")

    @patch("ray.serve._private.queue_monitor.pika")
    def test_get_rabbitmq_queue_length(self, mock_pika, rabbitmq_config):
        """Test RabbitMQ queue length retrieval."""
        mock_params = MagicMock()
        mock_pika.URLParameters.return_value = mock_params

        # Mock for initialization
        mock_init_connection = MagicMock()
        # Mock for queue length query
        mock_query_connection = MagicMock()
        mock_channel = MagicMock()
        mock_result = MagicMock()
        mock_result.method.message_count = 25

        mock_query_connection.channel.return_value = mock_channel
        mock_channel.queue_declare.return_value = mock_result

        # First call is for initialization, second is for query
        mock_pika.BlockingConnection.side_effect = [
            mock_init_connection,
            mock_query_connection,
        ]

        monitor = QueueMonitor(rabbitmq_config)
        monitor.initialize()
        length = monitor.get_queue_length()

        assert length == 25
        mock_channel.queue_declare.assert_called_with(
            queue="test_queue",
            passive=True,
        )

    @patch("ray.serve._private.queue_monitor.redis")
    def test_get_queue_length_returns_cached_on_error(
        self, mock_redis_module, redis_config
    ):
        """Test get_queue_length returns cached value on error."""
        mock_client = MagicMock()
        mock_client.llen.return_value = 50
        mock_redis_module.from_url.return_value = mock_client

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
