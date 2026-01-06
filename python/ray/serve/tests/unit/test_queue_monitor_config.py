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
