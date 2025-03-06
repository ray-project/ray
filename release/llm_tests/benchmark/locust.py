from locust.env import Environment
from locust.stats import stats_printer, stats_history, print_stats
import gevent
from typing import Dict, Any
import logging
from locust.log import setup_logging

from .load_test import LLMUser, events, collect_metrics
from .configs import LoadTestConfig


class LLMLoadTester:
    """
    Usage Example:

    ```python
        config = LoadTestConfig(
            host="http://localhost:8000",
            provider="vllm",
            model="meta-llama/Meta-Llama-3.1-8B-Instruct",
            api_key="NONE",
            prompt_tokens=550,
            max_tokens=150,
            users=128,
            run_time="1m",
            summary_file="./vllm.csv"
        )
        tester = LLMLoadTester(config)
        results = tester.run()
    ```


    """

    def __init__(self, config: LoadTestConfig):
        self.config = config

    def _setup_environment(self) -> Environment:

        setup_logging("INFO", None)

        # Setup Environment and Runner
        env = Environment(
            user_classes=[LLMUser],
            host=self.config.host,
            reset_stats=self.config.reset_stats,
            events=events,
        )
        env.parsed_options = self.config.to_namespace()

        return env

    def run(self) -> Dict[str, Any]:
        try:
            # Setup environment
            env = self._setup_environment()
            env.create_local_runner()

            # Log test start
            logging.info(f"Starting test with {self.config.users} users")

            # Create greenlets for stats
            stats_printer_greenlet = gevent.spawn(stats_printer(env.stats))
            stats_history_greenlet = gevent.spawn(stats_history, env.runner)

            # Start the test
            env.runner.start(
                user_count=self.config.users,
                spawn_rate=self.config.users,
            )

            # Run for specified duration
            gevent.sleep(self._parse_time(self.config.run_time))

            # Stop the test
            env.runner.quit()
            entries = collect_metrics(env)

            # Wait for greenlets
            env.runner.greenlet.join()
            stats_printer_greenlet.kill()
            stats_history_greenlet.kill()

            # Print final stats
            print_stats(env.stats)

            return entries

        except Exception as e:
            logging.error(f"Test failed: {str(e)}")
            raise

    @staticmethod
    def _parse_time(time_str: str) -> int:
        """Convert time string (e.g., '30s', '1m', '1h') to seconds"""
        unit = time_str[-1]
        value = int(time_str[:-1])
        if unit == "s":
            return value
        elif unit == "m":
            return value * 60
        elif unit == "h":
            return value * 3600
        else:
            raise ValueError(f"Invalid time unit: {unit}")
