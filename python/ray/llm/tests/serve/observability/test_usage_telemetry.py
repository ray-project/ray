import sys

import pytest

import ray
from ray.llm._internal.serve.observability.usage_telemetry.usage import (
    _retry_get_telemetry_agent,
)


@ray.remote
class Replica:
    def __init__(self):
        self.telemetry_agent = _retry_get_telemetry_agent()

    def get_telemetry_agent(self):
        return self.telemetry_agent


def test_telemetry_race_condition():
    replicas = [Replica.remote() for _ in range(20)]
    telemetry_agents = [
        ray.get(replica.get_telemetry_agent.remote()) for replica in replicas
    ]
    for telemetry_agent in telemetry_agents:
        assert telemetry_agent is not None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
