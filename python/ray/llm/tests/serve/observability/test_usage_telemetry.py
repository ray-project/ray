import sys

import pytest

import ray
from ray.llm._internal.serve.observability.usage_telemetry.usage import (
    _retry_get_telemetry_agent,
)


@ray.remote(lifetime="detached")
class Replica:
    def wait_for_init(self):
        """
        When this method returns, the actor initialization is guaranteed
        to be complete.

        This is used for synchronization between multiple replicas,
        increasing the chance for get_telemetry_agent() to be called
        at the same time.
        """
        pass

    def get_telemetry_agent(self):
        return _retry_get_telemetry_agent()


def test_telemetry_race_condition():
    replicas = [Replica.remote() for _ in range(30)]
    init_refs = [replica.wait_for_init.remote() for replica in replicas]
    ray.get(init_refs)

    get_refs = [replica.get_telemetry_agent.remote() for replica in replicas]
    telemetry_agents = ray.get(get_refs)
    for telemetry_agent in telemetry_agents:
        assert telemetry_agent is not None
    assert len(set(telemetry_agents)) == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
