import sys
import uuid
from typing import Any, Dict, List

import pytest

import ray
from ray._common.state import actors


@pytest.fixture(scope="module")
def ray_init():
    ray.init(num_cpus=4, namespace="test-state-internal")
    yield
    ray.shutdown()


EXPECTED_ROW_KEYS = {"Name", "ActorClassName", "ActorID", "State", "JobID"}


def _rows(result) -> List[Dict[str, Any]]:
    """Normalize actor_table output (list or dict) to a list of dicts."""
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        if not result:
            return []
        if any(k in result for k in EXPECTED_ROW_KEYS):
            return [result]
        return list(result.values())
    return []


def _names(rows: List[Dict[str, Any]]) -> List[str]:
    """Extract actor names (handles possible key variants)."""
    out = []
    for r in rows:
        for k in ("Name", "name", "actor_name"):
            if k in r and r[k]:
                out.append(r[k])
                break
    return out


def test_actors_find_named_and_missing(ray_init):
    @ray.remote
    class Pinger:
        def ping(self):
            return "pong"

    name1 = f"actor-{uuid.uuid4().hex[:8]}"
    name2 = f"actor-{uuid.uuid4().hex[:8]}"
    missing_name = f"actor-missing-{uuid.uuid4().hex[:8]}"

    a1 = Pinger.options(name=name1).remote()
    a2 = Pinger.options(name=name2).remote()

    # Starts actors
    assert ray.get(a1.ping.remote()) == "pong"
    assert ray.get(a2.ping.remote()) == "pong"

    job_id = ray.get_runtime_context().job_id
    result = actors(job_id=job_id, actor_state_name="ALIVE")
    rows = _rows(result)
    names = _names(rows)

    assert name1 in names, f"{name1} not found in actor table: {names}"
    assert name2 in names, f"{name2} not found in actor table: {names}"

    assert missing_name not in names


def test_actors_find_by_actor_id_gets_singleton(ray_init):
    @ray.remote
    class Actor:
        def get_id(self):
            ctx = ray.get_runtime_context()
            return ctx.get_actor_id()

    name1 = f"actor-{uuid.uuid4().hex[:8]}"
    name2 = f"actor-{uuid.uuid4().hex[:8]}"
    missing_name = f"actor-missing-{uuid.uuid4().hex[:8]}"

    a1 = Actor.options(name=name1).remote()
    a2 = Actor.options(name=name2).remote()

    a1_id = ray.get(a1.get_id.remote())
    _ = ray.get(a2.get_id.remote())

    result = actors(actor_id=a1_id)
    rows = _rows(result)
    names = _names(rows)

    assert name1 in names, f"{name1} not found in actor table: {names}"

    assert name2 not in names
    assert missing_name not in names


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
