import pytest

from ray._common.pydantic_compat import PYDANTIC_INSTALLED, ValidationError
from ray.dashboard.pydantic_models import ActorSchema, NodeDetailSchema, RayletSchema

pytestmark = pytest.mark.skipif(
    not PYDANTIC_INSTALLED, reason="Pydantic is not installed (minimal installation)"
)


def test_raylet_schema_validation():
    # Valid data
    valid_raylet_data = {
        "nodeId": "node_123",
        "state": "ALIVE",
        "numWorkers": 5,
        "pid": 1234,
        "nodeManagerPort": 54321,
        "startTime": 1600000000,
        "objectStoreAvailableMemory": 1000,
        "objectStoreUsedMemory": 500,
        "isHeadNode": True,
        "labels": {"key": "value"},
    }
    raylet = RayletSchema.parse_obj(valid_raylet_data)
    assert raylet.nodeId == "node_123"
    assert raylet.state == "ALIVE"

    # Invalid data (missing required field nodeId)
    invalid_raylet_data = {
        "state": "ALIVE",
        "numWorkers": 5,
    }
    with pytest.raises(ValidationError):
        RayletSchema.parse_obj(invalid_raylet_data)


def test_node_detail_schema_validation():
    valid_node_data = {
        "hostname": "host-1",
        "ip": "127.0.0.1",
        "cpu": 50.5,
        "raylet": {
            "nodeId": "node_123",
            "state": "ALIVE",
            "numWorkers": 5,
            "pid": 1234,
            "nodeManagerPort": 54321,
            "startTime": 1600000000,
            "objectStoreAvailableMemory": 1000,
            "objectStoreUsedMemory": 500,
            "isHeadNode": True,
            "labels": {},
        },
    }
    node_detail = NodeDetailSchema.parse_obj(valid_node_data)
    assert node_detail.hostname == "host-1"
    assert node_detail.raylet.nodeId == "node_123"

    # Invalid data (wrong type for cpu)
    invalid_node_data = {
        "hostname": "host-1",
        "ip": "127.0.0.1",
        "cpu": "high",  # Should be float
        "raylet": {
            "nodeId": "node_123",
            "state": "ALIVE",
            "numWorkers": 5,
            "pid": 1234,
            "nodeManagerPort": 54321,
            "startTime": 1600000000,
            "objectStoreAvailableMemory": 1000,
            "objectStoreUsedMemory": 500,
            "isHeadNode": True,
            "labels": {},
        },
    }
    with pytest.raises(ValidationError):
        NodeDetailSchema.parse_obj(invalid_node_data)


def test_actor_schema_validation():
    valid_actor_data = {
        "actorId": "actor_123",
        "jobId": "job_123",
        "state": "ALIVE",
        "address": {
            "nodeId": "node_123",
            "ipAddress": "127.0.0.1",
            "port": 1234,
            "workerId": "worker_123",
        },
        "name": "my_actor",
        "numRestarts": "0",
        "actorClass": "MyActor",
        "exitDetail": "",
        "reprName": "MyActor",
    }
    actor = ActorSchema.parse_obj(valid_actor_data)
    assert actor.actorId == "actor_123"
    assert actor.state == "ALIVE"

    # Invalid data (missing required field address)
    invalid_actor_data = {
        "actorId": "actor_123",
        "jobId": "job_123",
        "state": "ALIVE",
    }
    with pytest.raises(ValidationError):
        ActorSchema.parse_obj(invalid_actor_data)
