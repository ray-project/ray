from unittest.mock import patch

import pytest

from ray._common.pydantic_compat import PYDANTIC_INSTALLED, ValidationError
from ray.dashboard.pydantic_models import BundleSchema, PlacementGroupSchema, TaskSchema
from ray.dashboard.state_api_utils import handle_list_api
from ray.util.state.common import ListApiOptions, ListApiResponse

pytestmark = pytest.mark.skipif(
    not PYDANTIC_INSTALLED, reason="Pydantic is not installed (minimal installation)"
)


def test_bundle_schema_validation():
    # Valid data
    valid_bundle_data = {
        "bundle_id": "bundle_123",
        "node_id": "node_123",
        "unit_resources": {"CPU": 1.0},
        "label_selector": {"key": "value"},
    }
    bundle = BundleSchema.parse_obj(valid_bundle_data)
    assert bundle.bundle_id == "bundle_123"

    # Invalid data (missing required field bundle_id)
    invalid_bundle_data = {
        "node_id": "node_123",
    }
    with pytest.raises(ValidationError):
        BundleSchema.parse_obj(invalid_bundle_data)


def test_placement_group_schema_validation():
    valid_pg_data = {
        "placement_group_id": "pg_123",
        "name": "my_pg",
        "creator_job_id": "job_123",
        "state": "CREATED",
        "stats": {"created_at": 1600000000},
        "bundles": [
            {
                "bundle_id": "bundle_123",
                "node_id": "node_123",
                "unit_resources": {"CPU": 1.0},
            }
        ],
    }
    pg = PlacementGroupSchema.parse_obj(valid_pg_data)
    assert pg.placement_group_id == "pg_123"
    assert len(pg.bundles) == 1

    # Invalid data (wrong type for bundles)
    invalid_pg_data = {
        "placement_group_id": "pg_123",
        "name": "my_pg",
        "creator_job_id": "job_123",
        "state": "CREATED",
        "bundles": "not_a_list",
    }
    with pytest.raises(ValidationError):
        PlacementGroupSchema.parse_obj(invalid_pg_data)


def test_task_schema_validation():
    valid_task_data = {
        "task_id": "task_123",
        "attempt_number": 0,
        "name": "my_task",
        "state": "RUNNING",
        "job_id": "job_123",
        "type": "NORMAL_TASK",
        "func_or_class_name": "my_func",
        "parent_task_id": "parent_123",
    }
    task = TaskSchema.parse_obj(valid_task_data)
    assert task.task_id == "task_123"
    assert task.state == "RUNNING"

    # Invalid data (missing required field task_id)
    invalid_task_data = {
        "attempt_number": 0,
        "name": "my_task",
    }
    with pytest.raises(ValidationError):
        TaskSchema.parse_obj(invalid_task_data)


@pytest.mark.asyncio
async def test_handle_list_api_enforcement():
    """Test that handle_list_api correctly enforces the schema and applies defaults.

    Specifically, it verifies that if a placement group is missing the 'bundles'
    field, the returned dictionary will have 'bundles' filled in with an empty
    list [].
    """
    # Mock API function that returns data missing placement group 'bundles'
    async def mock_list_api(option: ListApiOptions):
        return ListApiResponse(
            total=1,
            num_after_truncation=1,
            num_filtered=1,
            result=[
                {
                    "placement_group_id": "pg_123",
                    "name": "my_pg",
                    "creator_job_id": "job_123",
                    "state": "CREATED",
                    # 'bundles' is missing!
                }
            ],
        )

    # Mock request
    class MockQuery(dict):
        def getall(self, key, default=None):
            return self.get(key, default or [])

    class MockRequest:
        def __init__(self):
            self.query = MockQuery()

    req = MockRequest()

    # Mock do_reply to return the result_dict directly
    with patch("ray.dashboard.state_api_utils.do_reply") as mock_reply:
        mock_reply.side_effect = (
            lambda status_code, error_message, result, **kwargs: result
        )

        response_dict = await handle_list_api(
            mock_list_api, req, schema=PlacementGroupSchema
        )

        # Verify that 'bundles' was filled in with the default empty list
        assert "result" in response_dict
        assert len(response_dict["result"]) == 1
        assert "bundles" in response_dict["result"][0]
        assert response_dict["result"][0]["bundles"] == []
