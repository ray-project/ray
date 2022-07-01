import asyncio
from unittest.mock import MagicMock
import sys
import pytest
from ray.dashboard.optional_utils import rest_response

from dashboard.modules.state.state_head import StateHead


@pytest.mark.asyncio
@pytest.mark.parametrize("extra_req_num", [-5, -3, -1, 0, 1, 3, 5])
async def test_max_concurrent_in_progress_query(extra_req_num):
    """Test rate limiting for concurrent in-progress requests on StateHead"""
    import json

    dashboard_mock = MagicMock()
    max_req = 10
    state_head = StateHead(dashboard_mock)
    state_head._max_http_req_in_progress = max_req

    # Decorator with states
    @state_head.enforce_max_concurrent_calls
    async def fn(self_):
        await asyncio.sleep(3)
        return rest_response(
            success=True, message="", result={}, convert_google_style=False
        )

    # Run more than allowed concurrent async functions should trigger rate limiting
    res_arr = await asyncio.gather(
        *[fn(state_head) for _ in range(max_req + extra_req_num)]
    )
    fail_cnt = 0
    for res in res_arr:
        res = json.loads(res.text)
        fail_cnt += 0 if res["result"] else 1
    expected_fail_cnt = max(0, extra_req_num)
    assert fail_cnt == expected_fail_cnt, (
        f"{expected_fail_cnt} out of {max_req + extra_req_num} "
        f"concurrent runs should fail with max={max_req} but {fail_cnt}."
    )

    assert state_head._num_requests_in_progress == 0, "All requests should be done"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
