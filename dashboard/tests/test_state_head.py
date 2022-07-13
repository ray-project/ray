import asyncio
import sys
import pytest
from ray.dashboard.modules.state.state_head import RateLimitedModule


class FailedCallError(Exception):
    pass


class A(RateLimitedModule):
    def __init__(self, max_num_call: int):
        import logging

        super().__init__(max_num_call, logging.getLogger(__name__))

    @RateLimitedModule.enforce_max_concurrent_calls
    async def fn1(self, err: bool = False):
        if err:
            raise FailedCallError

        await asyncio.sleep(3)
        return True

    @RateLimitedModule.enforce_max_concurrent_calls
    async def fn2(self):
        await asyncio.sleep(3)
        return True

    async def limit_handler_(self):
        return False


@pytest.mark.asyncio
@pytest.mark.parametrize("extra_req_num", [-5, -3, -1, 0, 1, 3, 5])
async def test_max_concurrent_in_progress_functions(extra_req_num):
    """Test rate limiting for concurrent in-progress requests on StateHead"""
    max_req = 10
    a = A(max_num_call=max_req)

    # Run more than allowed concurrent async functions should trigger rate limiting
    res_arr = await asyncio.gather(
        *[a.fn1() if i % 2 == 0 else a.fn2() for i in range(max_req + extra_req_num)]
    )
    fail_cnt = 0
    for ok in res_arr:
        fail_cnt += 0 if ok else 1

    expected_fail_cnt = max(0, extra_req_num)
    assert fail_cnt == expected_fail_cnt, (
        f"{expected_fail_cnt} out of {max_req + extra_req_num} "
        f"concurrent runs should fail with max={max_req} but {fail_cnt}."
    )

    assert a.num_call_ == 0, "All requests should be done"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failures",
    [
        [True, True, True, True, True],
        [False, False, False, False, False],
        [False, True, False, True, False],
        [False, False, False, True, True],
        [True, True, False, False, False],
    ],
)
async def test_max_concurrent_with_exceptions(failures):

    max_req = 10
    a = A(max_num_call=max_req)

    # Run more than allowed concurrent async functions should trigger rate limiting
    res_arr = await asyncio.gather(
        *[a.fn1(err=should_throw_err) for should_throw_err in failures],
        return_exceptions=True,
    )

    expected_num_failure = sum(failures)

    actual_num_failure = 0
    for res in res_arr:
        if isinstance(res, FailedCallError):
            actual_num_failure += 1

    assert expected_num_failure == actual_num_failure, "All failures should be captured"
    assert a.num_call_ == 0, "Failure should decrement the counter correctly"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
