# flake8: noqa

# fmt: off
# __tasks_fault_tolerance_retries_begin__
import numpy as np
import os
import ray
import time

ray.init(ignore_reinit_error=True)

@ray.remote(max_retries=1)
def potentially_fail(failure_probability):
    time.sleep(0.2)
    if np.random.random() < failure_probability:
        os._exit(0)
    return 0

for _ in range(3):
    try:
        # If this task crashes, Ray will retry it up to one additional
        # time. If either of the attempts succeeds, the call to ray.get
        # below will return normally. Otherwise, it will raise an
        # exception.
        ray.get(potentially_fail.remote(0.5))
        print('SUCCESS')
    except ray.exceptions.WorkerCrashedError:
        print('FAILURE')
# __tasks_fault_tolerance_retries_end__
# fmt: on

# fmt: off
# __tasks_fault_tolerance_retries_exception_begin__
import numpy as np
import os
import ray
import time

ray.init(ignore_reinit_error=True)

class RandomError(Exception):
    pass

@ray.remote(max_retries=1, retry_exceptions=True)
def potentially_fail(failure_probability):
    if failure_probability < 0 or failure_probability > 1:
        raise ValueError(
            "failure_probability must be between 0 and 1, but got: "
            f"{failure_probability}"
        )
    time.sleep(0.2)
    if np.random.random() < failure_probability:
        raise RandomError("Failed!")
    return 0

for _ in range(3):
    try:
        # If this task crashes, Ray will retry it up to one additional
        # time. If either of the attempts succeeds, the call to ray.get
        # below will return normally. Otherwise, it will raise an
        # exception.
        ray.get(potentially_fail.remote(0.5))
        print('SUCCESS')
    except RandomError:
        print('FAILURE')

# Provide the exceptions that we want to retry as an allowlist.
retry_on_exception = potentially_fail.options(retry_exceptions=[RandomError])
try:
    # This will fail since we're passing in -1 for the failure_probability,
    # which will raise a ValueError in the task and does not match the RandomError
    # exception that we provided.
    ray.get(retry_on_exception.remote(-1))
except ValueError:
    print("FAILED AS EXPECTED")
else:
    raise RuntimeError("An exception should be raised so this shouldn't be reached.")

# These will retry on the RandomError exception.
for _ in range(3):
    try:
        # If this task crashes, Ray will retry it up to one additional
        # time. If either of the attempts succeeds, the call to ray.get
        # below will return normally. Otherwise, it will raise an
        # exception.
        ray.get(retry_on_exception.remote(0.5))
        print('SUCCESS')
    except RandomError:
        print('FAILURE AFTER RETRIES')
# __tasks_fault_tolerance_retries_exception_end__
# fmt: on
