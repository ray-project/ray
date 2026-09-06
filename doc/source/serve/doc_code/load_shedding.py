# flake8: noqa
# fmt: off

# __example_deployment_start__
import time
from ray import serve
from starlette.requests import Request

@serve.deployment(
    # Each replica will be sent 2 requests at a time.
    max_ongoing_requests=2,
    # Each caller queues up to 2 requests at a time.
    # (beyond those that are sent to replicas).
    max_queued_requests=2,
)
class SlowDeployment:
    def __call__(self, request: Request) -> str:
        # Emulate a long-running request, such as ML inference.
        time.sleep(2)
        return "Hello!"
# __example_deployment_end__

# __client_test_start__
import ray
import aiohttp

@ray.remote
class Requester:
    async def do_request(self) -> int:
        async with aiohttp.ClientSession("http://localhost:8000/") as session:
            return (await session.get("/")).status

r = Requester.remote()
serve.run(SlowDeployment.bind())

# Send 4 requests first.
# 2 of these will be sent to the replica. These requests take a few seconds to execute.
first_refs = [r.do_request.remote() for _ in range(2)]
_, pending = ray.wait(first_refs, timeout=1)
assert len(pending) == 2
# 2 will be queued in the proxy.
queued_refs = [r.do_request.remote() for _ in range(2)]
_, pending = ray.wait(queued_refs, timeout=0.1)
assert len(pending) == 2

# Send an additional 5 requests. These will be rejected immediately because
# the replica and the proxy queue are already full.
for status_code in ray.get([r.do_request.remote() for _ in range(5)]):
    assert status_code == 503

# The initial requests will finish successfully.
for ref in first_refs:
    print(f"Request finished with status code {ray.get(ref)}.")

# __client_test_end__

# __custom_response_deployment_start__
from ray.serve.config import BackpressureConfig

@serve.deployment(
    max_ongoing_requests=2,
    max_queued_requests=2,
    # Return "429 Too Many Requests" instead of "503 Service Unavailable"
    # when shedding load, with a suggested retry delay of 5 seconds.
    backpressure_config=BackpressureConfig(
        status_code=429,
        retry_after_s=5,
    ),
)
class SlowDeploymentWithCustomResponse:
    def __call__(self, request: Request) -> str:
        # Emulate a long-running request, such as ML inference.
        time.sleep(2)
        return "Hello!"
# __custom_response_deployment_end__

# __custom_response_test_start__
@ray.remote
class RequesterWithHeaders:
    async def do_request(self) -> tuple:
        async with aiohttp.ClientSession("http://localhost:8000/") as session:
            response = await session.get("/")
            return response.status, response.headers.get("Retry-After")

serve.run(SlowDeploymentWithCustomResponse.bind())
r = RequesterWithHeaders.remote()

# Saturate the replica and the proxy queue.
first_refs = [r.do_request.remote() for _ in range(2)]
_, pending = ray.wait(first_refs, timeout=1)
assert len(pending) == 2
queued_refs = [r.do_request.remote() for _ in range(2)]
_, pending = ray.wait(queued_refs, timeout=0.1)
assert len(pending) == 2

# Additional requests are rejected with the configured status code and a
# "Retry-After" header telling clients when to try again.
for status_code, retry_after in ray.get([r.do_request.remote() for _ in range(5)]):
    assert status_code == 429
    assert retry_after == "5"
# __custom_response_test_end__
