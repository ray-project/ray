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

# Send 4 requests first. 2 of these will be sent to the replica and 2 will
# be queued in the proxy. These requests take a few seconds to execute.
first_refs = [r.do_request.remote() for _ in range(4)]
_, pending = ray.wait(first_refs, timeout=1)
assert len(pending) == 4

# Send an additional 5 requests. These will be rejected immediately because
# the replica and the proxy queue are already full.
for status_code in ray.get([r.do_request.remote() for _ in range(5)]):
    assert status_code == 503

# The initial requests will finish successfully.
for ref in first_refs:
    print(f"Request finished with status code {ray.get(ref)}.")

# __client_test_end__
