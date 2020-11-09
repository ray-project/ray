import asyncio
import time

from ray.serve.handle import SyncServeHandle

import ray
from ray import serve
from ray.serve.constants import SERVE_CONTROLLER_NAME
from ray.serve.context import TaskContext
from ray.serve.router import LongPullRouter, Query, RequestMetadata

#TODO: can't handle detached=False b/c can't serve.connect in handle
client = serve.start(detached=True)

client.create_backend("b", lambda f: "hi")
client.create_endpoint("a", backend="b")

router = LongPullRouter("router", SERVE_CONTROLLER_NAME)
ref = router.enqueue_request_blocking(
    Query(
        [], {},
        TaskContext.Python,
        metadata=RequestMetadata("a", TaskContext.Python)))
print("Sync result received", ray.get(ref))


async def main():
    router = LongPullRouter("router", SERVE_CONTROLLER_NAME)
    ref = await router.enqueue_request_async(
        Query(
            [], {},
            TaskContext.Python,
            metadata=RequestMetadata("a", TaskContext.Python)))
    print("Async result received", ray.get(ref))


asyncio.get_event_loop().run_until_complete(main())
