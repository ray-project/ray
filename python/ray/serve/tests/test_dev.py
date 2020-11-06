from ray.serve.context import TaskContext
from ray.serve.router import RequestMetadata, SyncRouter
from ray import serve
import asyncio
import ray
import time

from ray.serve.constants import SERVE_CONTROLLER_NAME
from ray.serve.router import SyncRouter, Query

#TODO: can't handle detached=False b/c can't serve.connect in handle
client = serve.start(detached=True)

client.create_backend("b", lambda f: "hi")
client.create_endpoint("a", backend="b")

router = SyncRouter()
router.setup("router", SERVE_CONTROLLER_NAME)
# while True:
#     router.long_pull_client.refresh()
#     print(router.long_pull_client.object_snapshots)
#     time.sleep(1)
ref = router.enqueue_request(RequestMetadata("a", TaskContext.Python))
print("Result gotten", ray.get(ref))

# client.create_backend("b", lambda f: "hi")
# client.create_endpoint("a", backend="b")

# async def main():
#     handle = client.get_handle("a")

#     handle.pull_state_sync()
#     handle.pull_state_sync()
#     client.update_backend_config("b", {"num_replicas": 2})
#     time.sleep(1)
#     handle.pull_state_sync()
#     # await asyncio.sleep(5)

#     client.update_backend_config("b", {"num_replicas": 4})
#     handle.pull_state_sync()
#     time.sleep(1)
#     # await asyncio.sleep(5)

#     client.update_backend_config("b", {"num_replicas": 1})
#     for _ in range(10):
#         handle.pull_state_sync()
#         time.sleep(0.1)
#     # await asyncio.sleep(5)

# asyncio.get_event_loop().run_until_complete(main())