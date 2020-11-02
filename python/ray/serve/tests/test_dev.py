from ray import serve
import asyncio
import ray
import time

#TODO: can't handle detached=False b/c can't serve.connect in handle
client = serve.start(detached=True)

client.create_backend("b", lambda f: "hi")
client.create_endpoint("a", backend="b")


async def main():
    handle = client.get_handle("a")

    handle.pull_state_sync()
    handle.pull_state_sync()
    client.update_backend_config("b", {"num_replicas": 2})
    time.sleep(1)
    handle.pull_state_sync()
    # await asyncio.sleep(5)

    client.update_backend_config("b", {"num_replicas": 4})
    handle.pull_state_sync()
    time.sleep(1)
    # await asyncio.sleep(5)

    client.update_backend_config("b", {"num_replicas": 1})
    for _ in range(10):
        handle.pull_state_sync()
        time.sleep(0.1)
    # await asyncio.sleep(5)


asyncio.get_event_loop().run_until_complete(main())