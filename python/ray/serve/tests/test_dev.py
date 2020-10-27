from ray import serve
import asyncio
import ray

#TODO: can't handle detached=False b/c can't serve.connect in handle
client = serve.start(detached=True)

client.create_backend("b", lambda f: "hi")
client.create_endpoint("a", backend="b")


async def main():
    handle = client.get_handle("a")
    client.update_backend_config("b", {"num_replicas": 2})
    await asyncio.sleep(5)

    client.update_backend_config("b", {"num_replicas": 4})
    await asyncio.sleep(5)

    client.update_backend_config("b", {"num_replicas": 1})
    await asyncio.sleep(5)


asyncio.get_event_loop().run_until_complete(main())