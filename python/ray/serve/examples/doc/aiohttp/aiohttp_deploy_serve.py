# File name: aiohttp_deploy_serve.py
import ray
from ray import serve

# Connect to the running Ray cluster.
ray.init(address="auto")

# Start a detached Ray Serve instance.  It will persist after the script exits.
serve.start(http_host=None, detached=True)


# Set up a deployment with the desired number of replicas. This could also be
# a stateful class (e.g., if we had an expensive model to set up).
@serve.deployment(name="my_model", num_replicas=2)
async def my_model(request):
    data = await request.body()
    return f"Model received data: {data}"


my_model.deploy()
