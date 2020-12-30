# File name: deploy_serve.py
import ray
from ray import serve

# Connect to the running Ray cluster.
ray.init(address="auto")

# Start a detached Ray Serve instance.  It will persist after the script exits.
client = serve.start(http_host=None, detached=True)

# Define a function to serve. Alternatively, you could define a stateful class.
async def my_model(request):
    data = await request.body()
    return f"Model received data: {data}"

client.create_backend("my_backend", my_model)
client.create_endpoint("my_endpoint", backend="my_backend")
