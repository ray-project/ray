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


# Set up a backend with the desired number of replicas and set up an endpoint.
backend_config = serve.BackendConfig(num_replicas=2)
client.create_backend("my_backend", my_model, config=backend_config)
client.create_endpoint("my_endpoint", backend="my_backend")
