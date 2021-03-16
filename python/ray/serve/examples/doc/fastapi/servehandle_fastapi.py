import ray
from ray import serve

from fastapi import FastAPI
from transformers import pipeline

app = FastAPI()

serve_handle = None


@app.on_event("startup")  # Code to be run when the server starts.
async def startup_event():
    ray.init(address="auto")  # Connect to the running Ray cluster.
    serve.start(http_host=None)  # Start the Ray Serve instance.

    # Define a callable class to use for our Ray Serve backend.
    class GPT2:
        def __init__(self):
            self.nlp_model = pipeline("text-generation", model="gpt2")

        async def __call__(self, request):
            return self.nlp_model(await request.body(), max_length=50)

    # Set up a Ray Serve backend with the desired number of replicas.
    backend_config = serve.BackendConfig(num_replicas=2)
    serve.create_backend("gpt-2", GPT2, config=backend_config)
    serve.create_endpoint("generate", backend="gpt-2")

    # Get a handle to our Ray Serve endpoint so we can query it in Python.
    global serve_handle
    serve_handle = serve.get_handle("generate")


@app.get("/generate")
async def generate(query: str):
    return await serve_handle.remote(query)
