import ray
from ray import serve

from fastapi import FastAPI
from transformers import pipeline

app = FastAPI()


# Define our deployment.
@serve.deployment(num_replicas=2)
class GPT2:
    def __init__(self):
        self.nlp_model = pipeline("text-generation", model="gpt2")

    async def __call__(self, request):
        return self.nlp_model(await request.body(), max_length=50)


@app.on_event("startup")  # Code to be run when the server starts.
async def startup_event():
    ray.init(address="auto")  # Connect to the running Ray cluster.
    serve.start(http_host=None)  # Start the Ray Serve instance.

    # Deploy our GPT2 Deployment.
    GPT2.deploy()


@app.get("/generate")
async def generate(query: str):
    # Get a handle to our deployment so we can query it in Python.
    handle = GPT2.get_handle()
    return await handle.remote(query)
