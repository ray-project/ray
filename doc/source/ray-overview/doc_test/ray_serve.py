# TODO: actually use this to predict something
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

    async def predict(self, query: str):
        return self.nlp_model(query, max_length=50)

    async def __call__(self, request):
        return self.predict(await request.body())


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
    return await handle.predict.remote(query)


@app.on_event("shutdown")  # Code to be run when the server shuts down.
async def shutdown_event():
    serve.shutdown()  # Shut down Ray Serve.
