# __example_code_start__
from fastapi import FastAPI
import torch
from transformers import pipeline

from ray import serve
from ray.serve.handle import DeploymentHandle


app = FastAPI()


@serve.deployment(num_replicas=1)
@serve.ingress(app)
class APIIngress:
    def __init__(self, distilbert_model_handle: DeploymentHandle) -> None:
        self.handle = distilbert_model_handle

    @app.get("/classify")
    async def classify(self, sentence: str):
        return await self.handle.classify.remote(sentence)


@serve.deployment(
    ray_actor_options={"num_gpus": 1},
    autoscaling_config={"min_replicas": 0, "max_replicas": 2},
)
class DistilBertModel:
    def __init__(self):
        self.classifier = pipeline(
            "sentiment-analysis",
            model="distilbert-base-uncased",
            framework="pt",
            # Transformers requires you to pass device with index
            device=torch.device("cuda:0"),
        )

    def classify(self, sentence: str):
        return self.classifier(sentence)


entrypoint = APIIngress.bind(DistilBertModel.bind())

# __example_code_end__

if __name__ == "__main__":
    import sys
    import traceback
    import requests
    import ray

    # Force unbuffered output so logs appear immediately
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    print("=" * 60, flush=True)
    print("DISTILBERT TEST: Starting test execution", flush=True)
    print("=" * 60, flush=True)

    try:
        print("DISTILBERT TEST: Checking CUDA availability...", flush=True)
        print(f"  torch.cuda.is_available(): {torch.cuda.is_available()}", flush=True)
        if torch.cuda.is_available():
            print(f"  torch.cuda.device_count(): {torch.cuda.device_count()}", flush=True)
            print(f"  torch.cuda.current_device(): {torch.cuda.current_device()}", flush=True)
            print(f"  torch.cuda.get_device_name(0): {torch.cuda.get_device_name(0)}", flush=True)
        else:
            print("  WARNING: CUDA is NOT available!", flush=True)

        print("DISTILBERT TEST: Initializing Ray...", flush=True)
        ray.init(runtime_env={"pip": ["transformers==4.52.4", "accelerate==1.7.0"]})
        print("DISTILBERT TEST: Ray initialized successfully", flush=True)

        print("DISTILBERT TEST: Starting Ray Serve deployment...", flush=True)
        serve.run(entrypoint)
        print("DISTILBERT TEST: Ray Serve deployment started successfully", flush=True)

        prompt = (
            "This was a masterpiece. Not completely faithful to the books, but "
            "enthralling  from beginning to end. Might be my favorite of the three."
        )
        input = "%20".join(prompt.split(" "))

        print(f"DISTILBERT TEST: Making request to classify endpoint...", flush=True)
        resp = requests.get(f"http://127.0.0.1:8000/classify?sentence={prompt}")
        print(f"DISTILBERT TEST: Response status_code: {resp.status_code}", flush=True)
        print(f"DISTILBERT TEST: Response body: {resp.text}", flush=True)

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        print("DISTILBERT TEST: Test PASSED!", flush=True)

    except Exception as e:
        print("=" * 60, flush=True)
        print(f"DISTILBERT TEST: FAILED with exception: {type(e).__name__}: {e}", flush=True)
        print("=" * 60, flush=True)
        traceback.print_exc()
        sys.exit(1)
    finally:
        print("DISTILBERT TEST: Cleaning up...", flush=True)
        try:
            ray.shutdown()
            print("DISTILBERT TEST: Ray shutdown complete", flush=True)
        except Exception as cleanup_error:
            print(f"DISTILBERT TEST: Cleanup error: {cleanup_error}", flush=True)
