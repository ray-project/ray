# __serve_example_begin__
import time
from ray import serve


@serve.deployment(num_replicas=3, external_scaler_enabled=True)
class TextProcessor:
    """A simple text processing deployment that can be scaled externally."""
    def __init__(self):
        self.request_count = 0

    def __call__(self, text: str) -> dict:
        # Simulate text processing work
        time.sleep(0.1)
        self.request_count += 1
        return {
            "processed_text": text.upper(),
            "length": len(text),
            "request_count": self.request_count,
        }


app = TextProcessor.bind()
# __serve_example_end__

if __name__ == "__main__":
    import requests

    serve.run(app)
    
    # Test the deployment
    resp = requests.post(
        "http://localhost:8000/",
        json="hello world"
    )
    print(f"Response: {resp.json()}")

