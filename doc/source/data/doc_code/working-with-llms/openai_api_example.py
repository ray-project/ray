"""
This file serves as a documentation example and CI test for OpenAI API batch inference.

"""

import os
from ray.data.llm import HttpRequestProcessorConfig, build_llm_processor


def run_openai_example():
    # __openai_example_start__
    import ray

    OPENAI_KEY = os.environ["OPENAI_API_KEY"]
    ds = ray.data.from_items(["Hand me a haiku."])

    config = HttpRequestProcessorConfig(
        url="https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_KEY}"},
        qps=1,
    )

    processor = build_llm_processor(
        config,
        preprocess=lambda row: dict(
            payload=dict(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a bot that responds with haikus.",
                    },
                    {"role": "user", "content": row["item"]},
                ],
                temperature=0.0,
                max_tokens=150,
            ),
        ),
        postprocess=lambda row: dict(
            response=row["http_response"]["choices"][0]["message"]["content"]
        ),
    )

    ds = processor(ds)
    print(ds.take_all())
    # __openai_example_end__


def run_openai_demo():
    """Run the OpenAI API configuration demo."""
    print("OpenAI API Configuration Demo")
    print("=" * 30)
    print("\nExample configuration:")
    print("config = HttpRequestProcessorConfig(")
    print("    url='https://api.openai.com/v1/chat/completions',")
    print("    headers={'Authorization': f'Bearer {OPENAI_KEY}'},")
    print("    qps=1,")
    print(")")
    print("\nThe processor handles:")
    print("- Preprocessing: Convert text to OpenAI API format")
    print("- HTTP requests: Send batched requests to OpenAI")
    print("- Postprocessing: Extract response content")


def preprocess_for_openai(row):
    """Preprocess function for OpenAI API requests."""
    return dict(
        payload=dict(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": row["item"]},
            ],
            temperature=0.0,
            max_tokens=150,
        )
    )


def postprocess_openai_response(row):
    """Postprocess function for OpenAI API responses."""
    return dict(response=row["http_response"]["choices"][0]["message"]["content"])


if __name__ == "__main__":
    # Run live call if API key is set; otherwise show demo with mock output
    if "OPENAI_API_KEY" in os.environ:
        run_openai_example()
    else:
        # Mock response without API key
        print(
            [
                {
                    "response": (
                        "Autumn leaves whisper\nSoft code flows in quiet lines\nBugs fall one by one"
                    )
                }
            ]
        )
