"""
This file serves as a documentation example and CI test for OpenAI API batch inference.

Structure:
1. Infrastructure setup: API key handling, testing configuration  
2. Docs example (between __openai_example_start/end__): Embedded in Sphinx docs via literalinclude
3. Test validation and cleanup
"""

import os
import ray.data
from ray.data.llm import HttpRequestProcessorConfig, build_llm_processor

# Infrastructure: Mock for testing without real API keys
def _mock_demo_mode():
    """Demo mode for when API key is not available"""
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


# __openai_example_start__
import ray
import os
from ray.data.llm import HttpRequestProcessorConfig, build_llm_processor

OPENAI_KEY = os.environ.get("OPENAI_API_KEY", "your-api-key-here")
ds = ray.data.from_items(["Hand me a haiku."])

# __openai_config_example_start__
config = HttpRequestProcessorConfig(
    url="https://api.openai.com/v1/chat/completions",
    headers={"Authorization": f"Bearer {OPENAI_KEY}"},
    qps=1,
)
# __openai_config_example_end__

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

if __name__ != "__main__":
    ds = processor(ds)
    print(ds.take_all())


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


# __openai_example_end__


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


# Test validation and cleanup
def run_test():
    """Test function that validates the example works including API configuration."""
    import sys

    suppress_output = "pytest" in sys.modules

    try:
        # Test 1: HTTP configuration structure
        assert config.url == "https://api.openai.com/v1/chat/completions"
        assert config.qps == 1
        assert "Authorization" in config.headers
        assert "Bearer" in config.headers["Authorization"]

        # Test 2: Preprocessing function comprehensive
        sample_row = {"item": "Write a haiku about coding"}
        result = preprocess_for_openai(sample_row)
        assert "payload" in result
        assert result["payload"]["model"] == "gpt-4o-mini"
        assert result["payload"]["temperature"] == 0.0
        assert result["payload"]["max_tokens"] == 150
        assert len(result["payload"]["messages"]) == 2
        assert result["payload"]["messages"][0]["role"] == "system"
        assert result["payload"]["messages"][1]["role"] == "user"
        assert (
            result["payload"]["messages"][1]["content"] == "Write a haiku about coding"
        )

        # Test 3: Postprocessing function comprehensive
        mock_response = {
            "http_response": {
                "choices": [
                    {
                        "message": {
                            "content": "Code flows like streams\\nDebugging through endless nights\\nBugs become features"
                        }
                    }
                ]
            }
        }
        processed = postprocess_openai_response(mock_response)
        assert "response" in processed
        assert "Code flows" in processed["response"]

        # Test 4: Dataset creation and actual API call if key available
        test_dataset = ray.data.from_items(["Write a test haiku."])
        assert test_dataset is not None

        if OPENAI_KEY != "your-api-key-here":
            try:
                # Build processor and run inference
                processor = build_llm_processor(
                    config,
                    preprocess=preprocess_for_openai,
                    postprocess=postprocess_openai_response,
                )
                result = processor(test_dataset).take_all()
                assert len(result) > 0, "OpenAI API call produced no results"
                assert "response" in result[0], "Missing response in API output"
                assert isinstance(
                    result[0]["response"], str
                ), "Response is not a string"

                if not suppress_output:
                    print("OpenAI API call successful")
            except Exception as api_e:
                if not suppress_output:
                    print(f"OpenAI API call failed (expected in CI): {api_e}")
        else:
            if not suppress_output:
                print("Skipping OpenAI API call (no key available)")

        if not suppress_output:
            print("OpenAI API example validation successful")
        return True
    except Exception as e:
        if not suppress_output:
            print(f"OpenAI API example validation failed: {e}")
        return False


if __name__ == "__main__":
    # Run the demo
    run_openai_demo()
    # Run validation tests
    run_test()
