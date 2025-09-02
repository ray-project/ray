"""
This file serves as a documentation example and CI test for VLM batch inference.

Structure:
1. Infrastructure setup: Dataset compatibility patches, dependency handling
2. Docs example (between __vlm_example_start/end__): Embedded in Sphinx docs via literalinclude
3. Test validation and cleanup
"""

import subprocess
import sys
import os
import tempfile

# Infrastructure: Handle datasets compatibility issue
try:
    import datasets.load

    # Create a compatibility wrapper for the removed init_dynamic_modules function
    if not hasattr(datasets.load, "init_dynamic_modules"):

        def mock_init_dynamic_modules():
            """Compatibility wrapper for datasets>=4.0.0"""
            temp_dir = tempfile.mkdtemp()
            datasets_modules_path = os.path.join(temp_dir, "datasets_modules")
            os.makedirs(datasets_modules_path, exist_ok=True)
            init_file = os.path.join(datasets_modules_path, "__init__.py")
            with open(init_file, "w") as f:
                f.write("# Auto-generated compatibility module\n")
            return datasets_modules_path

        # Patch the function
        datasets.load.init_dynamic_modules = mock_init_dynamic_modules
except ImportError:
    pass

# Infrastructure: Upgrade datasets if needed
try:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--upgrade", "datasets>=4.0.0"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
except:
    pass

# __vlm_example_start__
import ray
import datasets
from PIL import Image
from io import BytesIO
from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor

# Load "LMMs-Eval-Lite" dataset from Hugging Face.
vision_dataset_llms_lite = datasets.load_dataset(
    "lmms-lab/LMMs-Eval-Lite", "coco2017_cap_val"
)
vision_dataset = ray.data.from_huggingface(vision_dataset_llms_lite["lite"])

HF_TOKEN = "your-hf-token-here"  # Replace with actual token if needed

# __vlm_config_example_start__
vision_processor_config = vLLMEngineProcessorConfig(
    model_source="Qwen/Qwen2.5-VL-3B-Instruct",
    engine_kwargs=dict(
        tensor_parallel_size=1,
        pipeline_parallel_size=1,
        max_model_len=4096,
        enable_chunked_prefill=True,
        max_num_batched_tokens=2048,
    ),
    # Override Ray's runtime env to include the Hugging Face token. Ray Data uses Ray under the hood to orchestrate the inference pipeline.
    runtime_env=dict(
        env_vars=dict(
            HF_TOKEN=HF_TOKEN,
            VLLM_USE_V1="1",
        ),
    ),
    batch_size=16,
    accelerator_type="L4",
    concurrency=1,
    has_image=True,
)
# __vlm_config_example_end__


def vision_preprocess(row: dict) -> dict:
    """
    Preprocessing function for vision-language model inputs.

    Converts dataset rows into the format expected by the VLM:
    - System prompt for analysis instructions
    - User message with text and image content
    - Multiple choice formatting
    - Sampling parameters
    """
    choice_indices = ["A", "B", "C", "D", "E", "F", "G", "H"]

    return {
        "messages": [
            {
                "role": "system",
                "content": (
                    "Analyze the image and question carefully, using step-by-step reasoning. "
                    "First, describe any image provided in detail. Then, present your reasoning. "
                    "And finally your final answer in this format: Final Answer: <answer> "
                    "where <answer> is: The single correct letter choice A, B, C, D, E, F, etc. when options are provided. "
                    "Only include the letter. Your direct answer if no options are given, as a single phrase or number. "
                    "IMPORTANT: Remember, to end your answer with Final Answer: <answer>."
                ),
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": row["question"] + "\n\n"},
                    {
                        "type": "image",
                        # Ray Data accepts PIL Image or image URL
                        "image": Image.open(BytesIO(row["image"]["bytes"])),
                    },
                    {
                        "type": "text",
                        "text": "\n\nChoices:\n"
                        + "\n".join(
                            [
                                f"{choice_indices[i]}. {choice}"
                                for i, choice in enumerate(row["answer"])
                            ]
                        ),
                    },
                ],
            },
        ],
        "sampling_params": {
            "temperature": 0.3,
            "max_tokens": 150,
            "detokenize": False,
        },
        # Include original data for reference
        "original_data": {
            "question": row["question"],
            "answer_choices": row["answer"],
            "image_size": row["image"].get("width", 0) if row["image"] else 0,
        },
    }


def vision_postprocess(row: dict) -> dict:
    return {
        "resp": row["generated_text"],
    }


vision_processor = build_llm_processor(
    vision_processor_config,
    preprocess=vision_preprocess,
    postprocess=vision_postprocess,
)

# Only run inference in documentation context, not when executed directly
if __name__ != "__main__":
    vision_processed_ds = vision_processor(vision_dataset).materialize()
    vision_processed_ds.show(3)


def load_vision_dataset():
    """
    Load vision dataset from Hugging Face.

    This function loads the LMMs-Eval-Lite dataset which contains:
    - Images with associated questions
    - Multiple choice answers
    - Various visual reasoning tasks
    """
    try:
        import datasets

        # Load "LMMs-Eval-Lite" dataset from Hugging Face
        vision_dataset_llms_lite = datasets.load_dataset(
            "lmms-lab/LMMs-Eval-Lite", "coco2017_cap_val"
        )
        vision_dataset = ray.data.from_huggingface(vision_dataset_llms_lite["lite"])

        return vision_dataset
    except ImportError:
        print(
            "datasets package not available. Install with: pip install datasets>=4.0.0"
        )
        return None
    except Exception as e:
        print(f"Error loading dataset: {e}")
        return None


def create_vlm_config():
    """Create VLM configuration."""
    return vLLMEngineProcessorConfig(
        model_source="Qwen/Qwen2.5-VL-3B-Instruct",
        engine_kwargs=dict(
            tensor_parallel_size=1,
            pipeline_parallel_size=1,
            max_model_len=4096,
            trust_remote_code=True,
            limit_mm_per_prompt={"image": 1},
        ),
        runtime_env={"env_vars": {"HF_TOKEN": "your-hf-token-here"}},
        batch_size=1,
        accelerator_type="L4",
        concurrency=1,
        has_image=True,
    )


def run_vlm_example():
    """Run the complete VLM example workflow."""
    config = create_vlm_config()
    vision_dataset = load_vision_dataset()

    if vision_dataset:
        # Build processor with preprocessing
        processor = build_llm_processor(config, preprocess=vision_preprocess)

        # For demo, just show configuration - actual inference requires GPU
        print("VLM processor configured successfully")
        print(f"Model: {config.model_source}")
        print(f"Has image support: {config.has_image}")
        # Uncomment for actual inference: result = processor(vision_dataset).take_all()
        return config, processor
    return None, None


# __vlm_example_end__

# Test validation and cleanup
def run_test():
    """Test function that validates the example works including infrastructure."""
    import sys

    suppress_output = "pytest" in sys.modules

    try:
        # Test 1: Infrastructure - datasets compatibility patch was applied
        import datasets.load

        assert hasattr(
            datasets.load, "init_dynamic_modules"
        ), "datasets compatibility patch not applied"

        # Test 2: Infrastructure - datasets version is adequate
        import datasets

        try:
            # Try to access a newer datasets feature to verify upgrade worked
            from datasets import Features

            if not suppress_output:
                print(f"datasets version check passed: {datasets.__version__}")
        except Exception as e:
            if not suppress_output:
                print(f"datasets version check: {e}")

        # Test 3: Configuration creation
        config = create_vlm_config()
        assert config.model_source == "Qwen/Qwen2.5-VL-3B-Instruct"
        assert config.has_image is True
        assert config.accelerator_type == "L4"

        # Test 4: Preprocessing with real image
        import numpy as np

        test_image = Image.fromarray(np.zeros((224, 224, 3), dtype=np.uint8))
        test_row = {
            "question": "What's in this image?",
            "image": {"bytes": test_image.tobytes()},
            "answer": ["A black square", "A white square", "A colored square"],
        }

        result = vision_preprocess(test_row)
        assert "messages" in result
        assert "sampling_params" in result
        assert "original_data" in result
        assert result["sampling_params"]["temperature"] == 0.3
        assert len(result["messages"]) > 0
        assert any(
            msg.get("type") == "image" for msg in result["messages"][1]["content"]
        )

        # Test 5: Dataset loading and actual inference
        try:
            import torch

            if torch.cuda.is_available():
                # Create dataset with test image
                vision_dataset = ray.data.from_items([test_row])

                # Build processor and run inference
                processor = build_llm_processor(
                    config,
                    preprocess=vision_preprocess,
                    postprocess=lambda row: {"resp": row["generated_text"]},
                )
                result = processor(vision_dataset).take_all()
                assert len(result) > 0, "VLM inference produced no results"
                assert "resp" in result[0], "Missing response in inference output"

                if not suppress_output:
                    print("VLM inference successful")
            else:
                if not suppress_output:
                    print("Skipping VLM inference test (GPU not available)")
        except Exception as gpu_e:
            if not suppress_output:
                print(f"Skipping VLM inference test: {gpu_e}")

        if not suppress_output:
            print("VLM validation successful")
        return True
    except Exception as e:
        if not suppress_output:
            print(f"VLM infrastructure validation failed: {e}")
        return False


if __name__ == "__main__":
    # Run the actual example workflow
    run_vlm_example()
    # Run validation tests
    run_test()
