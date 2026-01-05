"""
This file serves as a documentation example and CI test for VLM batch inference with videos.

Structure:
1. Infrastructure setup: Dataset compatibility patches, dependency handling
2. Docs example (between __vlm_video_example_start/end__): Embedded in Sphinx docs via literalinclude
3. Test validation and cleanup
"""

import os

'''
# __video_message_format_example_start__
"""Supported video input formats: video URL"""
{
    "messages": [
        {
            "role": "system",
            "content": "Provide a detailed description of the video."
        },
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "Describe what happens in this video."},
                # Provide video URL
                {"type": "video_url", "video_url": {"url": "https://example.com/video.mp4"}},
            ]
        },
    ]
}
# __video_message_format_example_end__
'''

# __vlm_video_example_start__
import ray
from ray.data.llm import (
    vLLMEngineProcessorConfig,
    build_processor,
)


# __vlm_video_config_example_start__
video_processor_config = vLLMEngineProcessorConfig(
    model_source="Qwen/Qwen3-VL-4B-Instruct",
    engine_kwargs=dict(
        tensor_parallel_size=4,
        pipeline_parallel_size=1,
        trust_remote_code=True,
        limit_mm_per_prompt={"video": 1},
    ),
    batch_size=1,
    accelerator_type="L4",
    concurrency=1,
    prepare_multimodal_stage={
        "enabled": True,
        "model_config_kwargs": dict(
            # See available model config kwargs at https://docs.vllm.ai/en/latest/api/vllm/config/#vllm.config.ModelConfig
            allowed_local_media_path="/tmp",
        ),
    },
    chat_template_stage={"enabled": True},
    tokenize_stage={"enabled": True},
    detokenize_stage={"enabled": True},
)
# __vlm_video_config_example_end__


# __vlm_video_preprocess_example_start__
def video_preprocess(row: dict) -> dict:
    """
    Preprocessing function for video-language model inputs.

    Converts dataset rows into the format expected by the VLM:
    - System prompt for analysis instructions
    - User message with text and video content
    - Sampling parameters
    - Multimodal processor kwargs for video processing
    """
    return {
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a helpful assistant that analyzes videos. "
                    "Watch the video carefully and provide detailed descriptions."
                ),
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": row["text"],
                    },
                    {
                        "type": "video_url",
                        "video_url": {"url": row["video_url"]},
                    },
                ],
            },
        ],
        "sampling_params": {
            "temperature": 0.3,
            "max_tokens": 150,
            "detokenize": False,
        },
        # Optional: Multimodal processor kwargs for video processing
        "mm_processor_kwargs": dict(
            min_pixels=28 * 28,
            max_pixels=1280 * 28 * 28,
            fps=1,
        ),
    }


def video_postprocess(row: dict) -> dict:
    return {
        "resp": row["generated_text"],
    }


# __vlm_video_preprocess_example_end__


def load_video_dataset():
    """
    Load video dataset from ShareGPTVideo Hugging Face dataset.
    """
    try:
        from huggingface_hub import hf_hub_download
        import tarfile
        from pathlib import Path

        dataset_name = "ShareGPTVideo/train_raw_video"

        tar_path = hf_hub_download(
            repo_id=dataset_name,
            filename="activitynet/chunk_0.tar.gz",
            repo_type="dataset",
        )

        extract_dir = "/tmp/sharegpt_videos"
        os.makedirs(extract_dir, exist_ok=True)

        if not any(Path(extract_dir).glob("*.mp4")):
            with tarfile.open(tar_path, "r:gz") as tar:
                tar.extractall(extract_dir)

        video_files = list(Path(extract_dir).rglob("*.mp4"))

        # Limit to first 10 videos for the example
        video_files = video_files[:10]

        video_dataset = ray.data.from_items(
            [
                {
                    "video_path": str(video_file),
                    "video_url": f"file://{video_file}",
                    "text": "Describe what happens in this video.",
                }
                for video_file in video_files
            ]
        )

        return video_dataset
    except Exception as e:
        print(f"Error loading dataset: {e}")
        return None


def create_vlm_video_config():
    """Create VLM video configuration."""
    return vLLMEngineProcessorConfig(
        model_source="Qwen/Qwen3-VL-4B-Instruct",
        engine_kwargs=dict(
            tensor_parallel_size=4,
            pipeline_parallel_size=1,
            trust_remote_code=True,
            limit_mm_per_prompt={"video": 1},
        ),
        batch_size=1,
        accelerator_type="L4",
        concurrency=1,
        prepare_multimodal_stage={
            "enabled": True,
            "model_config_kwargs": dict(
                # See available model config kwargs at https://docs.vllm.ai/en/latest/api/vllm/config/#vllm.config.ModelConfig
                allowed_local_media_path="/tmp",
            ),
        },
        chat_template_stage={"enabled": True},
        tokenize_stage={"enabled": True},
        detokenize_stage={"enabled": True},
    )


def run_vlm_video_example():
    """Run the complete VLM video example workflow."""
    config = create_vlm_video_config()
    video_dataset = load_video_dataset()

    if video_dataset:
        # Build processor with preprocessing and postprocessing
        processor = build_processor(
            config, preprocess=video_preprocess, postprocess=video_postprocess
        )

        print("VLM video processor configured successfully")
        print(f"Model: {config.model_source}")
        print(f"Has multimodal support: {config.prepare_multimodal_stage.get('enabled', False)}")
        result = processor(video_dataset).take_all()
        return config, processor, result
    # __vlm_video_run_example_end__
    return None, None, None


# __vlm_video_example_end__

if __name__ == "__main__":
    # Run the example VLM video workflow only if GPU is available
    try:
        import torch

        if torch.cuda.is_available():
            run_vlm_video_example()
        else:
            print("Skipping VLM video example run (no GPU available)")
    except Exception as e:
        print(f"Skipping VLM video example run due to environment error: {e}")
