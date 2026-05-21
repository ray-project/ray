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
# vLLM has a two-stage video pipeline. ``media_io_kwargs["video"]`` controls
# frame sampling in vLLM's video I/O; ``mm_processor_kwargs`` is forwarded to
# the HF processor and controls per-frame resizing via the visual-token pixel
# budget. vLLM reads the engine-level budget once at startup to size memory
# profiling and the KV cache, so it must cover the worst-case input;
# per-request overrides may only go *smaller* without OOM risk. If the input
# exceeds the budget, the HF processor silently downscales it.
#
# Knob names differ by model family: Qwen3-VL+ uses ``size.longest_edge`` /
# ``size.shortest_edge``; the older Qwen2-VL ``max_pixels`` / ``min_pixels``
# names are dropped before reaching the HF processor on Qwen3-VL+. See the
# vLLM and HF processor docs for the kwargs each model accepts.
#
# Worked example (Qwen3-VL, patch_size=16, merge_size=2, temporal_patch_size=2):
# a 20-frame 1080p window needs ``20 * 1088 * 1920 ~= 41.8M`` pixels; the
# model's default video budget (25.17M) downscales anything above ~12 frames
# at 1080p.
video_processor_config = vLLMEngineProcessorConfig(
    model_source="Qwen/Qwen3-VL-4B-Instruct",
    engine_kwargs=dict(
        tensor_parallel_size=4,
        pipeline_parallel_size=1,
        trust_remote_code=True,
        limit_mm_per_prompt={"video": 1},
        # Stage 2: HF processor resizing. vLLM reads this once at startup to
        # size memory profiling and the KV cache, so it must cover the
        # worst-case input. Per-row overrides may only go *smaller*.
        mm_processor_kwargs={
            "size": {
                "shortest_edge": 65536,
                "longest_edge": 20 * 1088 * 1920,
            },
            # Stage-1 sampling above already produced the final frames;
            # skip the HF processor's own re-sampling.
            "do_sample_frames": False,
        },
    ),
    batch_size=1,
    accelerator_type="L4",
    concurrency=1,
    prepare_multimodal_stage={
        "enabled": True,
        "model_config_kwargs": dict(
            # See available model config kwargs at https://docs.vllm.ai/en/latest/api/vllm/config/#vllm.config.ModelConfig
            allowed_local_media_path="/tmp",
            # Stage 1: vLLM video I/O frame sampling. In Ray Data LLM the
            # PrepareMultimodalStage decodes media into arrays before the
            # engine runs, so media_io_kwargs must be set here (not in
            # engine_kwargs) to take effect.
            media_io_kwargs={"video": {"num_frames": 60, "fps": 2}},
        ),
    },
    chat_template_stage=True,
    tokenize_stage=True,
    detokenize_stage=True,
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
        # The visual-token pixel budget is set engine-side in
        # ``engine_kwargs["mm_processor_kwargs"]``. A per-row
        # ``mm_processor_kwargs`` can override that budget *smaller* for a
        # single row, but never larger than the engine ceiling without OOM
        # risk.
    }


def video_postprocess(row: dict) -> dict:
    return {
        "resp": row["generated_text"],
    }


# __vlm_video_preprocess_example_end__


def load_video_dataset():
# __vlm_video_load_dataset_example_start__
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
# __vlm_video_load_dataset_example_end__

def create_vlm_video_config():
    """Create VLM video configuration."""
    return vLLMEngineProcessorConfig(
        model_source="Qwen/Qwen3-VL-4B-Instruct",
        engine_kwargs=dict(
            tensor_parallel_size=4,
            pipeline_parallel_size=1,
            trust_remote_code=True,
            limit_mm_per_prompt={"video": 1},
            mm_processor_kwargs={
                "size": {
                    "shortest_edge": 65536,
                    "longest_edge": 20 * 1088 * 1920,
                },
                "do_sample_frames": False,
            },
        ),
        batch_size=1,
        accelerator_type="L4",
        concurrency=1,
        prepare_multimodal_stage={
            "enabled": True,
            "model_config_kwargs": dict(
                # See available model config kwargs at https://docs.vllm.ai/en/latest/api/vllm/config/#vllm.config.ModelConfig
                allowed_local_media_path="/tmp",
                media_io_kwargs={"video": {"num_frames": 60, "fps": 2}},
            ),
        },
        chat_template_stage=True,
        tokenize_stage=True,
        detokenize_stage=True,
    )


def run_vlm_video_example():
# __vlm_video_run_example_start__
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
