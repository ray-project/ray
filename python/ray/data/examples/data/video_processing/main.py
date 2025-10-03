"""Example script showing how to combine ``VideoProcessor`` with VLM inference and a Ray Data dataset pipeline.

This file intentionally keeps a minimal, readable flow for demonstration:
- Imports are at module top (no in-function imports).
- A single local video path is wrapped into a Ray Data dataset and passed to
    ``map_batches`` for decoding and VLM generation.
- In production, you would typically consume videos from Kafka as a streaming source.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from functools import partial
from typing import Any, Dict, List, MutableMapping, Sequence

# Optional dependencies for VLM inference and Ray Data. These are assumed to be
# installed in environments where this example is used.
import qwen_vl_utils  # type: ignore[import-not-found]
import transformers  # type: ignore[import-not-found]
import vllm  # type: ignore[import-not-found]
import ray
import ray.data
from ray.data import ActorPoolStrategy

from ray.data.examples.data.video_processing.video_processor import VideoProcessor

EXAMPLE_VIDEO_PATH = (
    "https://videos.pexels.com/video-files/30527638/13076846_2160_3240_30fps.mp4"
)
EXAMPLE_MODEL_PATH = "/vllm-workspace/tmp/vlm"

DEFAULT_PROMPT = "Summarize the content of this video"
_WORKER_VIDEO_PROCESSOR: VideoProcessor | None = None
_VLM_STATE: Dict[str, Dict[str, Any]] = {}


def _get_video_processor() -> VideoProcessor:
    global _WORKER_VIDEO_PROCESSOR
    if _WORKER_VIDEO_PROCESSOR is None:
        _WORKER_VIDEO_PROCESSOR = VideoProcessor(
            sampling={"num_frames": 4},
            output_format="pil",
            preprocess={"resize": {"size": [384, 384]}, "convert": "RGB"},
        )
    return _WORKER_VIDEO_PROCESSOR


async def _process_sources_async(sources: Sequence[str]) -> List[Dict[str, Any]]:
    processor = _get_video_processor()
    return await processor.process(list(sources))


def _ensure_vlm_components(model_path: str) -> Dict[str, Any]:
    state = _VLM_STATE.get(model_path)
    if state is None:
        processor = transformers.AutoProcessor.from_pretrained(
            model_path, trust_remote_code=True
        )
        llm = vllm.LLM(
            model=model_path,
            limit_mm_per_prompt={"image": 10},
            trust_remote_code=True,
            enforce_eager=True,
        )
        sampling_params = vllm.SamplingParams(
            temperature=0.1, top_p=0.001, max_tokens=512
        )
        state = {
            "processor": processor,
            "llm": llm,
            "sampling_params": sampling_params,
            "process_vision_info": qwen_vl_utils.process_vision_info,
        }
        _VLM_STATE[model_path] = state
    return state


async def process_video_with_vlm(video_path: str, model_path: str) -> None:
    components = _ensure_vlm_components(model_path)
    frames_and_meta = await _process_sources_async([video_path])
    frames = frames_and_meta[0]["frames"]
    print(f"Extracted {len(frames)} frames from {video_path}")

    with tempfile.TemporaryDirectory() as tmp_dir:
        image_paths: List[str] = []
        for idx, frame in enumerate(frames):
            temp_path = os.path.join(tmp_dir, f"frame_{idx}.jpg")
            frame.save(temp_path)
            image_paths.append(temp_path)

        messages = [
            {"role": "system", "content": "You are a helpful assistant."},
            {
                "role": "user",
                "content": [
                    *[{"type": "image", "image": path} for path in image_paths],
                    {"type": "text", "text": DEFAULT_PROMPT},
                ],
            },
        ]

        processor = components["processor"]
        llm = components["llm"]
        sampling_params = components["sampling_params"]
        process_vision_info = components["process_vision_info"]

        prompt = processor.apply_chat_template(
            messages, tokenize=False, add_generation_prompt=True
        )
        image_inputs, _ = process_vision_info(messages)

        generations = llm.generate(
            [
                {
                    "prompt": prompt,
                    "multi_modal_data": {"image": image_inputs},
                }
            ],
            sampling_params=sampling_params,
        )
        text = generations[0].outputs[0].text
        print("Generated result:", text)


def decode_videos(batch: List[MutableMapping[str, Any]]) -> List[Dict[str, Any]]:
    sources = [str(row["video_url"]) for row in batch]
    prompts = [row.get("prompt", DEFAULT_PROMPT) for row in batch]
    results = asyncio.run(_process_sources_async(sources))

    enriched: List[Dict[str, Any]] = []
    for row, prompt_text, res in zip(batch, prompts, results):
        enriched.append(
            {
                "video_url": row["video_url"],
                "prompt": prompt_text,
                "frames": res["frames"],
                "meta": res["meta"],
            }
        )
    return enriched


def vlm_generate(
    batch: List[MutableMapping[str, Any]], *, model_path: str
) -> List[Dict[str, Any]]:
    components = _ensure_vlm_components(model_path)
    processor = components["processor"]
    llm = components["llm"]
    sampling_params = components["sampling_params"]
    process_vision_info = components["process_vision_info"]

    outputs: List[Dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as tmp_dir:
        for row in batch:
            frames = row.get("frames", [])
            prompt_text = row.get("prompt", DEFAULT_PROMPT)

            image_paths: List[str] = []
            for idx, frame in enumerate(frames):
                temp_path = os.path.join(tmp_dir, f"frame_{idx}.jpg")
                frame.save(temp_path)
                image_paths.append(temp_path)

            messages = [
                {"role": "system", "content": "You are a helpful assistant."},
                {
                    "role": "user",
                    "content": [
                        *[{"type": "image", "image": path} for path in image_paths],
                        {"type": "text", "text": prompt_text},
                    ],
                },
            ]

            prompt = processor.apply_chat_template(
                messages, tokenize=False, add_generation_prompt=True
            )
            image_inputs, _ = process_vision_info(messages)
            generations = llm.generate(
                [
                    {
                        "prompt": prompt,
                        "multi_modal_data": {"image": image_inputs},
                    }
                ],
                sampling_params=sampling_params,
            )
            summary = generations[0].outputs[0].text
            outputs.append(
                {
                    "video_url": row.get("video_url"),
                    "summary": summary,
                    "meta": row.get("meta"),
                }
            )
    return outputs


def run_dataset_pipeline(
    model_path: str,
    *,
    batch_size: int = 4,
    actor_workers: int = 1,
) -> None:
    """Create a Ray Data dataset from a single local video path and run VLM.

    This mirrors the streaming pipeline but uses a simple in-memory dataset:
    [{"video_url": EXAMPLE_VIDEO_PATH, "prompt": DEFAULT_PROMPT}].
    """
    ds = ray.data.from_items(
        [{"video_url": EXAMPLE_VIDEO_PATH, "prompt": DEFAULT_PROMPT}]
    )

    decoded = ds.map_batches(
        decode_videos,
        batch_format="native",
        batch_size=batch_size,
        compute=ActorPoolStrategy(size=actor_workers),
    )
    generated = decoded.map_batches(
        partial(vlm_generate, model_path=model_path),
        batch_format="native",
        batch_size=1,
        compute=ActorPoolStrategy(size=actor_workers),
    )

    for row in generated.take_all():
        print(f"[dataset] {row['video_url']}: {row['summary']}")


def main() -> None:
    asyncio.run(process_video_with_vlm(EXAMPLE_VIDEO_PATH, EXAMPLE_MODEL_PATH))

    run_dataset_pipeline(EXAMPLE_MODEL_PATH, batch_size=4, actor_workers=1)


if __name__ == "__main__":
    main()
