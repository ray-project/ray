from __future__ import annotations

import asyncio
import json
import os
import tempfile
from functools import partial
from io import BytesIO
from typing import Any, Dict, List

import pyarrow as pa
import qwen_vl_utils
import transformers
import vllm

import ray
import ray.data
from ray.data.examples.data.video_processing.video_processor import VideoProcessor

EXAMPLE_VIDEO_PATH = (
    "https://videos.pexels.com/video-files/30527638/13076846_2160_3240_30fps.mp4"
)
EXAMPLE_MODEL_PATH = "/vllm-workspace/tmp/vlm"

DEFAULT_PROMPT = "Summarize the content of this video"


async def process_video_with_vlm(video_path: str, model_path: str) -> None:
    processor = VideoProcessor(
        sampling={"num_frames": 4},
        output_format="pil",
        preprocess={"resize": {"size": [384, 384]}, "convert": "RGB"},
    )
    frames_and_meta = await processor.process([video_path])
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

        hf_processor = transformers.AutoProcessor.from_pretrained(
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
        process_vision_info = qwen_vl_utils.process_vision_info

        prompt = hf_processor.apply_chat_template(
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
        if hasattr(llm, "close"):
            try:
                llm.close()
            except Exception:
                pass


def decode_frames_stage(batch: Any):
    if isinstance(batch, pa.Table):
        records = batch.to_pylist()
    else:
        records = list(batch)
    sources = [str(row["video_url"]) for row in records]
    prompts = [row.get("prompt", DEFAULT_PROMPT) for row in records]
    processor = VideoProcessor(
        sampling={"num_frames": 4},
        output_format="pil",
        preprocess={"resize": {"size": [384, 384]}, "convert": "RGB"},
    )
    results = asyncio.run(processor.process(sources))
    outputs: List[Dict[str, Any]] = []
    for row, prompt_text, res in zip(records, prompts, results):
        frames = res.get("frames", [])
        frames_jpeg: List[bytes] = []
        for f in frames:
            buf = BytesIO()
            f.save(buf, format="JPEG", quality=90)
            frames_jpeg.append(buf.getvalue())
        meta_obj = res.get("meta")
        outputs.append(
            {
                "video_url": str(row.get("video_url")),
                "prompt": str(prompt_text),
                "frames_jpeg": frames_jpeg,
                "meta": json.dumps(meta_obj) if meta_obj is not None else None,
            }
        )
    return pa.Table.from_pylist(outputs)


def vlm_infer_stage(batch: Any, *, model_path: str):
    if isinstance(batch, pa.Table):
        records = batch.to_pylist()
    else:
        records = list(batch)
    hf_processor = transformers.AutoProcessor.from_pretrained(
        model_path, trust_remote_code=True
    )
    llm = vllm.LLM(
        model=model_path,
        limit_mm_per_prompt={"image": 10},
        trust_remote_code=True,
        enforce_eager=True,
    )
    sampling_params = vllm.SamplingParams(temperature=0.1, top_p=0.001, max_tokens=512)
    process_vision_info = qwen_vl_utils.process_vision_info
    outputs: List[Dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as tmp_dir:
        for row in records:
            frames_jpeg = row.get("frames_jpeg") or []
            image_paths: List[str] = []
            for idx, b in enumerate(frames_jpeg):
                path = os.path.join(tmp_dir, f"frame_{idx}.jpg")
                with open(path, "wb") as f:
                    f.write(b)
                image_paths.append(path)
            messages = [
                {"role": "system", "content": "You are a helpful assistant."},
                {
                    "role": "user",
                    "content": [
                        *[{"type": "image", "image": p} for p in image_paths],
                        {"type": "text", "text": row.get("prompt", DEFAULT_PROMPT)},
                    ],
                },
            ]
            prompt = hf_processor.apply_chat_template(
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
            outputs.append(
                {
                    "video_url": str(row.get("video_url")),
                    "summary": str(generations[0].outputs[0].text),
                    "meta": row.get("meta"),
                }
            )

    return pa.Table.from_pylist(outputs)


def run_dataset_pipeline(
    model_path: str,
    *,
    batch_size: int = 4,
) -> None:
    if not ray.is_initialized():
        ray.init(include_dashboard=False)

    ds = ray.data.from_items(
        [{"video_url": EXAMPLE_VIDEO_PATH, "prompt": DEFAULT_PROMPT}]
    )

    ds_decoded = ds.map_batches(
        decode_frames_stage,
        batch_format="pyarrow",
        batch_size=batch_size,
    )

    ds_inferred = ds_decoded.map_batches(
        partial(vlm_infer_stage, model_path=model_path),
        batch_format="pyarrow",
        batch_size=batch_size,
        num_gpus=1,
    )

    for row in ds_inferred.take_all():
        print("\n=== Dataset result ===")
        print(f"video:   {row['video_url']}")
        print(f"summary: {row['summary']}")


def main() -> None:
    asyncio.run(process_video_with_vlm(EXAMPLE_VIDEO_PATH, EXAMPLE_MODEL_PATH))

    run_dataset_pipeline(EXAMPLE_MODEL_PATH, batch_size=4)


if __name__ == "__main__":
    main()
