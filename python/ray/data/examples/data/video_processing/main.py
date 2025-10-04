from __future__ import annotations

import asyncio
import json
import os
import tempfile
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


class DecodeFramesUDF:
    def __init__(self, sampling=None, preprocess=None):
        self.processor = VideoProcessor(
            sampling=sampling or {"num_frames": 4},
            output_format="pil",
            preprocess=preprocess or {"resize": {"size": [384, 384]}, "convert": "RGB"},
        )

    def __call__(self, batch: Any):
        if isinstance(batch, pa.Table):
            records = batch.to_pylist()
        else:
            records = list(batch)
        sources = [str(r["video_url"]) for r in records]
        prompts = [r.get("prompt", DEFAULT_PROMPT) for r in records]
        results = asyncio.run(self.processor.process(sources))
        out: List[Dict[str, Any]] = []
        for row, prompt_text, res in zip(records, prompts, results):
            frames = res.get("frames", [])
            frames_jpeg: List[bytes] = []
            for f in frames:
                buf = BytesIO()
                f.save(buf, format="JPEG", quality=90)
                frames_jpeg.append(buf.getvalue())
            meta_obj = res.get("meta")
            out.append(
                {
                    "video_url": str(row.get("video_url")),
                    "prompt": str(prompt_text),
                    "frames_jpeg": frames_jpeg,
                    "meta": json.dumps(meta_obj) if meta_obj is not None else None,
                }
            )
        return pa.Table.from_pylist(out)


class VLMInferenceUDF:
    def __init__(
        self,
        model_path: str,
        temperature: float = 0.1,
        top_p: float = 0.001,
        max_tokens: int = 512,
    ):
        self.model_path = model_path
        self.temperature = temperature
        self.top_p = top_p
        self.max_tokens = max_tokens
        self.llm = None
        self.processor = None
        self.process_vision_info = qwen_vl_utils.process_vision_info
        self._initialized = False

    def _ensure_initialized(self):
        if self._initialized:
            return
        self.processor = transformers.AutoProcessor.from_pretrained(
            self.model_path, trust_remote_code=True
        )
        self.llm = vllm.LLM(
            model=self.model_path,
            limit_mm_per_prompt={"image": 10},
            trust_remote_code=True,
            enforce_eager=True,
        )
        self.sampling_params = vllm.SamplingParams(
            temperature=self.temperature,
            top_p=self.top_p,
            max_tokens=self.max_tokens,
        )
        self._initialized = True

    def __call__(self, batch: Any):
        self._ensure_initialized()
        if isinstance(batch, pa.Table):
            records = batch.to_pylist()
        else:
            records = list(batch)
        outputs: List[Dict[str, Any]] = []
        with tempfile.TemporaryDirectory() as tmp_dir:
            for row in records:
                frames_jpeg = row.get("frames_jpeg") or []
                image_paths: List[str] = []
                for idx, b in enumerate(frames_jpeg):
                    p = os.path.join(tmp_dir, f"frame_{idx}.jpg")
                    with open(p, "wb") as f:
                        f.write(b)
                    image_paths.append(p)
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
                prompt = self.processor.apply_chat_template(
                    messages, tokenize=False, add_generation_prompt=True
                )
                image_inputs, _ = self.process_vision_info(messages)
                generations = self.llm.generate(
                    [
                        {"prompt": prompt, "multi_modal_data": {"image": image_inputs}},
                    ],
                    sampling_params=self.sampling_params,
                )
                outputs.append(
                    {
                        "video_url": str(row.get("video_url")),
                        "summary": str(generations[0].outputs[0].text),
                        "meta": row.get("meta"),
                    }
                )
        return pa.Table.from_pylist(outputs)

    def close(self):
        if self.llm and hasattr(self.llm, "close"):
            try:
                self.llm.close()
            except Exception:
                pass


def run_dataset_pipeline(
    model_path: str,
) -> None:
    if not ray.is_initialized():
        ray.init(include_dashboard=False)

    ds = ray.data.from_items(
        [
            {
                "video_url": EXAMPLE_VIDEO_PATH,
                "prompt": "Summarize the content of this video",
            },
            {"video_url": EXAMPLE_VIDEO_PATH, "prompt": "List notable objects."},
            {"video_url": EXAMPLE_VIDEO_PATH, "prompt": "Describe the scene."},
        ]
    )

    decode_udf = DecodeFramesUDF()
    infer_udf = VLMInferenceUDF(model_path)

    try:
        ds_decoded = ds.map_batches(
            decode_udf,
            batch_format="pyarrow",
            batch_size=1,
        )

        ds_inferred = ds_decoded.map_batches(
            infer_udf,
            batch_format="pyarrow",
            batch_size=1,
            num_gpus=1,
        )

        for row in ds_inferred.take_all():
            print("\n=== Dataset result ===")
            print(f"video:   {row['video_url']}")
            print(f"summary: {row['summary']}")
    finally:
        infer_udf.close()


def main() -> None:
    run_dataset_pipeline(EXAMPLE_MODEL_PATH)


if __name__ == "__main__":
    main()
