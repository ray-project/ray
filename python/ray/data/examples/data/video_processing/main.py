from __future__ import annotations

import asyncio
import json
import os
import tempfile
from io import BytesIO
from typing import Any, Callable, Dict, List, Optional

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


class VLLMProcessorConfig:
    def __init__(
        self,
        model_path: str,
        temperature: float = 0.1,
        top_p: float = 0.001,
        max_tokens: int = 512,
        max_images: int = 10,
    ):
        self.model_path = model_path
        self.temperature = temperature
        self.top_p = top_p
        self.max_tokens = max_tokens
        self.max_images = max_images


def build_llm_stage(
    config: VLLMProcessorConfig,
    preprocess: Optional[Callable[[List[Dict[str, Any]]], Any]] = None,
    postprocess: Optional[Callable[[List[Dict[str, Any]]], Any]] = None,
):
    state: Dict[str, Any] = {
        "llm": None,
        "processor": None,
        "sampling": None,
    }
    process_vision_info = qwen_vl_utils.process_vision_info

    def _lazy_init():
        if state["llm"] is not None:
            return
        state["processor"] = transformers.AutoProcessor.from_pretrained(
            config.model_path, trust_remote_code=True
        )
        state["llm"] = vllm.LLM(
            model=config.model_path,
            limit_mm_per_prompt={"image": config.max_images},
            trust_remote_code=True,
            enforce_eager=True,
        )
        state["sampling"] = vllm.SamplingParams(
            temperature=config.temperature,
            top_p=config.top_p,
            max_tokens=config.max_tokens,
        )

    def _batch_fn(batch: Any):
        _lazy_init()
        if isinstance(batch, pa.Table):
            records = batch.to_pylist()
        else:
            records = list(batch)

        proc_inputs = preprocess(records) if callable(preprocess) else records
        outputs: List[Dict[str, Any]] = []
        with tempfile.TemporaryDirectory() as tmp_dir:

            requests = []
            meta_refs = []
            for row in proc_inputs:
                frames_jpeg = row.get("frames_jpeg") or []
                if not frames_jpeg:
                    raise RuntimeError(
                        f"No frames decoded for video: {row.get('video_url')}"
                    )
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
                prompt = state["processor"].apply_chat_template(
                    messages, tokenize=False, add_generation_prompt=True
                )
                image_inputs, _ = process_vision_info(messages)
                if image_inputs is None:
                    raise RuntimeError("Failed to prepare image inputs.")
                requests.append(
                    {"prompt": prompt, "multi_modal_data": {"image": image_inputs}}
                )
                meta_refs.append((row.get("video_url"), row.get("meta")))

            generations = state["llm"].generate(
                requests,
                sampling_params=state["sampling"],
            )
            for (video_url, meta_val), gen in zip(meta_refs, generations):
                text_out = gen.outputs[0].text if gen.outputs else ""
                outputs.append(
                    {
                        "video_url": str(video_url),
                        "summary": str(text_out),
                        "meta": meta_val,
                    }
                )

        if callable(postprocess):
            mod = postprocess(outputs)
            if mod is not None:
                outputs = mod
        return pa.Table.from_pylist(outputs)

    def stage(ds):
        return ds.map_batches(
            _batch_fn,
            batch_format="pyarrow",
            num_gpus=1,
            batch_size=1,
        )

    def _close():
        llm = state.get("llm")
        if llm and hasattr(llm, "close"):
            try:
                llm.close()
            except Exception:
                pass

    stage.close = _close
    return stage


def run_dataset_pipeline(
    model_path: str,
    *,
    preprocess: Optional[Callable[[List[Dict[str, Any]]], Any]] = None,
    postprocess: Optional[Callable[[List[Dict[str, Any]]], Any]] = None,
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

    try:
        decode_udf = DecodeFramesUDF()

        ds_decoded = ds.map_batches(
            decode_udf,
            batch_format="pyarrow",
            batch_size=1,
        )

        llm_stage = build_llm_stage(
            VLLMProcessorConfig(model_path),
            preprocess=preprocess,
            postprocess=postprocess,
        )

        ds_inferred = llm_stage(ds_decoded)

        for row in ds_inferred.take_all():
            print("\n=== Dataset result ===")
            print(f"video:   {row['video_url']}")
            print(f"summary: {row['summary']}")
    finally:
        llm_stage.close()


def main() -> None:
    run_dataset_pipeline(EXAMPLE_MODEL_PATH)


if __name__ == "__main__":
    main()
