from __future__ import annotations

import asyncio
import base64
import threading
from io import BytesIO
from queue import Queue
from typing import Any, Dict, List

import pyarrow as pa

import ray
import ray.data
from ray.data.examples.data.video_processing.video_processor import VideoProcessor
from ray.data.llm import build_llm_processor, vLLMEngineProcessorConfig

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

    def _run_async(self, coro):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is None:
            return asyncio.run(coro)
        q: Queue = Queue(maxsize=1)

        def _runner():
            try:
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                res = new_loop.run_until_complete(coro)
                q.put((True, res))
            except Exception as e:
                q.put((False, e))
            finally:
                try:
                    new_loop.close()
                except Exception:
                    pass

        t = threading.Thread(target=_runner, daemon=True)
        t.start()
        ok, val = q.get()
        if ok:
            return val
        raise val

    def __call__(self, batch: Any):
        records = batch.to_pylist() if isinstance(batch, pa.Table) else list(batch)
        if not records:
            return pa.Table.from_pylist([])
        sources = [str(r["video_url"]) for r in records]
        prompts = [r.get("prompt", DEFAULT_PROMPT) for r in records]
        results = self._run_async(self.processor.process(sources))
        out: List[Dict[str, Any]] = []
        for row, prompt_text, res in zip(records, prompts, results):
            frames = res.get("frames", [])
            frames_b64: List[str] = []
            for f in frames:
                buf = BytesIO()
                f.save(buf, format="JPEG", quality=90)
                frames_b64.append(base64.b64encode(buf.getvalue()).decode("ascii"))
            out.append(
                {
                    "video_url": str(row.get("video_url")),
                    "prompt": str(prompt_text),
                    "frames_b64": frames_b64,
                }
            )
        return pa.Table.from_pylist(out)


def _preprocess(row: Dict[str, Any], max_images: int = 10) -> Dict[str, Any]:
    frames_b64: List[str] = row.get("frames_b64") or []
    if not frames_b64:
        raise RuntimeError(f"No frames decoded for video: {row.get('video_url')}")
    image_contents = [
        {"type": "image", "image": f"data:image/jpeg;base64,{b64}"}
        for b64 in frames_b64[:max_images]
    ]
    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {
            "role": "user",
            "content": [
                *image_contents,
                {"type": "text", "text": row.get("prompt", DEFAULT_PROMPT)},
            ],
        },
    ]
    return {
        "messages": messages,
        "sampling_params": {"temperature": 0.1, "top_p": 0.001, "max_tokens": 512},
        "video_url": row.get("video_url"),
    }


def run_dataset_pipeline(model_path: str) -> None:
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
    config = vLLMEngineProcessorConfig(
        model_source=model_path,
        batch_size=1,
        concurrency=1,
        has_image=True,
        engine_kwargs={
            "enable_chunked_prefill": True,
            "enforce_eager": True,
            "limit_mm_per_prompt": {"image": 10},
        },
        apply_chat_template=True,
    )
    decode_udf = DecodeFramesUDF()
    ds_decoded = ds.map_batches(decode_udf, batch_format="pyarrow", batch_size=1)
    inference_stage = build_llm_processor(
        config,
        preprocess=_preprocess,
        postprocess=lambda row: {
            "video_url": row.get("video_url"),
            "generated_text": row.get("generated_text", ""),
        },
    )
    ds_inferred = inference_stage(ds_decoded)
    for row in ds_inferred.take_all():
        print("\n=== Dataset result ===")
        print(f"video:   {row['video_url']}")
        print(f"generated_text: {row.get('generated_text', '')}")


def main() -> None:
    run_dataset_pipeline(EXAMPLE_MODEL_PATH)


if __name__ == "__main__":
    main()
