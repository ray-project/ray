import sys
import logging

import pytest

import ray
from ray.data.llm import build_llm_processor, vLLMEngineProcessorConfig

logger = logging.getLogger(__name__)


def test_run_processor_in_loop():
    """Test running a processor in a loop."""

    for _ in range(2):
        processor_config = vLLMEngineProcessorConfig(
            model_source="unsloth/Llama-3.2-1B-Instruct",
            engine_kwargs=dict(
                enforce_eager=True,
            ),
        )

        ds = ray.data.range(10)
        ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})

        processor = build_llm_processor(
            processor_config,
            preprocess=lambda row: dict(
                messages=[
                    {"role": "system", "content": "You are a calculator"},
                    {"role": "user", "content": f"{row['id']} ** 3 = ?"},
                ],
                sampling_params=dict(
                    max_tokens=10,
                ),
            ),
            postprocess=lambda row: {
                "resp": row["generated_text"],
            },
        )
        ds = processor(ds)
        ds = ds.materialize()
        outs = ds.take_all()
        assert len(outs) == 10
        assert all("resp" in out for out in outs)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
