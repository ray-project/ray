"""KV-aware P/D with LoRA adapters through HAProxy (two P, two D)."""

import sys
import uuid

import pytest

from utils import deploy_pd_apps, pd_completion, prefill_cached_tokens


@pytest.fixture(scope="module")
def base_model():
    with deploy_pd_apps("serve_pd_kv_lora.yaml") as model:
        yield model


@pytest.mark.parametrize("adapter", ["adapter-a", "adapter-b"])
def test_lora_prefill_follows_kv_cache(base_model, adapter):
    """A repeated adapter prompt goes to the P replica holding its prefix."""
    model = f"{base_model}:{adapter}"
    prompt = f"Case {uuid.uuid4().hex}: " + "Describe the water cycle in detail. " * 8
    first = pd_completion(model, prompt)
    assert first["model"] == model
    assert prefill_cached_tokens(first) == 0
    assert prefill_cached_tokens(pd_completion(model, prompt)) > 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
