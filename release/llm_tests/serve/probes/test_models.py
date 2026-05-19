import pytest

from probes.models import model_loader
from probes.openai_client import openai_client


@pytest.mark.parametrize("model", model_loader.model_ids())
def test_get_model(model: str):
    # Use list() rather than retrieve(): under
    # RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING the ingress is vLLM's native
    # FastAPI app, which does not implement GET /v1/models/{model_id}
    # (https://github.com/vllm-project/vllm/pull/43127).
    served_ids = {m.id for m in openai_client.models.list().data}
    assert model in served_ids, f"{model=} not in {served_ids=}"
