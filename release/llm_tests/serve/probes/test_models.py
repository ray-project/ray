import pytest

from probes.models import model_loader
from probes.openai_client import openai_client


@pytest.mark.parametrize("model", model_loader.model_ids())
def test_get_model(model: str):
    served_ids = {m.id for m in openai_client.models.list().data}
    assert model in served_ids, f"{model=} not in {served_ids=}"
