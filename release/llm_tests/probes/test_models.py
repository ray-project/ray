import pytest

from probes.models import model_loader
from probes.openai_client import openai_client


@pytest.mark.parametrize("model", model_loader.model_ids())
def test_get_model(model: str):
    model_description = openai_client.models.retrieve(model)
    assert model_description.id == model
    assert "rayllm_metadata" in model_description.model_dump()
