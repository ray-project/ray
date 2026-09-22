"""Model discovery shared by ``OpenAiIngress`` and ``DirectStreamingIngress``.

Discovery is the one thing the two ingress classes have in common: the OpenAI
ingress also dispatches inference, while the direct-streaming control ingress
does nothing else. ``OpenAiIngress``'s discovery behavior is already pinned by
``test_lora_deployment_base_client.py``; these tests cover what is new -- that
both ingresses answer identically, and the control ingress's route inventory,
which is load bearing because the ingress request router takes exactly those
routes away from the model deployments.
"""

import sys
from typing import Awaitable, Callable, Dict, List, Optional
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ModelCard,
    OpenAIHTTPException,
)
from ray.llm._internal.serve.core.ingress.ingress import (
    DirectStreamingIngress,
    OpenAiIngress,
    _ModelDiscovery,
    make_direct_streaming_control_ingress,
)

BASE_MODEL = "meta-llama/Llama-2-7b-hf"
LORA_MODEL = f"{BASE_MODEL}:suffix:1234"
SECOND_MODEL = "qwen-0_5b"


def _model_card(model_id: str, **metadata) -> ModelCard:
    return ModelCard(
        id=model_id,
        object="model",
        owned_by="organization-owner",
        permission=[],
        metadata={"model_id": model_id, **metadata},
    )


async def _lora_metadata(model_id: str, base_path: str) -> Dict[str, object]:
    return {
        "model_id": model_id,
        "base_model_id": BASE_MODEL,
        "max_request_context_length": 4096,
    }


def _make_discovery(
    model_ids: List[str],
    *,
    lora_paths: Optional[Dict[str, str]] = None,
    metadata_func: Optional[Callable[..., Awaitable]] = None,
) -> _ModelDiscovery:
    return _ModelDiscovery(
        {model_id: _model_card(model_id) for model_id in model_ids},
        lora_paths=lora_paths,
        _get_lora_model_metadata_func=metadata_func,
    )


@pytest.mark.asyncio
class TestModelDiscovery:
    """Branches of the shared discovery that no ingress-level test reaches."""

    async def test_model_data_resolves_an_id_with_escaped_slashes(self):
        """`replace_prefix` rewrites `--` back to `/`, so a client that escaped
        the slash out of the URL path segment still resolves."""
        discovery = _make_discovery([BASE_MODEL])
        card = await discovery.model_data(BASE_MODEL.replace("/", "--"))
        assert card.id == BASE_MODEL

    async def test_models_omits_an_adapter_whose_config_cannot_be_read(self):
        """An unreadable adapter config must not fail the whole listing."""

        async def _raises(model_id: str, base_path: str):
            raise HTTPException(status_code=404, detail="no adapter config")

        discovery = _make_discovery(
            [BASE_MODEL],
            lora_paths={BASE_MODEL: "s3://base_path"},
            metadata_func=_raises,
        )
        with patch(
            "ray.llm._internal.serve.core.ingress.ingress.get_lora_model_ids",
            return_value=[LORA_MODEL],
        ):
            listed = await discovery.models()
        assert {card.id for card in listed.data} == {BASE_MODEL}


def _both_ingresses(**kwargs):
    """One ``OpenAiIngress`` and one ``DirectStreamingIngress`` over one config."""
    llm_deployments = {model_id: object() for model_id in kwargs["model_cards"]}
    return [
        OpenAiIngress(llm_deployments=llm_deployments, **kwargs),
        DirectStreamingIngress(llm_deployments=llm_deployments, **kwargs),
    ]


@pytest.mark.asyncio
class TestIngressDiscoveryEquivalence:
    """Both ingress classes must report the same models.

    A client that lists models through the direct-streaming control ingress and
    one that lists them through the OpenAI ingress are looking at the same
    application; the two paths differing would be a bug visible to users.
    """

    @pytest.fixture(name="ingresses")
    def _ingresses(self):
        return _both_ingresses(
            model_cards={
                BASE_MODEL: _model_card(BASE_MODEL),
                SECOND_MODEL: _model_card(SECOND_MODEL),
            },
            lora_paths={BASE_MODEL: "s3://base_path"},
            _get_lora_model_metadata_func=_lora_metadata,
        )

    async def test_models_agree(self, ingresses):
        with patch(
            "ray.llm._internal.serve.core.ingress.ingress.get_lora_model_ids",
            return_value=[LORA_MODEL],
        ):
            listings = [
                {card.id for card in (await ingress.models()).data}
                for ingress in ingresses
            ]
        assert listings[0] == listings[1] == {BASE_MODEL, SECOND_MODEL, LORA_MODEL}

    @pytest.mark.parametrize("model", [BASE_MODEL, SECOND_MODEL, LORA_MODEL])
    async def test_model_data_agrees(self, ingresses, model: str):
        cards = [await ingress.model_data(model) for ingress in ingresses]
        assert cards[0].model_dump() == cards[1].model_dump()

    async def test_unknown_model_404s_on_both(self, ingresses):
        for ingress in ingresses:
            with pytest.raises(OpenAIHTTPException) as e:
                await ingress.model_data("not-configured")
            assert e.value.status_code == 404

    async def test_mismatched_deployments_and_cards_are_rejected(self):
        with pytest.raises(ValueError, match="same model IDs"):
            DirectStreamingIngress(
                llm_deployments={BASE_MODEL: object()},
                model_cards={SECOND_MODEL: _model_card(SECOND_MODEL)},
            )


class TestControlIngressRoutes:
    """The control ingress's route inventory is a routing contract.

    The builder extracts this inventory from the FastAPI app and passes it to
    ``LLMRouter``. Anything extra here -- a docs page, an inference endpoint --
    is a path silently taken away from the model deployments.
    """

    def test_declares_exactly_the_two_discovery_routes(self):
        _, patterns = make_direct_streaming_control_ingress()
        assert [(p.methods, p.path) for p in patterns] == [
            (["GET"], "/v1/models"),
            (["GET"], "/v1/models/{model:path}"),
        ]

    def test_is_not_an_openai_ingress(self):
        """Inheriting the data-plane ingress would pull its inference routes
        (and with them `/v1/chat/completions`) onto the control ingress."""
        assert not issubclass(DirectStreamingIngress, OpenAiIngress)


class TestControlIngressDeploymentOptions:
    def test_does_not_inherit_scale_to_zero(self):
        """``OpenAiIngress`` scales to zero with its models; the control ingress
        is the app's only front door, so it may not."""
        scale_to_zero = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id=BASE_MODEL),
            deployment_config={"autoscaling_config": {"min_replicas": 0}},
        )

        options = DirectStreamingIngress.get_deployment_options([scale_to_zero])
        assert "min_replicas" not in options["autoscaling_config"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
