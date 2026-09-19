"""Model discovery shared by ``OpenAiIngress`` and ``DirectStreamingIngress``.

Discovery is the one thing the two ingress classes have in common: the OpenAI
ingress also dispatches inference, while the direct-streaming control ingress
does nothing else. These tests pin the shared behavior to ``_ModelDiscovery``,
assert both ingresses answer identically, and pin the control ingress's route
inventory -- which is load bearing, because the ingress request router takes
exactly those routes away from the model deployments.
"""

import sys
from typing import Awaitable, Callable, Dict, List, Optional
from unittest.mock import patch

import pytest

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
    async def test_base_model_card_is_returned_verbatim(self):
        discovery = _make_discovery([BASE_MODEL])
        assert await discovery.model(BASE_MODEL) is discovery.model_cards[BASE_MODEL]

    async def test_unknown_model_has_no_card(self):
        discovery = _make_discovery([BASE_MODEL])
        assert await discovery.model("not-configured") is None

    async def test_model_data_404s_for_an_unknown_model(self):
        discovery = _make_discovery([BASE_MODEL])
        with pytest.raises(OpenAIHTTPException) as e:
            await discovery.model_data("not-configured")
        assert e.value.status_code == 404
        assert e.value.type == "InvalidModel"

    @pytest.mark.parametrize(
        ("requested", "expected"),
        [
            (BASE_MODEL, BASE_MODEL),
            # `replace_prefix` rewrites `--` back to `/`, so a client that
            # escaped the slash out of the URL path segment still resolves.
            (BASE_MODEL.replace("/", "--"), BASE_MODEL),
        ],
    )
    async def test_model_data_resolves_ids_containing_slashes(
        self, requested: str, expected: str
    ):
        discovery = _make_discovery([BASE_MODEL])
        assert (await discovery.model_data(requested)).id == expected

    async def test_models_lists_every_configured_base_model(self):
        discovery = _make_discovery([BASE_MODEL, SECOND_MODEL])
        listed = await discovery.models()
        assert {card.id for card in listed.data} == {BASE_MODEL, SECOND_MODEL}

    async def test_lora_metadata_is_merged_over_the_base_card(self):
        discovery = _make_discovery(
            [BASE_MODEL],
            lora_paths={BASE_MODEL: "s3://base_path"},
            metadata_func=_lora_metadata,
        )
        card = await discovery.model(LORA_MODEL)
        assert card.id == LORA_MODEL
        assert card.owned_by == discovery.model_cards[BASE_MODEL].owned_by
        assert card.metadata["model_id"] == LORA_MODEL
        assert card.metadata["base_model_id"] == BASE_MODEL
        assert card.metadata["max_request_context_length"] == 4096

    async def test_models_includes_adapters_for_a_lora_base_model(self):
        discovery = _make_discovery(
            [BASE_MODEL],
            lora_paths={BASE_MODEL: "s3://base_path"},
            metadata_func=_lora_metadata,
        )
        with patch(
            "ray.llm._internal.serve.core.ingress.ingress.get_lora_model_ids",
            return_value=[LORA_MODEL],
        ):
            listed = await discovery.models()
        assert {card.id for card in listed.data} == {BASE_MODEL, LORA_MODEL}

    async def test_models_omits_an_adapter_whose_config_cannot_be_read(self):
        """An unreadable adapter config must not fail the whole listing."""
        from fastapi import HTTPException

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

    async def test_discovery_owns_its_copies(self):
        model_cards = {BASE_MODEL: _model_card(BASE_MODEL)}
        lora_paths = {BASE_MODEL: "s3://base_path"}
        discovery = _ModelDiscovery(model_cards, lora_paths=lora_paths)

        model_cards[SECOND_MODEL] = _model_card(SECOND_MODEL)
        lora_paths.clear()

        assert set(discovery.model_cards) == {BASE_MODEL}
        assert discovery.lora_paths == {BASE_MODEL: "s3://base_path"}


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
        for cls in (OpenAiIngress, DirectStreamingIngress):
            with pytest.raises(ValueError, match="same model IDs"):
                cls(
                    llm_deployments={BASE_MODEL: object()},
                    model_cards={SECOND_MODEL: _model_card(SECOND_MODEL)},
                )


async def _build_control_ingress_replica():
    """Instantiate the ``serve.ingress``-wrapped control ingress in-process.

    ``make_fastapi_ingress`` produces a class whose ``__init__`` is async (it is
    what a Serve replica awaits), so construct it the way the replica does
    rather than calling the class directly.
    """
    cls, _ = make_direct_streaming_control_ingress()
    replica = cls.__new__(cls)
    await cls.__init__(
        replica,
        llm_deployments={BASE_MODEL: object()},
        model_cards={BASE_MODEL: _model_card(BASE_MODEL)},
    )
    return replica


@pytest.mark.asyncio
class TestControlIngressRoutes:
    """The control ingress's route inventory is a routing contract.

    The builder extracts this inventory from the FastAPI app and passes it to
    ``LLMRouter``. Anything extra here -- a docs page, an inference endpoint --
    is a path silently taken away from the model deployments.
    """

    async def test_declares_exactly_the_two_discovery_routes(self):
        _, patterns = make_direct_streaming_control_ingress()
        assert [(p.methods, p.path) for p in patterns] == [
            (["GET"], "/v1/models"),
            (["GET"], "/v1/models/{model:path}"),
        ]

    async def test_chat_completions_is_not_claimed(self):
        _, patterns = make_direct_streaming_control_ingress()
        paths = {p.path for p in patterns}
        assert "/v1/chat/completions" not in paths

    @pytest.mark.parametrize("path", ["/docs", "/redoc", "/openapi.json"])
    async def test_fastapi_docs_routes_are_disabled(self, path: str):
        _, patterns = make_direct_streaming_control_ingress()
        paths = {p.path for p in patterns}
        assert path not in paths

    @pytest.mark.parametrize(
        "method_name",
        [
            "chat",
            "completions",
            "embeddings",
            "tokenize",
            "detokenize",
            "score",
            "transcriptions",
        ],
    )
    def test_no_inference_handlers_are_defined(self, method_name: str):
        assert not hasattr(DirectStreamingIngress, method_name)

    def test_is_not_an_openai_ingress(self):
        """Subclassing would inherit the OpenAI routes and re-add the proxy hop."""
        assert not issubclass(DirectStreamingIngress, OpenAiIngress)

    async def test_check_health_is_exposed_for_serve(self):
        replica = await _build_control_ingress_replica()
        assert await replica.check_health() is None


class TestControlIngressDeploymentOptions:
    def test_does_not_inherit_scale_to_zero(self):
        """``OpenAiIngress`` scales to zero with its models; this one may not."""
        from ray.llm._internal.serve.core.configs.llm_config import (
            LLMConfig,
            ModelLoadingConfig,
        )

        scale_to_zero = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id=BASE_MODEL),
            deployment_config={"autoscaling_config": {"min_replicas": 0}},
        )

        assert (
            OpenAiIngress.get_deployment_options([scale_to_zero])["autoscaling_config"][
                "min_replicas"
            ]
            == 0
        )
        options = DirectStreamingIngress.get_deployment_options([scale_to_zero])
        assert "min_replicas" not in options["autoscaling_config"]

    def test_options_are_not_shared_between_calls(self):
        first = DirectStreamingIngress.get_deployment_options()
        first["autoscaling_config"]["min_replicas"] = 7
        assert "min_replicas" not in (
            DirectStreamingIngress.get_deployment_options()["autoscaling_config"]
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
