import os
import pprint
from typing import Any, Dict, List, Optional, Type, Union

from pydantic import Field, field_validator, model_validator

from ray import serve
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.dict_utils import (
    maybe_apply_llm_deployment_config_defaults,
)
from ray.llm._internal.common.utils.import_utils import load_class
from ray.llm._internal.serve.constants import RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.openai_api_models import to_model_metadata
from ray.llm._internal.serve.core.ingress.ingress import (
    DirectStreamingIngress,
    OpenAiIngress,
    make_direct_streaming_control_ingress,
    make_fastapi_ingress,
)
from ray.llm._internal.serve.core.server.builder import (
    build_llm_deployment,
)
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_router import (
    is_kv_aware,
)
from ray.serve._private.constants import (
    RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY,
)
from ray.serve._private.thirdparty.get_asgi_route_name import RoutePattern
from ray.serve.config import RequestRouterConfig
from ray.serve.deployment import Application
from ray.serve.experimental.round_robin_router import RoundRobinRouter

logger = get_logger(__name__)


def _get_direct_streaming_serve_options(
    llm_config: LLMConfig,
    override_serve_options: Optional[dict] = None,
) -> dict:
    override_serve_options = dict(override_serve_options or {})
    if (
        "request_router_config" not in llm_config.deployment_config
        and "request_router_config" not in override_serve_options
    ):
        override_serve_options["request_router_config"] = RequestRouterConfig(
            request_router_class=RoundRobinRouter,
        )
    return override_serve_options


def _build_direct_streaming_llm_deployment(
    llm_config: LLMConfig,
    *,
    name_prefix: Optional[str] = None,
    bind_kwargs: Optional[dict] = None,
    override_serve_options: Optional[dict] = None,
    deployment_cls: Optional[Type[LLMServer]] = None,
    direct_http: bool = False,
) -> Application:
    """Build an LLM deployment with late-bound ASGI ingress enabled.

    Used by the OpenAI, DP, and PD builders to wrap their respective server
    class (``LLMServer``, ``DPServer``, ``PDDecodeServer``/``DPPDDecodeServer``)
    as the ingress. The real ASGI app (vLLM FastAPI) is constructed inside
    ``LLMServer.__serve_build_asgi_app__`` after the engine starts; subclasses
    inherit this hook.

    Replica selection is driven by the deployment's ``request_router_config``.
    Default to ``RoundRobinRouter`` when the user hasn't set one, and otherwise
    leave their configured value untouched.

    ``direct_http`` marks the deployment as owning HTTP listeners without being
    the application's ingress. The standard OpenAI builder sets it, because
    there a ``DirectStreamingIngress`` deployment is the app's front door; the
    DP and P/D builders do not, because there the server deployment still *is*
    the ingress.
    """
    server_cls = deployment_cls or llm_config.server_cls or LLMServer
    return build_llm_deployment(
        llm_config,
        name_prefix=name_prefix,
        bind_kwargs=bind_kwargs,
        deployment_cls=serve.ingress()(server_cls),
        override_serve_options=_get_direct_streaming_serve_options(
            llm_config, override_serve_options
        ),
        direct_http=direct_http,
    )


def _get_tokenizing_router_runtime_env(llm_config: LLMConfig) -> Optional[dict]:
    runtime_env = llm_config.runtime_env
    if runtime_env is None or "env_vars" not in runtime_env:
        return None

    return {"env_vars": runtime_env["env_vars"]}


def _build_openai_ingress_request_router(
    *,
    servers: Dict[str, Application],
    llm_config: Optional[LLMConfig] = None,
    ingress: Optional[Application] = None,
    ingress_route_patterns: Optional[List[RoutePattern]] = None,
) -> Application:
    """Build the ingress request router peer for OpenAI compatible LLM apps.

    The returned Application is attached to the ingress application with
    ``Application._with_ingress_request_router``.

    ``num_cpus=0`` lets the router schedule alongside the proxy on any node,
    including a resource-less head node. ``max_ongoing_requests`` is raised
    above the Serve default because the router sits on the ingress hot path and
    must not throttle it.

    ``servers`` is the model-id -> model deployment registry the router selects
    from; the DP and P/D builders pass their single server, the standard builder
    passes one entry per configured model.

    ``ingress``, when given, is the application's control-plane ingress.
    ``ingress_route_patterns`` is extracted from that ingress's FastAPI app at
    build time, and tells the router which requests to send there instead of to
    a model. The DP and P/D topologies have no separate ingress -- their server
    deployment *is* the ingress -- so they leave both unset.

    Pre-routing tokenization is wired on only when ``llm_config`` configures a
    KVAwareRouter, the sole policy that scores replicas on prompt token IDs.
    """
    from ray.llm._internal.serve.core.ingress.router import LLMRouter

    kv_aware = llm_config is not None and is_kv_aware(llm_config)

    ray_actor_options: Dict[str, Any] = {"num_cpus": 0}
    if kv_aware:
        runtime_env = _get_tokenizing_router_runtime_env(llm_config)
        if runtime_env is not None:
            ray_actor_options["runtime_env"] = runtime_env

    deployment = serve.deployment(
        LLMRouter,
        max_ongoing_requests=1000,
        ray_actor_options=ray_actor_options,
    )
    return deployment.bind(
        servers=servers,
        llm_config=llm_config if kv_aware else None,
        ingress=ingress,
        ingress_route_patterns=ingress_route_patterns,
    )


class IngressClsConfig(BaseModelExtended):
    ingress_cls: Union[str, Type[OpenAiIngress]] = Field(
        default=OpenAiIngress,
        description="The class name of the ingress to use. It can be in form of `module_name.class_name` or `module_name:class_name` or the class itself. The class constructor should take the following arguments: `(llm_deployments: Dict[str, DeploymentHandle], model_cards: Dict[str, ModelCard], lora_paths: Optional[Dict[str, str]] = None, **extra_kwargs)` where the dicts are keyed by base model ID.",
    )

    ingress_extra_kwargs: Optional[dict] = Field(
        default_factory=dict,
        description="""The kwargs to bind to the ingress deployment. This will be passed to the ingress class constructor.""",
    )

    @field_validator("ingress_cls")
    @classmethod
    def validate_class(
        cls, value: Union[str, Type[OpenAiIngress]]
    ) -> Type[OpenAiIngress]:
        if isinstance(value, str):
            return load_class(value)
        return value


class LLMServingArgs(BaseModelExtended):
    llm_configs: List[Union[str, dict, LLMConfig]] = Field(
        description="A list of LLMConfigs, or dicts representing LLMConfigs, or paths to yaml files defining LLMConfigs.",
    )
    ingress_cls_config: Union[dict, IngressClsConfig] = Field(
        default_factory=IngressClsConfig,
        description="The configuration for the ingress class. It can be a dict representing the ingress class configuration, or an IngressClsConfig object.",
    )
    ingress_deployment_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="""
            The Ray @server.deployment options for the ingress server.
        """,
    )

    @field_validator("ingress_cls_config")
    @classmethod
    def _validate_ingress_cls_config(
        cls, value: Union[dict, IngressClsConfig]
    ) -> IngressClsConfig:
        if isinstance(value, dict):
            return IngressClsConfig.model_validate(value)
        return value

    @field_validator("llm_configs")
    @classmethod
    def _validate_llm_configs(
        cls, value: List[Union[str, dict, LLMConfig]]
    ) -> List[LLMConfig]:
        llm_configs = []
        for config in value:
            if isinstance(config, str):
                if not os.path.exists(config):
                    raise ValueError(
                        f"Could not load model config from {config}, as the file does not exist."
                    )
                llm_configs.append(LLMConfig.from_file(config))
            elif isinstance(config, dict):
                llm_configs.append(LLMConfig.model_validate(config))
            elif isinstance(config, LLMConfig):
                llm_configs.append(config)
            else:
                raise TypeError(f"Invalid LLMConfig type: {type(config)}")
        return llm_configs

    @model_validator(mode="after")
    def _validate_model_ids(self):
        """Validate that model IDs are unique and at least one model is configured."""
        if len({m.model_id for m in self.llm_configs}) != len(self.llm_configs):
            raise ValueError("Duplicate models found. Make sure model ids are unique.")

        if len(self.llm_configs) == 0:
            raise ValueError(
                "List of models is empty. Maybe some parameters cannot be parsed into the LLMConfig config."
            )
        return self


def _validate_direct_streaming_ingress_cls_config(
    ingress_cls_config: IngressClsConfig,
) -> None:
    """Reject a user-supplied ingress class while direct streaming is on.

    Every builder rejects it, for two different reasons. Where the server class
    is the ingress (DP, P/D) there is no ingress deployment to substitute at
    all. In the standard builder there is one, ``DirectStreamingIngress``, but
    the routes it declares are exactly the routes the ingress request router
    takes away from the model deployments, so a custom ingress would silently
    redirect inference traffic through a Python hop. Custom control ingresses
    and custom control routes are deferred until that coupling is expressed in
    the API.
    """
    if (
        ingress_cls_config.ingress_cls != OpenAiIngress
        or ingress_cls_config.ingress_extra_kwargs
    ):
        raise ValueError(
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING does not support "
            "ingress_cls_config. Direct streaming owns the ingress deployment "
            "and the set of routes it claims from the model deployments; "
            "custom ingress classes and custom ingress routes are not "
            "supported yet."
        )


def _validate_direct_streaming_ingress_config(
    ingress_deployment_config: Optional[dict],
    ingress_cls_config: IngressClsConfig,
) -> None:
    """Validation for the builders whose LLM server *is* the ingress (DP, P/D).

    Those topologies have no ingress deployment of their own, so there is
    nothing for ``ingress_deployment_config`` to configure. The standard
    builder does have one and accepts it; see
    ``_build_direct_streaming_openai_app``.
    """
    if ingress_deployment_config:
        raise ValueError(
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING does not support "
            "ingress_deployment_config because the LLM server class is used "
            "directly as the ingress deployment. Configure the server through "
            "each LLMConfig.deployment_config instead."
        )

    _validate_direct_streaming_ingress_cls_config(ingress_cls_config)


def _get_min_replicas(deployment_options: Dict[str, Any]) -> Optional[int]:
    """``autoscaling_config.min_replicas`` from built deployment options.

    The value survives merging as either a dict or an ``AutoscalingConfig``
    depending on what the user supplied, so read both shapes.
    """
    autoscaling_config = deployment_options.get("autoscaling_config")
    if autoscaling_config is None:
        return None
    if isinstance(autoscaling_config, dict):
        return autoscaling_config.get("min_replicas")
    return getattr(autoscaling_config, "min_replicas", None)


def _validate_control_ingress_stays_up(ingress_options: Dict[str, Any]) -> None:
    """The control ingress must keep at least one replica.

    Unlike ``OpenAiIngress``, it may not scale to zero along with the models.
    It is the only deployment that answers ``GET /v1/models``, and the ingress
    request router learns which routes belong to the ingress by asking a
    running ingress replica -- with none, model discovery is exactly what an
    idle application loses first. Serve's controller also stops publishing an
    app's direct-HTTP targets once its ingress has no running replicas, so a
    scaled-to-zero ingress takes the model backends down with it.
    """
    if _get_min_replicas(ingress_options) == 0:
        raise ValueError(
            "The direct-streaming control ingress cannot scale to zero: it is "
            "the only deployment serving model discovery, and the ingress "
            "request router needs a running ingress replica to resolve its "
            "routes. Set ingress_deployment_config.autoscaling_config."
            "min_replicas to at least 1."
        )


def _validate_direct_streaming_models(llm_configs: List[LLMConfig]) -> None:
    """Reject the multi-model configurations direct streaming cannot serve yet.

    Single-model apps are unaffected by all of these.
    """
    if len(llm_configs) <= 1:
        return

    if not RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY:
        raise ValueError(
            "Serving multiple models with RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING "
            "requires RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY=1. The "
            "request's `model` field lives in its body, so without body "
            "forwarding HAProxy gives the ingress request router nothing to "
            "select a model deployment with and every request fails closed. "
            "Set it in the Ray controller's and proxies' environment, or "
            "deploy one application per model."
        )

    kv_aware_models = sorted(c.model_id for c in llm_configs if is_kv_aware(c))
    if kv_aware_models:
        raise ValueError(
            "KV-aware routing supports one model per application. The KV token "
            "tracker is a process global and the pre-routing tokenizer is "
            "per-model, so several KV-aware models in one ingress request "
            "router would share one tracker and score against the wrong "
            f"tokenizer. Models requesting KV-aware routing: {kv_aware_models}."
        )

    lora_models = sorted(c.model_id for c in llm_configs if c.lora_config is not None)
    if lora_models:
        raise ValueError(
            "LoRA supports one model per application under "
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING. Adapter-aware replica "
            "selection is not implemented for direct streaming, so a "
            "multi-model app cannot report which adapters are loaded where. "
            f"Models configuring lora_config: {lora_models}."
        )


def _build_direct_streaming_openai_app(builder_config: LLMServingArgs) -> Application:
    """Build the multi-model direct-streaming topology.

        DirectStreamingIngress          discovery routes; app front door
        |- LLMServer:model-a            _direct_http=True
        |- LLMServer:model-b            _direct_http=True
        `- LLMRouter                    ingress request router

    Every request reaches HAProxy, which asks ``LLMRouter`` where to send it.
    A request on a route the ingress declares goes to an ingress replica;
    anything else is matched by its ``model`` field to one model deployment and
    sent straight to that deployment's replica, with no Python proxy hop.

    The same bound model ``Application`` objects go to both the ingress (as
    ``llm_deployments``) and the router (as ``servers``). Serve's graph
    traversal keys on object identity, so passing the same objects is what makes
    the router a peer of the existing model deployments rather than a second
    copy of them -- and ``build_app`` rejects an ingress request router that
    introduces more than its own deployment.
    """
    llm_configs = builder_config.llm_configs
    _validate_direct_streaming_ingress_cls_config(builder_config.ingress_cls_config)
    _validate_direct_streaming_models(llm_configs)

    model_deployments = {
        c.model_id: _build_direct_streaming_llm_deployment(c, direct_http=True)
        for c in llm_configs
    }
    model_cards = {c.model_id: to_model_metadata(c.model_id, c) for c in llm_configs}
    lora_paths = {
        c.model_id: c.lora_config.dynamic_lora_loading_path
        for c in llm_configs
        if c.lora_config is not None
    }

    ingress_options = maybe_apply_llm_deployment_config_defaults(
        DirectStreamingIngress.get_deployment_options(llm_configs),
        builder_config.ingress_deployment_config,
    )
    _validate_control_ingress_stays_up(ingress_options)

    logger.info("============== Ingress Options ==============")
    logger.info(pprint.pformat(ingress_options))

    ingress_cls, ingress_route_patterns = make_direct_streaming_control_ingress()
    ingress = serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=model_deployments,
        model_cards=model_cards,
        lora_paths=lora_paths,
    )

    logger.info(
        "Direct streaming enabled: DirectStreamingIngress=ingress, "
        "LLMRouter=ingress_request_router, models=%s",
        sorted(model_deployments),
    )
    return ingress._with_ingress_request_router(
        _build_openai_ingress_request_router(
            servers=model_deployments,
            # Only a lone model can be KV-aware; `_validate_direct_streaming_models`
            # has already rejected the multi-model case.
            llm_config=llm_configs[0] if len(llm_configs) == 1 else None,
            ingress=ingress,
            ingress_route_patterns=ingress_route_patterns,
        )
    )


def build_openai_app(builder_config: dict) -> Application:
    """Build an OpenAI compatible app with the llm deployment setup from
    the given builder configuration.

    Args:
        builder_config: The configuration for the builder. It has to conform
            to the LLMServingArgs pydantic model.

    Returns:
        The configured Ray Serve Application router.
    """

    builder_config = LLMServingArgs.model_validate(builder_config)
    llm_configs = builder_config.llm_configs

    # Direct streaming fronts the model deployments with a control-plane ingress
    # and an LLMRouter, so it returns before the regular OpenAiIngress wiring.
    if RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING:
        return _build_direct_streaming_openai_app(builder_config)

    llm_deployments = {c.model_id: build_llm_deployment(c) for c in llm_configs}
    model_cards = {c.model_id: to_model_metadata(c.model_id, c) for c in llm_configs}
    lora_paths = {
        c.model_id: c.lora_config.dynamic_lora_loading_path
        for c in llm_configs
        if c.lora_config is not None
    }

    ingress_cls_config = builder_config.ingress_cls_config
    default_ingress_options = ingress_cls_config.ingress_cls.get_deployment_options(
        llm_configs
    )

    ingress_options = maybe_apply_llm_deployment_config_defaults(
        default_ingress_options, builder_config.ingress_deployment_config
    )

    ingress_cls = make_fastapi_ingress(ingress_cls_config.ingress_cls)

    logger.info("============== Ingress Options ==============")
    logger.info(pprint.pformat(ingress_options))

    return serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=llm_deployments,
        model_cards=model_cards,
        lora_paths=lora_paths,
        **ingress_cls_config.ingress_extra_kwargs,
    )
