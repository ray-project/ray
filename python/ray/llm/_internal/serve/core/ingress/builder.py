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
    OpenAiIngress,
    make_fastapi_ingress,
)
from ray.llm._internal.serve.core.server.builder import (
    build_llm_deployment,
)
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve.config import DeploymentActorConfig, RequestRouterConfig
from ray.serve.deployment import Application
from ray.serve.experimental.round_robin_router import RoundRobinRouter

logger = get_logger(__name__)


def _get_dynamo_direct_streaming_actor_config(llm_config: LLMConfig) -> Optional[dict]:
    kv_transfer_config = llm_config.engine_kwargs.get("kv_transfer_config") or {}
    if kv_transfer_config.get("kv_connector") != "DynamoConnector":
        return None

    extra_config = kv_transfer_config.setdefault("kv_connector_extra_config", {})
    dynamo_config = extra_config.setdefault("ray_serve_dynamo", {})
    if not dynamo_config.get("endpoint"):
        return None

    from ray.llm._internal.serve.routing_policies.dynamo_kv import (
        DYNAMO_KV_ROUTER_DEPLOYMENT_ACTOR_NAME,
    )

    model_loading_config = llm_config.model_loading_config
    model_source = model_loading_config.model_source
    if not isinstance(model_source, str):
        model_source = llm_config.model_id

    deployment_actor_name = dynamo_config.get(
        "deployment_actor_name", DYNAMO_KV_ROUTER_DEPLOYMENT_ACTOR_NAME
    )
    dynamo_config["deployment_actor_name"] = deployment_actor_name

    actor_config = {
        **dynamo_config,
        "model_id": llm_config.model_id,
        "model_source": model_source,
        "tokenizer_source": model_loading_config.tokenizer_source,
        "trust_remote_code": llm_config.engine_kwargs.get("trust_remote_code", False),
    }
    actor_config.pop("actor_name", None)
    return actor_config


def _get_dynamo_direct_streaming_router_config(
    llm_config: LLMConfig,
) -> Optional[RequestRouterConfig]:
    actor_config = _get_dynamo_direct_streaming_actor_config(llm_config)
    if actor_config is None:
        return None

    from ray.llm._internal.serve.routing_policies.dynamo_kv import (
        DynamoKVRequestRouter,
    )

    return RequestRouterConfig(
        request_router_class=DynamoKVRequestRouter,
        request_router_kwargs={
            "deployment_actor_name": actor_config["deployment_actor_name"],
        },
    )


def _get_dynamo_direct_streaming_deployment_actor(
    llm_config: LLMConfig,
) -> Optional[DeploymentActorConfig]:
    actor_config = _get_dynamo_direct_streaming_actor_config(llm_config)
    if actor_config is None:
        return None

    from ray.llm._internal.serve.routing_policies.dynamo_kv import (
        DynamoKVRouterActor,
    )

    actor_options = {"num_cpus": 0}
    actor_options.update(actor_config.pop("deployment_actor_options", {}))

    return DeploymentActorConfig(
        name=actor_config["deployment_actor_name"],
        actor_class=DynamoKVRouterActor,
        init_args=(actor_config,),
        actor_options=actor_options,
    )


def _deployment_actor_name(actor_config: Union[dict, DeploymentActorConfig]) -> str:
    if isinstance(actor_config, dict):
        return actor_config["name"]
    return actor_config.name


def _get_direct_streaming_serve_options(
    llm_config: LLMConfig,
    override_serve_options: Optional[dict] = None,
) -> dict:
    override_serve_options = dict(override_serve_options or {})
    dynamo_deployment_actor = _get_dynamo_direct_streaming_deployment_actor(llm_config)
    if (
        "request_router_config" not in llm_config.deployment_config
        and "request_router_config" not in override_serve_options
    ):
        dynamo_router_config = _get_dynamo_direct_streaming_router_config(llm_config)
        override_serve_options["request_router_config"] = (
            dynamo_router_config
            or RequestRouterConfig(
                request_router_class=RoundRobinRouter,
            )
        )
    if dynamo_deployment_actor is not None:
        deployment_actors = list(
            override_serve_options.get(
                "deployment_actors",
                llm_config.deployment_config.get("deployment_actors") or [],
            )
        )
        if not any(
            _deployment_actor_name(actor) == dynamo_deployment_actor.name
            for actor in deployment_actors
        ):
            deployment_actors.append(dynamo_deployment_actor)
        override_serve_options["deployment_actors"] = deployment_actors
    return override_serve_options


def _build_direct_streaming_llm_deployment(
    llm_config: LLMConfig,
    *,
    name_prefix: Optional[str] = None,
    bind_kwargs: Optional[dict] = None,
    override_serve_options: Optional[dict] = None,
    deployment_cls: Optional[Type[LLMServer]] = None,
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
    )


def _build_openai_ingress_request_router(*, server: Application) -> Application:
    """Build the ingress request router peer for OpenAI compatible LLM apps.

    The returned Application is attached to the ingress application with
    ``Application._with_ingress_request_router``.

    ``num_replicas`` is pinned to 1 because HAProxy's ingress request router
    backend currently expects a single endpoint. TODO(eicherseiji): expose
    these as a user-overridable IngressRequestRouterConfig once HAProxy
    supports multiple router replicas.
    """
    from ray.llm._internal.serve.core.ingress.router import LLMRouter

    return serve.deployment(
        LLMRouter,
        num_replicas=1,
        max_ongoing_requests=1000,
    ).bind(server=server)


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


def _validate_direct_streaming_ingress_config(
    ingress_deployment_config: Optional[dict],
    ingress_cls_config: IngressClsConfig,
) -> None:
    if ingress_deployment_config:
        raise ValueError(
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING does not support "
            "ingress_deployment_config because the LLM server class is used "
            "directly as the ingress deployment. Configure the server through "
            "each LLMConfig.deployment_config instead."
        )

    if (
        ingress_cls_config.ingress_cls != OpenAiIngress
        or ingress_cls_config.ingress_extra_kwargs
    ):
        raise ValueError(
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING does not support "
            "ingress_cls_config because the LLM server class is used directly "
            "as the ingress deployment."
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

    # Direct streaming attaches LLMRouter as the ingress request router and
    # uses the LLMServer deployment itself as the ingress app, so it returns
    # before the regular OpenAiIngress wiring.
    if RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING:
        if len(llm_configs) > 1:
            raise ValueError(
                "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING currently supports exactly "
                "one LLM config. Multi-model direct streaming requires composing "
                "multiple LLMServer deployments into the main application graph, "
                "which is not supported yet."
            )
        _validate_direct_streaming_ingress_config(
            builder_config.ingress_deployment_config,
            builder_config.ingress_cls_config,
        )
        direct_deployment = _build_direct_streaming_llm_deployment(llm_configs[0])
        logger.info(
            "Direct streaming enabled: "
            "LLMServer=ingress, LLMRouter=ingress_request_router"
        )
        return direct_deployment._with_ingress_request_router(
            _build_openai_ingress_request_router(server=direct_deployment)
        )

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
