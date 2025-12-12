from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Dict,
    List,
    Optional,
    Protocol,
    Union,
)

from starlette.requests import Request

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
    from ray.llm._internal.serve.core.configs.openai_api_models import (
        ChatCompletionRequest,
        ChatCompletionResponse,
        CompletionRequest,
        CompletionResponse,
        ErrorResponse,
    )


@dataclass
class RawRequestInfo:
    """A serializable representation of important fields from a Starlette Request.

    This dataclass captures key request data that needs to be passed through
    RPC boundaries (e.g., from ingress to LLMServer). The Starlette Request
    object itself is not serializable, so we extract the needed fields here.

    Usage:
        raw_request = RawRequestInfo.from_starlette_request(starlette_request)
        # Pass raw_request through RPC...
        starlette_request = raw_request.to_starlette_request()
    """

    headers: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_starlette_request(cls, request: Request) -> "RawRequestInfo":
        """Create a RawRequestInfo from a Starlette Request object."""
        return cls(headers=dict(request.headers))

    def to_starlette_request(self) -> Request:
        """Create a minimal Starlette Request from this RawRequestInfo."""
        scope = {
            "type": "http",
            "method": "POST",
            "path": "/",
            "headers": [
                (k.lower().encode(), (v or "").encode())
                for k, v in self.headers.items()
            ],
            "query_string": b"",
        }
        return Request(scope)

    @classmethod
    def to_starlette_request_optional(
        cls, raw_request_info: Optional["RawRequestInfo"] = None
    ) -> Optional[Request]:
        """Convert RawRequestInfo to Starlette Request, or return None if input is None."""
        if raw_request_info is not None:
            return raw_request_info.to_starlette_request()
        return None


class DeploymentProtocol(Protocol):
    @classmethod
    def get_deployment_options(cls, **kwargs) -> Dict[str, Any]:
        """Get the default deployment options for the this deployment."""


class LLMServerProtocol(DeploymentProtocol):
    """
    This is the common interface between all the llm deployment. All llm deployments
    need to implement a sync constructor, an async start method, and check_health method.
    """

    def __init__(self):
        """
        Constructor takes basic setup that doesn't require async operations.
        """

    async def start(self) -> None:
        """
        Start the underlying engine. This handles async initialization.
        """

    async def chat(
        self,
        request: "ChatCompletionRequest",
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[Union[str, "ChatCompletionResponse", "ErrorResponse"], None]:
        """
        Inferencing to the engine for chat, and return the response.
        """

    async def completions(
        self,
        request: "CompletionRequest",
        raw_request_info: Optional[RawRequestInfo] = None,
    ) -> AsyncGenerator[
        Union[List[Union[str, "ErrorResponse"]], "CompletionResponse"], None
    ]:
        """
        Inferencing to the engine for completion api, and return the response.
        """

    async def check_health(self) -> None:
        """
        Check the health of the replica. Does not return anything.
        Raise error when the engine is dead and needs to be restarted.
        """

    async def reset_prefix_cache(self) -> None:
        """Reset the prefix cache of the underlying engine"""

    async def start_profile(self) -> None:
        """Start profiling"""

    async def stop_profile(self) -> None:
        """Stop profiling"""

    # TODO (Kourosh): This does not belong here.
    async def llm_config(self) -> Optional["LLMConfig"]:
        """Get the LLM config"""
