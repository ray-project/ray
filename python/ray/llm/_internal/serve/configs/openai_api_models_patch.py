import json
from abc import ABC, abstractmethod

from typing import (
    Any,
    Dict,
    Optional,
    Union,
    Literal,
    TYPE_CHECKING,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)
from typing_extensions import Annotated
from ray.llm._internal.utils import try_import

if TYPE_CHECKING:
    from vllm.sampling_params import GuidedDecodingParams

vllm = try_import("vllm")


class ErrorResponse(BaseModel):
    message: str
    internal_message: str
    code: int
    type: str
    param: Dict[str, Any] = {}
    # We use `Any` here since pydantic doesn't have a validator for exceptions.
    # This is fine since the field is excluded.
    original_exception: Annotated[Optional[Any], Field(exclude=True)] = None


class ResponseFormat(BaseModel, ABC):
    # make allow extra fields false
    model_config = ConfigDict(extra="forbid")

    @abstractmethod
    def to_guided_decoding_params(
        self, backend: str
    ) -> Optional["GuidedDecodingParams"]:
        """Convert the response format to a vLLM guided decoding params.

        Args:
            backend: The backend to use for the guided decoding. (e.g. "xgrammar", "outlines")

        Returns:
            A vLLM guided decoding params object. It can also return None if the response format is not supported. (e.g. "text")
        """
        pass


class ResponseFormatText(ResponseFormat):
    type: Literal["text"]

    def to_guided_decoding_params(
        self, backend: str
    ) -> Optional["GuidedDecodingParams"]:
        return None


class JSONSchemaBase(ResponseFormat, ABC):
    @property
    @abstractmethod
    def json_schema_str(self) -> str:
        pass

    @abstractmethod
    def to_dict(self):
        pass


class ResponseFormatJsonObject(JSONSchemaBase):
    model_config = ConfigDict(populate_by_name=True)

    # Support either keywords because it makes it more robust.
    type: Literal["json_object", "json_schema"]
    # Can use `schema` or `json_schema` interchangeably.
    # `schema` is allowed for backwards compatibility
    # (We released docs with `schema` field name)
    json_schema: Optional[Union[Dict[str, Any], str]] = Field(
        default={}, alias="schema", description="Schema for the JSON response format"
    )

    @model_validator(mode="after")
    def read_and_validate_json_schema(self):
        from ray.llm._internal.serve.configs.json_mode_utils import JSONSchemaValidator

        # JSONSchemaValidator is a singleton so the initialization cost is
        # amortized over all the processes's lifetime.
        validator = JSONSchemaValidator()

        # Make sure the json schema is valid and dereferenced.
        self.json_schema = validator.try_load_json_schema(self.json_schema)
        return self

    @property
    def json_schema_str(self) -> str:
        return json.dumps(self.json_schema)

    def to_guided_decoding_params(
        self, backend: str
    ) -> Optional["GuidedDecodingParams"]:
        kwargs = {}

        if self.json_schema:
            kwargs["json"] = self.json_schema_str
        else:
            kwargs["json_object"] = True

        return vllm.sampling_params.GuidedDecodingParams.from_optional(
            backend=backend,
            **kwargs,
        )

    def to_dict(self):
        return {
            "type": self.type,
            "schema": self.json_schema_str,
        }


# TODO(Kourosh): Grammar has this known issue that if there is a syntax error in the grammar
# The engine will die. We need to fix this from vLLM side.
# For now avoiding documenting this approach in the docs.
class ResponseFormatGrammar(ResponseFormat):
    type: Literal["grammar", "grammar_gbnf"]
    grammar: str

    def to_guided_decoding_params(
        self, backend: str
    ) -> Optional["GuidedDecodingParams"]:
        return vllm.sampling_params.GuidedDecodingParams.from_optional(
            backend=backend,
            grammar=self.grammar,
        )


ResponseFormatType = Union[
    ResponseFormatText,
    ResponseFormatGrammar,
    ResponseFormatJsonObject,
]
