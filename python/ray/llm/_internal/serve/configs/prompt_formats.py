from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Union,
    TYPE_CHECKING,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    PrivateAttr,
    field_validator,
    model_validator,
)
from ray.llm._internal.utils import try_import

if TYPE_CHECKING:
    from transformers import AutoProcessor

transformers = try_import("transformers")


class Text(BaseModel):
    field: str = "text"
    type: str = "text"
    text: str


# Ref: https://huggingface.co/mistral-community/pixtral-12b
#
# Community version of pixtral uses the key `content` instead of `text` in the content.
# This is to support the "content" content type in the prompt format, as opposite of
# the "text" content from the above which most other model uses.
class Content(BaseModel):
    field: str = "text"
    type: str = "text"
    content: str


class Image(BaseModel):
    field: str = "image_url"
    image_url: Dict

    @field_validator("image_url")
    @classmethod
    def check_image_url(cls, value):
        if "url" not in value or not value["url"] or not isinstance(value["url"], str):
            raise ValueError(
                # TODO(xwjiang): Link to doc.
                "Expecting 'url' string to be provided under 'image_url' dict."
            )
        return value


ContentList = List[Union[Image, Text, Content]]


class Message(BaseModel):
    role: Literal["system", "assistant", "user"]
    content: Optional[Union[str, ContentList]] = None

    def __str__(self):
        return self.model_dump_json()

    @model_validator(mode="after")
    def check_fields(self):
        if self.role == "system":
            if not isinstance(self.content, str):
                raise ValueError("System content must be a string")
        if self.role == "user" and self.content is None:
            raise ValueError("User content must not be None.")
        if self.role == "assistant":
            # passing a regular assistant message
            if self.content is not None and not isinstance(self.content, str):
                raise ValueError("content must be a string or None")
        return self


class Prompt(BaseModel):
    prompt: Union[str, List[Message]]
    use_prompt_format: bool = True
    parameters: Optional[Dict[str, Any]] = None

    @field_validator("parameters", mode="before")
    @classmethod
    def parse_parameters(cls, value):
        if isinstance(value, BaseModel):
            # Use exclude_unset so that we can distinguish unset values from default values
            return value.model_dump(exclude_unset=True)
        return value

    @field_validator("prompt")
    @classmethod
    def check_prompt(cls, value):
        if isinstance(value, list) and not value:
            raise ValueError("Messages cannot be an empty list.")
        return value

    def to_unformatted_string(self) -> str:
        if isinstance(self.prompt, list):
            return ", ".join(str(message.content) for message in self.prompt)
        return self.prompt


class ImageInput(BaseModel):
    """Prompt output that contains image info."""

    image_url: str


class EngineInput(BaseModel):
    """Input to the engine.

    Which is also output from `PromptFormat.generate_prompt()`."""

    text: str
    image: Optional[List[ImageInput]] = None


class AbstractPromptFormat(BaseModel):
    model_config = ConfigDict(extra="forbid")

    def generate_prompt(self, messages: Union[Prompt, List[Message]]) -> EngineInput:
        raise NotImplementedError()


class HuggingFacePromptFormat(AbstractPromptFormat):
    _processor: "AutoProcessor" = PrivateAttr()

    def set_processor(self, model_id_or_path: str, trust_remote_code: bool = False):
        if hasattr(self, "_processor"):
            return

        self._processor = transformers.AutoProcessor.from_pretrained(
            model_id_or_path,
            trust_remote_code=trust_remote_code,
        )

    def generate_prompt(
        self, messages: Union[Prompt, List[Message], dict, List[dict]]
    ) -> EngineInput:
        # Normalize to Prompt if the input is a dict
        if isinstance(messages, dict):
            messages = Prompt.model_validate(messages)

        # Normalize to List[Message] if the input is a Prompt object
        if isinstance(messages, Prompt):
            if isinstance(messages.prompt, str):
                if not messages.use_prompt_format:
                    return EngineInput(text=messages.prompt)
                raise ValueError("String prompts are not supported.")
            messages = messages.prompt

        # If messages is a list, ensure all elements are of the same type and convert List[dict]to List[Message]
        elif isinstance(messages, list):
            if messages == []:
                raise ValueError("List cannot be empty.")
            elif all(isinstance(msg, dict) for msg in messages):
                messages = [Message.model_validate(msg) for msg in messages]
            elif all(isinstance(msg, Message) for msg in messages):
                pass
            else:
                raise ValueError(
                    "List must contain either all dicts or all Message objects."
                )

        assert hasattr(
            self, "_processor"
        ), "HuggingFacePromptFormat's processor is not set."

        conversation = []
        images = []
        for message in messages:
            content = []
            if isinstance(message.content, list):
                for c in message.content:
                    if isinstance(c, (Text, Content)):
                        content.append(c.model_dump())
                    elif isinstance(c, Image):
                        content.append({"type": "image"})
                        images.append(ImageInput(image_url=c.image_url["url"]))
            else:
                content = message.content
            conversation.append({"role": message.role, "content": content})

        prompt = self._processor.apply_chat_template(
            conversation=conversation,
            tokenize=False,
            add_generation_prompt=True,
        )

        return EngineInput(text=prompt, image=images)
