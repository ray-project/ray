from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Union,
)

from pydantic import (
    BaseModel,
    field_validator,
    model_validator,
)

from ray.llm._internal.common.utils.import_utils import try_import

transformers = try_import("transformers")


class Text(BaseModel):
    type: str = "text"
    text: str


# Ref: https://huggingface.co/mistral-community/pixtral-12b
#
# Community version of pixtral uses the key `content` instead of `text` in the content.
# This is to support the "content" content type in the prompt format, as opposite of
# the "text" content from the above which most other model uses.
class Content(BaseModel):
    type: str = "text"
    content: str


class Image(BaseModel):
    type: str = "image_url"
    image_url: Dict

    @field_validator("image_url")
    @classmethod
    def check_image_url(cls, value):
        """Checks if the image_url is a dict with a 'url' key.
        Example:
            image_url = {
                "url": "https://example.com/image.png"
            }
        """
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
