from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

from fastapi import HTTPException, status
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    field_validator,
    model_validator,
)
from transformers import AutoProcessor


class TextContent(BaseModel):
    field: str = "text"
    type: str = "text"
    text: str


# Ref: https://huggingface.co/mistral-community/pixtral-12b
#
# Community version of pixtral uses the key `content` instead of `text` in the content.
# This is to support the "content" content type in the prompt format, as opposite of
# the "text" content from the above which most other model uses.
class ContentContent(BaseModel):
    field: str = "text"
    type: str = "text"
    content: str


class ImageContent(BaseModel):
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


ContentList = List[Union[ImageContent, TextContent, ContentContent]]


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


class PromptFormat(AbstractPromptFormat):
    system: str = Field(
        description="The template for system message. It should include `{instruction}` template.",
    )
    assistant: str = Field(
        description="The template for the assistant message. This is used when "
        "the input list of messages includes assistant messages. The content "
        "of those messages is reformatted with this template. It should "
        "include `{instruction}` template.",
    )
    trailing_assistant: str = Field(
        default="",
        description="(Inference only) The string that should be appended to the end of the text before sending it to the model for completion at inference time. This is not used during fine-tuning.",
    )
    user: str = Field(
        description="The template for the user message. It should include `{instruction}` template. If `system_in_user` is set to True, it should also include `{system}` template.",
    )
    bos: str = Field(
        default="",
        description="The string that should be prepended to the text before sending it to the model for completion. Defaults to empty string",
    )

    default_system_message: str = Field(
        default="",
        description="The default system message that should be included in the prompt if no system message is provided in the input list of messages. If not specified, this is an empty string",
    )
    add_system_tags_even_if_message_is_empty: bool = Field(
        default=False,
        description="If True, the system message will be included in the prompt even if the content of the system message is empty.",
    )
    system_in_user: bool = Field(
        default=False,
        description="If True, the system message will be included in the user message.",
    )
    system_in_last_user: bool = Field(
        default=False,
        description="(Inference only) If True, the system message will be included in the last user message. Otherwise, it will be included in the first user message. This is not used during fine-tuning.",
    )

    strip_whitespace: bool = Field(
        default=True,
        description="If True, the whitespace in the content of the messages will be stripped.",
    )

    @field_validator("system")
    @classmethod
    def check_system(cls, value):
        assert value and (
            "{instruction}" in value
        ), "system must be a string containing '{instruction}'"
        return value

    @field_validator("assistant")
    @classmethod
    def check_assistant(cls, value):
        assert (
            value and "{instruction}" in value
        ), "assistant must be a string containing '{instruction}'"
        return value

    @field_validator("user")
    @classmethod
    def check_user(cls, value):
        assert value and (
            "{instruction}" in value
        ), "user must be a string containing '{instruction}'"
        return value

    @model_validator(mode="after")
    def check_user_system_in_user(self):
        if self.system_in_user:
            assert (
                "{system}" in self.user
            ), "If system_in_user=True, user must contain '{system}'"
        return self

    @model_validator(mode="after")
    def check_system_in_last_user(self):
        assert self.system_in_user or not self.system_in_last_user, (
            "If system is not included in the user message, "
            "system_in_last_user cannot be set to False."
        )
        return self

    def _parse_system_message(self, messages: List[Message]) -> Optional[Message]:
        # Get system message
        system_message_index = -1
        for i, message in enumerate(messages):
            if message.role == "system":
                if system_message_index == -1:
                    system_message_index = i
                else:
                    raise HTTPException(
                        status.HTTP_400_BAD_REQUEST,
                        "Only one system message can be specified.",
                    )

        system_message = None
        if system_message_index != -1:
            system_message = messages.pop(system_message_index)
        elif (
            self.default_system_message or self.add_system_tags_even_if_message_is_empty
        ):
            system_message = Message(role="system", content=self.default_system_message)
        if (
            system_message is not None
            and (
                system_message.content or self.add_system_tags_even_if_message_is_empty
            )
            and not self.system_in_user
        ):
            messages.insert(0, system_message)

        return system_message

    def create_user_prompt(
        self,
        message: Message,
        system_message: Optional[Message],
        is_first: bool,
        is_last: bool,
    ) -> str:
        usr_msg_kwargs = {"instruction": message.content}
        if self.system_in_user:
            system = ""
            if system_message is not None:
                if (is_last and self.system_in_last_user) or (
                    is_first and not self.system_in_last_user
                ):
                    system = self.system.format(instruction=system_message.content)
            usr_msg_kwargs["system"] = system

        return self.user.format(**usr_msg_kwargs)

    def create_system_prompt(self, message: Message) -> str:
        return self.system.format(instruction=message.content)

    def create_assistant_message(self, message: Message) -> str:
        return self.assistant.format(instruction=message.content)

    def _maybe_strip_whitespaces(self, message: Message) -> None:
        """Message object is modified in-place if strip is set."""
        message_content = message.content
        if not isinstance(message_content, str):
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "Content needs to be a string",
            )
        if self.strip_whitespace:
            message.content = message_content.strip()

    def _get_usr_msg_inds(self, messages: List[Message]) -> Tuple[int, int]:
        last_user_idx = -1
        first_user_idx = -1
        for i, msg in enumerate(messages):
            if msg.role == "user":
                if first_user_idx == -1:
                    first_user_idx = i
                last_user_idx = i
        return first_user_idx, last_user_idx

    def _prepare_messages(
        self, messages: Union[Prompt, List[Message]]
    ) -> List[Message]:
        if isinstance(messages, Prompt):
            if isinstance(messages.prompt, str):
                new_messages = []
                if self.default_system_message:
                    new_messages.append(
                        Message(role="system", content=self.default_system_message),
                    )
                new_messages.append(
                    Message(role="user", content=messages.prompt),
                )
                messages = new_messages
            else:
                messages = messages.prompt

        return messages

    def generate_prompt(self, messages: Union[Prompt, List[Message]]) -> EngineInput:
        if isinstance(messages, Prompt):
            if isinstance(messages.prompt, str):
                if not messages.use_prompt_format:
                    return EngineInput(text=self.bos + messages.prompt)

        messages = self._prepare_messages(messages)
        system_message = self._parse_system_message(messages)

        first_user_msg_idx, last_user_msg_idx = self._get_usr_msg_inds(messages)

        prompt = []
        msg_idx = 0
        while msg_idx < len(messages):
            message = messages[msg_idx]
            self._maybe_strip_whitespaces(message)

            if message.role == "user":
                prompt.append(
                    self.create_user_prompt(
                        message=message,
                        system_message=system_message,
                        is_last=(msg_idx == last_user_msg_idx),
                        is_first=(msg_idx == first_user_msg_idx),
                    )
                )
            elif message.role == "system":
                prompt.append(self.create_system_prompt(message))
            elif message.role == "assistant":
                prompt.append(self.create_assistant_message(message))
            msg_idx += 1

        prompt.append(self.trailing_assistant)
        text = self.bos + "".join(prompt)
        return EngineInput(text=text)


class HuggingFacePromptFormat(AbstractPromptFormat):
    use_hugging_face_chat_template: Literal[True]
    _processor: AutoProcessor = PrivateAttr()

    def set_processor(self, model_id_or_path: str):
        if hasattr(self, "_processor"):
            return

        self._processor = AutoProcessor.from_pretrained(model_id_or_path)

    def generate_prompt(self, messages: Union[Prompt, List[Message]]) -> EngineInput:
        assert hasattr(
            self, "_processor"
        ), "HuggingFacePromptFormat's processor is not set."

        if isinstance(messages, Prompt):
            if isinstance(messages.prompt, str):
                raise ValueError("String prompts are not supported.")
            messages = messages.prompt

        conversation = []
        images = []
        for message in messages:
            content = []
            if isinstance(message.content, list):
                for c in message.content:
                    if isinstance(c, (TextContent, ContentContent)):
                        content.append(c.model_dump())
                    elif isinstance(c, ImageContent):
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
