from ray.llm._internal.serve.configs.openai_api_models import DeltaMessage


def test_delta_message_null_content():
    """Test that the DeltaMessage class is correctly constructed.

    When the content is passed as None, it should be set to an empty string.
    """
    role = "user"
    delta_message_implicitly_null_content = DeltaMessage(
        role=role,
    )

    delta_message_explicitly_null_content = DeltaMessage(
        role=role,
        content=None,
    )

    delta_message_empty_string_content = DeltaMessage(
        role=role,
        content="",
    )

    assert delta_message_implicitly_null_content.role == role
    assert delta_message_explicitly_null_content.role == role
    assert delta_message_empty_string_content.role == role
    assert delta_message_implicitly_null_content.content == ""
    assert delta_message_explicitly_null_content.content == ""
    assert delta_message_empty_string_content.content == ""
