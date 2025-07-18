import sys

import openai
import pytest


class TestOpenAICompatibility:
    """Test that the rayllm are compatible with the OpenAI API"""

    def test_models(self, testing_model):  # noqa: F811
        client, model = testing_model
        models = client.models.list()
        assert len(models.data) == 1, "Only the test model should be returned"
        assert models.data[0].id == model, "The test model id should match"
        assert models.data[0].metadata["input_modality"] == "text"

    def test_completions(self, testing_model):  # noqa: F811
        client, model = testing_model
        completion = client.completions.create(
            model=model,
            prompt="Hello world",
            max_tokens=2,
        )
        assert completion.model == model
        assert completion.model
        assert completion.choices[0].text == "test_0 test_1"

    def test_chat(self, testing_model):  # noqa: F811
        client, model = testing_model
        # create a chat completion
        chat_completion = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "Hello world"}],
        )
        assert chat_completion
        assert chat_completion.usage
        assert chat_completion.id
        assert isinstance(chat_completion.choices, list)
        assert chat_completion.choices[0].message.content

    def test_completions_missing_model(self, testing_model):  # noqa: F811
        client, _ = testing_model
        with pytest.raises(openai.NotFoundError) as exc_info:
            client.completions.create(
                model="notarealmodel",
                prompt="Hello world",
            )
        assert "Could not find" in str(exc_info.value)

    def test_chat_missing_model(self, testing_model):  # noqa: F811
        client, _ = testing_model
        with pytest.raises(openai.NotFoundError) as exc_info:
            client.chat.completions.create(
                model="notarealmodel",
                messages=[{"role": "user", "content": "Hello world"}],
            )
        assert "Could not find" in str(exc_info.value)

    def test_completions_stream(self, testing_model):  # noqa: F811
        client, model = testing_model
        i = 0
        for completion in client.completions.create(
            model=model,
            prompt="Hello world",
            stream=True,
        ):
            i += 1
            assert completion
            assert completion.id
            assert isinstance(completion.choices, list)
            assert isinstance(completion.choices[0].text, str)
        assert i > 4

    def test_chat_stream(self, testing_model):  # noqa: F811
        client, model = testing_model
        i = 0
        for chat_completion in client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "Hello world"}],
            stream=True,
            stream_options=dict(
                include_usage=True,
            ),
            temperature=0.4,
            frequency_penalty=0.02,
            max_tokens=5,
        ):
            if i == 0:
                assert chat_completion
                assert chat_completion.id
                assert isinstance(chat_completion.choices, list)
                assert chat_completion.choices[0].delta.role
            else:
                assert chat_completion
                assert chat_completion.id
                assert isinstance(chat_completion.choices, list)
                assert chat_completion.choices[0].delta == {} or hasattr(
                    chat_completion.choices[0].delta, "content"
                )
            i += 1

    def test_completions_stream_missing_model(self, testing_model):  # noqa: F811
        client, _ = testing_model
        with pytest.raises(openai.NotFoundError) as exc_info:
            for _chat_completion in client.completions.create(
                model="notarealmodel",
                prompt="Hello world",
                stream=True,
            ):
                pass
        assert "Could not find" in str(exc_info.value)

    def test_chat_stream_missing_model(self, testing_model):  # noqa: F811
        client, _ = testing_model
        with pytest.raises(openai.NotFoundError) as exc_info:
            for _chat_completion in client.chat.completions.create(
                model="notarealmodel",
                messages=[{"role": "user", "content": "Hello world"}],
                stream=True,
            ):
                pass
        assert "Could not find" in str(exc_info.value)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
