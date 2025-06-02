import sys

import openai
import pytest

from ray.llm._internal.serve.configs.constants import (
    MAX_NUM_TOPLOGPROBS_ALLOWED,
    MIN_NUM_TOPLOGPROBS_ALLOWED,
)


class TestOpenAICompatibility:
    """Test that the rayllm are compatible with the OpenAI API"""

    def test_models(self, testing_model):  # noqa: F811
        client, model = testing_model
        models = client.models.list()
        assert len(models.data) == 1, "Only the test model should be returned"
        assert models.data[0].id == model, "The test model id should match"
        assert models.data[0].rayllm_metadata["input_modality"] == "text"

    def test_completions(self, testing_model):  # noqa: F811
        client, model = testing_model
        completion = client.completions.create(
            model=model,
            prompt="Hello world",
            max_tokens=2,
        )
        assert completion.model == model
        assert completion.model
        assert completion.choices[0].text == "test_0 test_1 "

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

    def test_chat_logprobs(self, testing_model):
        client, model = testing_model
        num_tokens = 5
        # test logprobs for non-streaming chat completions
        for top_logprobs in range(5):
            chat_completion = client.chat.completions.create(
                model=model,
                max_tokens=num_tokens,
                messages=[{"role": "user", "content": "Hello world"}],
                logprobs=True,
                top_logprobs=top_logprobs,
            )
            logprobs = chat_completion.choices[0].logprobs.content
            assert logprobs, "Logprobs should be not be None or Empty"
            assert len(logprobs) == num_tokens
            assert all(
                len(logprob.top_logprobs) == top_logprobs for logprob in logprobs
            )
            text_from_logprobs = []
            for logprob in logprobs:
                text_from_logprobs.append(logprob.token)
                if logprob.top_logprobs:
                    assert logprob.token == logprob.top_logprobs[0].token
            text_from_logprobs = "".join(text_from_logprobs)
            assert (
                text_from_logprobs == chat_completion.choices[0].message.content
            ), "Text from logprobs should match text from completion"

        for num_top_logprobs in range(5):
            chat_completion = client.chat.completions.create(
                model=model,
                max_tokens=num_tokens,
                messages=[{"role": "user", "content": "Hello world"}],
                logprobs=True,
                top_logprobs=num_top_logprobs,
                stream=True,
            )

            for c in chat_completion:
                choice_logprobs = c.choices[0].logprobs
                if choice_logprobs and choice_logprobs.content:
                    for chat_completion_token_logprob in choice_logprobs.content:
                        top_logprobs_res = chat_completion_token_logprob.top_logprobs
                        assert len(top_logprobs_res) == num_top_logprobs
                        if top_logprobs_res:
                            assert (
                                top_logprobs_res[0].token
                                == chat_completion_token_logprob.token
                            )

        # try to send logprobs request with invalid number of toplogprobs
        with pytest.raises(openai.BadRequestError):
            for top_logprobs in [
                MAX_NUM_TOPLOGPROBS_ALLOWED + 1,
                MIN_NUM_TOPLOGPROBS_ALLOWED - 1,
            ]:
                client.chat.completions.create(
                    model=model,
                    max_tokens=num_tokens,
                    messages=[{"role": "user", "content": "Hello world"}],
                    logprobs=True,
                    top_logprobs=top_logprobs,
                )

    def test_completions_bad_request(self, testing_model):  # noqa: F811
        client, model = testing_model
        with pytest.raises(openai.BadRequestError) as exc_info:
            client.completions.create(
                model=model,
                prompt="Hello world",
                temperature=-0.1,
            )
        assert "temperature" in str(exc_info.value)

    def test_chat_bad_request(self, testing_model):  # noqa: F811
        client, model = testing_model
        with pytest.raises(openai.BadRequestError) as exc_info:
            client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": "Hello world"}],
                temperature=-0.1,
            )
        assert "temperature" in str(exc_info.value)

        with pytest.raises(openai.BadRequestError) as exc_info:
            client.chat.completions.create(
                model=model,
                messages=[],
            )
        assert "least 1 item" in str(exc_info.value)

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
            temperature=0.4,
            frequency_penalty=0.02,
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
        assert chat_completion
        assert chat_completion.id
        assert isinstance(chat_completion.choices, list)
        assert not chat_completion.choices[0].delta.content
        assert chat_completion.choices[0].finish_reason
        assert i > 4

    def test_completions_stream_bad_request(self, testing_model):  # noqa: F811
        client, model = testing_model
        with pytest.raises(openai.BadRequestError) as exc_info:
            for _ in client.completions.create(
                model=model,
                prompt="Hello world",
                stream=True,
                temperature=-0.1,
            ):
                pass
        assert "temperature" in str(exc_info.value)

    def test_chat_stream_bad_request(self, testing_model):  # noqa: F811
        client, model = testing_model
        with pytest.raises(openai.BadRequestError) as exc_info:
            for _chat_completion in client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": "Hello world"}],
                stream=True,
                temperature=-0.1,
            ):
                pass
        assert "temperature" in str(exc_info.value)

        with pytest.raises(openai.BadRequestError) as exc_info:
            for _chat_completion in client.chat.completions.create(
                model=model,
                messages=[],
                stream=True,
            ):
                pass
        assert "least 1 item" in str(exc_info.value)

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
