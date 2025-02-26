import pytest
import sys


class TestOpenAICompatibilityNoAcceleratorType:
    """Test that rayllm is compatible with OpenAI API without specifying accelerator_type"""

    def test_models_no_accelerator_type(
        self, testing_model_no_accelerator
    ):  # noqa: F811
        """Check model listing without accelerator_type"""
        client, model = testing_model_no_accelerator
        models = client.models.list()
        assert len(models.data) == 1, "Only the test model should be returned"
        assert models.data[0].id == model, "The test model id should match"

    def test_completions_no_accelerator_type(
        self, testing_model_no_accelerator
    ):  # noqa: F811
        """Check completions without accelerator_type"""
        client, model = testing_model_no_accelerator
        completion = client.completions.create(
            model=model,
            prompt="Hello world",
            max_tokens=2,
        )
        assert completion.model == model
        assert completion.model
        assert completion.choices[0].text == "test_0 test_1 "

    def test_chat_no_accelerator_type(self, testing_model_no_accelerator):  # noqa: F811
        """Check chat completions without accelerator_type"""
        client, model = testing_model_no_accelerator
        chat_completion = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "Hello world"}],
        )
        assert chat_completion
        assert chat_completion.usage
        assert chat_completion.id
        assert isinstance(chat_completion.choices, list)
        assert chat_completion.choices[0].message.content


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
