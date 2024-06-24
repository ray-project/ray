# flake8: noqa

# __start_translation_model__
# File name: model.py
from transformers import pipeline


class Translator:
    def __init__(self):
        # Load model
        self.model = pipeline("translation_en_to_fr", model="t5-small")

    def translate(self, text: str) -> str:
        # Run inference
        model_output = self.model(text)

        # Post-process output to return only the translation text
        translation = model_output[0]["translation_text"]

        return translation


translator = Translator()

translation = translator.translate("Hello world!")
print(translation)
# __end_translation_model__

# Test model behavior
assert translation == "Bonjour monde!"


# __start_summarization_model__
# File name: summary_model.py
from transformers import pipeline


class Summarizer:
    def __init__(self):
        # Load model
        self.model = pipeline("summarization", model="t5-small")

    def summarize(self, text: str) -> str:
        # Run inference
        model_output = self.model(text, min_length=5, max_length=15)

        # Post-process output to return only the summary text
        summary = model_output[0]["summary_text"]

        return summary


summarizer = Summarizer()

summary = summarizer.summarize(
    "It was the best of times, it was the worst of times, it was the age "
    "of wisdom, it was the age of foolishness, it was the epoch of belief"
)
print(summary)
# __end_summarization_model__

# Test model behavior
assert summary == "it was the best of times, it was worst of times ."
