# flake8: noqa

# __start_translation_model__
# File name: model.py
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer


class Translator:
    def __init__(self):
        # Load model. t5-small is a text-to-text model that translates when the
        # input is prefixed with the task. (transformers 5 removed the
        # "translation_en_to_fr" pipeline task, so we call the model directly.)
        self.tokenizer = AutoTokenizer.from_pretrained("t5-small")
        self.model = AutoModelForSeq2SeqLM.from_pretrained("t5-small")

    def translate(self, text: str) -> str:
        # Run inference: prefix the input with the task, then generate.
        input_ids = self.tokenizer(
            f"translate English to French: {text}", return_tensors="pt"
        ).input_ids
        output_ids = self.model.generate(
            input_ids, num_beams=4, early_stopping=True, max_new_tokens=40
        )

        # Post-process output to return only the translation text
        translation = self.tokenizer.decode(output_ids[0], skip_special_tokens=True)

        return translation


translator = Translator()

translation = translator.translate("Hello world!")
print(translation)
# __end_translation_model__

# Test model behavior
assert translation == "Bonjour monde!"


# __start_summarization_model__
# File name: summary_model.py
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer


class Summarizer:
    def __init__(self):
        # Load model. t5-small is a text-to-text model that summarizes when the
        # input is prefixed with the task. (transformers 5 removed the
        # "summarization" pipeline task, so we call the model directly.)
        self.tokenizer = AutoTokenizer.from_pretrained("t5-small")
        self.model = AutoModelForSeq2SeqLM.from_pretrained("t5-small")

    def summarize(self, text: str) -> str:
        # Run inference: prefix the input with the task, then generate.
        input_ids = self.tokenizer(f"summarize: {text}", return_tensors="pt").input_ids
        output_ids = self.model.generate(
            input_ids,
            num_beams=4,
            early_stopping=True,
            min_new_tokens=5,
            max_new_tokens=20,
        )

        # Post-process output to return only the summary text
        summary = self.tokenizer.decode(output_ids[0], skip_special_tokens=True)

        return summary


summarizer = Summarizer()

summary = summarizer.summarize(
    "It was the best of times, it was the worst of times, it was the age "
    "of wisdom, it was the age of foolishness, it was the epoch of belief"
)
print(summary)
# __end_summarization_model__

# Test model behavior
expected_summary = (
    "it was the best of times, it was the worst of times, it was the age of wisdom"
)
assert summary == expected_summary
