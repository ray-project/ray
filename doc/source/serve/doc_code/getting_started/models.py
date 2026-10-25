# flake8: noqa

# __start_translation_model__
# File name: model.py
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer


class Translator:
    def __init__(self):
        # Load model
        self.tokenizer = AutoTokenizer.from_pretrained("t5-small")
        self.model = AutoModelForSeq2SeqLM.from_pretrained("t5-small")

    def translate(self, text: str) -> str:
        # Run inference
        input_ids = self.tokenizer(
            f"translate English to French: {text}", return_tensors="pt"
        ).input_ids
        output_ids = self.model.generate(
            input_ids, num_beams=4, early_stopping=True, max_length=300
        )

        # Post-process output to return only the translation text
        translation = self.tokenizer.decode(
            output_ids[0], skip_special_tokens=True, clean_up_tokenization_spaces=False
        )

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
        # Load model
        self.tokenizer = AutoTokenizer.from_pretrained("t5-small")
        self.model = AutoModelForSeq2SeqLM.from_pretrained("t5-small")

    def summarize(self, text: str) -> str:
        # Run inference
        input_ids = self.tokenizer(f"summarize: {text}", return_tensors="pt").input_ids
        output_ids = self.model.generate(
            input_ids,
            num_beams=4,
            early_stopping=True,
            length_penalty=2.0,
            no_repeat_ngram_size=3,
            min_length=5,
            max_length=15,
        )

        # Post-process output to return only the summary text
        summary = self.tokenizer.decode(
            output_ids[0], skip_special_tokens=True, clean_up_tokenization_spaces=False
        )

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
