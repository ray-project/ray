# __start__
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
# __end__

assert translation == "Bonjour monde!"
