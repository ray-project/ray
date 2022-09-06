# __doc_import_begin__
import requests
from transformers import pipeline
from io import BytesIO
from PIL import Image, ImageFile
from typing import Dict

from ray import serve
from ray.dag.input_node import InputNode
from ray.serve.drivers import DAGDriver

# __doc_import_end__


# __doc_downloader_begin__
@serve.deployment
def downloader(image_url: str) -> ImageFile.ImageFile:
    image_bytes = requests.get(image_url).content
    image = Image.open(BytesIO(image_bytes)).convert("RGB")
    return image
    # __doc_downloader_end__


# __doc_classifier_begin__
@serve.deployment
class ImageClassifier:
    def __init__(self):
        self.model = pipeline(
            "image-classification", model="google/vit-base-patch16-224"
        )

    def classify(self, image: ImageFile.ImageFile) -> Dict[str, float]:
        results = self.model(image)
        return {pred["label"]: pred["score"] for pred in results}
        # __doc_classifier_end__


# __doc_translator_begin__
@serve.deployment
class Translator:
    def __init__(self):
        self.model = pipeline("translation_en_to_de", model="t5-small")

    def translate(self, dict: Dict[str, float]) -> Dict[str, float]:
        results = {}
        for label, score in dict.items():
            translated_label = self.model(label)[0]["translation_text"]
            results[translated_label] = score

        return results
        # __doc_translator_end__


# __doc_build_graph_begin__
with InputNode(input_type=str) as user_input:
    classifier = ImageClassifier.bind()
    translator = Translator.bind()

    downloaded_image = downloader.bind(user_input)
    classes = classifier.classify.bind(downloaded_image)
    translated_classes = translator.translate.bind(classes)

    serve_entrypoint = DAGDriver.bind(translated_classes)
    # __doc_build_graph_end__
