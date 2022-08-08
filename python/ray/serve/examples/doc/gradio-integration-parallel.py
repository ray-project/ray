# __doc_import_begin__
import ray
from ray import serve
from ray.serve.gradio_integrations import GradioIngress

import gradio as gr

from transformers import pipeline

# __doc_import_end__


# __doc_models_begin__
@serve.deployment
class TextGenerationModel:
    def __init__(self, model_name):
        self.generator = pipeline("text-generation", model=model_name)

    def __call__(self, text):
        generated_list = self.generator(
            text, do_sample=True, min_length=20, max_length=100
        )
        generated = generated_list[0]["generated_text"]
        return generated


app1 = TextGenerationModel.bind("gpt2")
app2 = TextGenerationModel.bind("EleutherAI/gpt-neo-125M")
# __doc_models_end__


# __doc_gradio_server_begin__
@serve.deployment
class MyGradioServer(GradioIngress):
    def __init__(self, downstream_model_1, downstream_model_2):
        self._d1 = downstream_model_1
        self._d2 = downstream_model_2

        io = gr.Interface(self.fanout, "textbox", "textbox")
        super().__init__(io)

    def fanout(self, text):
        [result1, result2] = ray.get([self._d1.remote(text), self._d2.remote(text)])
        return f"{result1}\n------------\n{result2}"


# __doc_gradio_server_end__


# __doc_app_begin__
app = MyGradioServer.bind(app1, app2)
# __doc_app_end__
