import ray
from ray import serve
from ray.serve.experimental.gradio import GradioIngress

import gradio as gr


@serve.deployment
class Model:
    def __init__(self, name: str):
        self._name = name

    def __call__(self, inp: str):
        return inp + self._name


@serve.deployment
class MyGradioUI(GradioIngress):
    def __init__(self, downstream_model_1, downstream_model_2):
        self._d1 = downstream_model_1
        self._d2 = downstream_model_2

        io = gr.Interface(self.fanout, "textbox", "textbox")
        super().__init__(io)  # This API is kinda weird?

    def fanout(self, input):
        [result1, result2] = ray.get([self._d1.remote(input), self._d2.remote(input)])
        return f"{result1} | {result2}"


m1 = Model.bind("Alice")
m2 = Model.bind("Bob")
app = MyGradioUI.bind(m1, m2)


# Open questions:
# - Is this running in a thread for every call?
# - Support `async def`?
