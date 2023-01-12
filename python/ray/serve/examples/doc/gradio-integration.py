# __doc_import_begin__
from ray.serve.gradio_integrations import GradioServer

import gradio as gr

from transformers import pipeline

# __doc_import_end__


# __doc_gradio_app_begin__
def gradio_summarizer_builder():
    summarizer = pipeline("summarization", model="t5-small")

    def model(text):
        summary_list = summarizer(text)
        summary = summary_list[0]["summary_text"]
        return summary

    example = (
        "HOUSTON -- Men have landed and walked on the moon. "
        "Two Americans, astronauts of Apollo 11, steered their fragile "
        "four-legged lunar module safely and smoothly to the historic landing "
        "yesterday at 4:17:40 P.M., Eastern daylight time. Neil A. Armstrong, the "
        "38-year-old commander, radioed to earth and the mission control room "
        'here: "Houston, Tranquility Base here. The Eagle has landed." The '
        "first men to reach the moon -- Armstrong and his co-pilot, Col. Edwin E. "
        "Aldrin Jr. of the Air Force -- brought their ship to rest on a level, "
        "rock-strewn plain near the southwestern shore of the arid Sea of "
        "Tranquility. About six and a half hours later, Armstrong opened the "
        "landing craft's hatch, stepped slowly down the ladder and declared as "
        "he planted the first human footprint on the lunar crust: \"That's one "
        'small step for man, one giant leap for mankind." His first step on the '
        "moon came at 10:56:20 P.M., as a television camera outside the craft "
        "transmitted his every move to an awed and excited audience of hundreds "
        "of millions of people on earth."
    )

    return gr.Interface(
        fn=model,
        inputs=[gr.inputs.Textbox(default=example, label="Input prompt")],
        outputs=[gr.outputs.Textbox(label="Model output")],
    )
    # __doc_gradio_app_end__


# __doc_app_begin__
app = GradioServer.options(num_replicas=2, ray_actor_options={"num_cpus": 4}).bind(
    gradio_summarizer_builder
)
# __doc_app_end__
