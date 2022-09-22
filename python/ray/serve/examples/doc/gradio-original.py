import gradio as gr
from transformers import pipeline

# __doc_code_begin__
generator1 = pipeline("text-generation", model="gpt2")
generator2 = pipeline("text-generation", model="EleutherAI/gpt-neo-125M")


def model1(text):
    generated_list = generator1(text, do_sample=True, min_length=20, max_length=100)
    generated = generated_list[0]["generated_text"]
    return generated


def model2(text):
    generated_list = generator2(text, do_sample=True, min_length=20, max_length=100)
    generated = generated_list[0]["generated_text"]
    return generated


demo = gr.Interface(
    lambda text: f"{model1(text)}\n------------\n{model2(text)}", "textbox", "textbox"
)
demo.launch()
# __doc_code_end__
