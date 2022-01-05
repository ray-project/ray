# __fastapi_start__
# File name: serve_with_fastapi.py
import ray
from ray import serve
from fastapi import FastAPI
from transformers import pipeline

app = FastAPI()

ray.init(address="auto", namespace="serve")
serve.start(detached=True)


@serve.deployment
@serve.ingress(app)
class Summarizer:
    def __init__(self):
        self.summarize = pipeline("summarization", model="t5-small")

    @app.get("/")
    def get_summary(self, txt: str):
        summary_list = self.summarize(txt)
        summary = summary_list[0]["summary_text"]
        return summary

    @app.get("/min10")
    def get_summary_min10(self, txt: str):
        summary_list = self.summarize(txt, min_length=10)
        summary = summary_list[0]["summary_text"]
        return summary

    @app.get("/max10")
    def get_summary_max10(self, txt: str):
        summary_list = self.summarize(txt, max_length=10)
        summary = summary_list[0]["summary_text"]
        return summary


Summarizer.deploy()
# __fastapi_end__
