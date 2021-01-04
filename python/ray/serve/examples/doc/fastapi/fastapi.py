from fastapi import FastAPI
from transformers import pipeline  # A simple API for NLP tasks.

app = FastAPI()

nlp_model = pipeline("text-generation", model="gpt2")  # Load the model.


# The function below handles GET requests to the URL `/generate`.
@app.get("/generate")
def generate(query: str):
    return nlp_model(query, max_length=50)  # Output 50 words based on query.
