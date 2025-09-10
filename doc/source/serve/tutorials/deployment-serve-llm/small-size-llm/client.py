from urllib.parse import urljoin
from openai import OpenAI

API_KEY = "FAKE_KEY"
BASE_URL = "http://localhost:8000"

client = OpenAI(BASE_URL=urljoin(BASE_URL, "v1"), API_KEY=API_KEY)

response = client.chat.completions.create(
    model="my-llama-3.1-8b",
    messages=[{"role": "user", "content": "Tell me a joke"}],
    stream=True,
)

for chunk in response:
    content = chunk.choices[0].delta.content
    if content:
        print(content, end="", flush=True)
