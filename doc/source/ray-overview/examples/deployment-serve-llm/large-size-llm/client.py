#client.py
from urllib.parse import urljoin
from openai import OpenAI

api_key = "FAKE_KEY"
base_url = "http://localhost:8000"

client = OpenAI(base_url=urljoin(base_url, "v1"), api_key=api_key)

response = client.chat.completions.create(
    model="my-deepseek-r1",
    messages=[{"role": "user", "content": "Tell me a joke"}],
    stream=True
)

for chunk in response:
    content = chunk.choices[0].delta.content
    if content:
        print(content, end="", flush=True)