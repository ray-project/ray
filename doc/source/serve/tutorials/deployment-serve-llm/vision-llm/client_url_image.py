# client_url_image.py
from urllib.parse import urljoin
from openai import OpenAI

API_KEY = "FAKE_KEY"
BASE_URL = "http://localhost:8000"

client = OpenAI(BASE_URL=urljoin(BASE_URL, "v1"), API_KEY=API_KEY)

response = client.chat.completions.create(
    model="my-qwen-VL",
    messages=[
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "What is in this image?"},
                {
                    "type": "image_url",
                    "image_url": {
                        "url": "http://images.cocodataset.org/val2017/000000039769.jpg"
                    },
                },
            ],
        }
    ],
    temperature=0.5,
    stream=True,
)

for chunk in response:
    content = chunk.choices[0].delta.content
    if content:
        print(content, end="", flush=True)
