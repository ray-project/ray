# client_local_image.py
from urllib.parse import urljoin
import base64
from openai import OpenAI

API_KEY = "FAKE_KEY"
BASE_URL = "http://localhost:8000"

client = OpenAI(BASE_URL=urljoin(BASE_URL, "v1"), API_KEY=API_KEY)

### From an image locally saved as `example.jpg`
# Load and encode image as base64
with open("vision-llm/example.jpg", "rb") as f:
    img_base64 = base64.b64encode(f.read()).decode()

response = client.chat.completions.create(
    model="my-qwen-VL",
    messages=[
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "What is in this image?"},
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{img_base64}"},
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
