# client.py
from urllib.parse import urljoin
from openai import OpenAI

API_KEY = "FAKE_KEY"
BASE_URL = "http://localhost:8000"

client = OpenAI(BASE_URL=urljoin(BASE_URL, "v1"), API_KEY=API_KEY)

response = client.chat.completions.create(
    model="my-qwq-32B",
    messages=[
        {
            "role": "user",
            "content": "What is the sum of all even numbers between 1 and 100?",
        }
    ],
)

print(f"Reasoning: \n{response.choices[0].message.reasoning_content}\n\n")
print(f"Answer: \n {response.choices[0].message.content}")
