# client.py
from urllib.parse import urljoin
from openai import OpenAI

api_key = "FAKE_KEY"
base_url = "http://localhost:8000"

client = OpenAI(base_url=urljoin(base_url, "v1"), api_key=api_key)

# Example: Complex query with thinking process
response = client.chat.completions.create(
    model="my-qwen-3-32b",
    messages=[{"role": "user", "content": "What's the capital of France ?"}],
    extra_body={"chat_template_kwargs": {"enable_thinking": False}},
)

print(f"Reasoning: \n{response.choices[0].message.reasoning_content}\n\n")
print(f"Answer: \n {response.choices[0].message.content}")
