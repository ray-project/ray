# __sglang_query_start__
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

# Chat completions
print("=== Chat Completions ===")
chat_response = client.chat.completions.create(
    model="qwen3.5-0.8b",
    messages=[
        {"role": "user", "content": "List 3 countries and their capitals."},
    ],
    temperature=0,
    max_tokens=64,
)
print(chat_response.choices[0].message.content)

# Text completions
print("\n=== Text Completions ===")
completion_response = client.completions.create(
    model="qwen3.5-0.8b",
    prompt="San Francisco is a",
    temperature=0,
    max_tokens=30,
)
print(completion_response.choices[0].text)
# __sglang_query_end__
