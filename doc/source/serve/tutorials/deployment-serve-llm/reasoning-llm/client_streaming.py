# client_streaming.py
from urllib.parse import urljoin
from openai import OpenAI

API_KEY = "FAKE_KEY"
BASE_URL = "http://localhost:8000"

client = OpenAI(BASE_URL=urljoin(BASE_URL, "v1"), API_KEY=API_KEY)

# Example: Complex query with thinking process
response = client.chat.completions.create(
    model="my-qwq-32B",
    messages=[
        {
            "role": "user",
            "content": "I need to plan a trip to Paris from Seattle. Can you help me research flight costs, create an itinerary for 3 days, and suggest restaurants based on my dietary restrictions (vegetarian)?",
        }
    ],
    stream=True,
)

# Stream
for chunk in response:
    # Stream reasoning content
    if hasattr(chunk.choices[0].delta, "reasoning_content"):
        data_reasoning = chunk.choices[0].delta.reasoning_content
        if data_reasoning:
            print(data_reasoning, end="", flush=True)
    # Later, stream the final answer
    if hasattr(chunk.choices[0].delta, "content"):
        data_content = chunk.choices[0].delta.content
        if data_content:
            print(data_content, end="", flush=True)
