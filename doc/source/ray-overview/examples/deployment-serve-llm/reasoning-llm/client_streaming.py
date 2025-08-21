#client_streaming.py
from urllib.parse import urljoin
from openai import OpenAI

api_key = <YOUR-TOKEN-HERE>
base_url = <YOUR-ENDPOINT-HERE>

client = OpenAI(base_url=urljoin(base_url, "v1"), api_key=api_key)

# Example: Complex query with thinking process
response = client.chat.completions.create(
    model="my-qwq-32B",
    messages=[
        {"role": "user", "content": "I need to plan a trip to Paris from Seattle. Can you help me research flight costs, create an itinerary for 3 days, and suggest restaurants based on my dietary restrictions (vegetarian)?"}
    ],
    stream=True
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