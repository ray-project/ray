# client.py
import requests

response = requests.post(
    "http://localhost:8000",
    json={
        "user_id": "user_42",
        "top_k": 5
    }
)

print(response.json())
