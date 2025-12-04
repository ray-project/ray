# client.py
import requests

response = requests.post(
    "http://localhost:8000",
    json={
        "user_id": "user_42",
        "candidate_items": [
            "item_101", "item_102", "item_103", 
            "item_104", "item_105", "item_106",
            "item_107", "item_108", "item_109", "item_110"
        ],
        "top_k": 5
    }
)

print(response.json())
