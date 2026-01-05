# client_multiple_requests.py
import requests
import random

# Test with multiple users
for i in range(100):
    user_id = f"user_{random.randint(1, 1000)}"
    
    response = requests.post(
        "http://localhost:8000",
        json={
            "user_id": user_id,
            "top_k": 3
        }
    )
    
    result = response.json()
    top_items = [rec["item_id"] for rec in result["recommendations"]]
    print(f"Request {i+1} - {user_id}: {top_items}")

print(f"\nSent 100 requests total")
