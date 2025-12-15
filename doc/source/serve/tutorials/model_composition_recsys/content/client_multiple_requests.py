# client_multiple_requests.py
import requests
import random

# Generate sample candidate items
all_items = [f"item_{i}" for i in range(1000)]

# Test with multiple users
for i in range(100):
    user_id = f"user_{random.randint(1, 1000)}"
    # Each user gets a random subset of candidate items
    candidate_items = random.sample(all_items, k=50)
    
    response = requests.post(
        "http://localhost:8000",
        json={
            "user_id": user_id,
            "candidate_items": candidate_items,
            "top_k": 3
        }
    )
    
    result = response.json()
    top_items = [rec["item_id"] for rec in result["recommendations"]]
    print(f"Request {i+1} - {user_id}: {top_items}")

print(f"\nSent 100 requests total")
