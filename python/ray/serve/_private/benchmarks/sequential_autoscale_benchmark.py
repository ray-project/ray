import threading

import requests

URL = "http://127.0.0.1:8787/v1/completions"  # Change if your deployment uses a different port
NUM_THREADS = 10
REQUESTS_PER_THREAD = 2  # Number of requests each thread will send


def send_requests(thread_id):
    for i in range(REQUESTS_PER_THREAD):
        try:
            response = requests.post(
                URL,
                json={
                    # "model": "/models/my-model",
                    "prompt": "The sky is blue because",
                    "max_tokens": 30,
                    "temperature": 0.5,
                },
            )
            print(f"Thread {thread_id}, Request {i}: {response.text}")
        except Exception as e:
            print(f"Thread {thread_id}, Request {i}: Error: {e}")


threads = []
for t in range(NUM_THREADS):
    thread = threading.Thread(target=send_requests, args=(t,))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

print("All requests completed.")
