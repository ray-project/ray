import threading
import requests

URL = "http://localhost:8081/"  # Change if your deployment uses a different port
NUM_THREADS = 100
REQUESTS_PER_THREAD = 10  # Number of requests each thread will send

def send_requests(thread_id):
    for i in range(REQUESTS_PER_THREAD):
        try:
            response = requests.get(URL)
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