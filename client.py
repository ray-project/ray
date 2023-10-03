import requests
import time
import io

def send_serve_requests():
    request_meta = {
        "request_type": "InvokeEndpoint",
        "name": "Streamtest",
        "start_time": time.time(),
        "response_length": 0,
        "response": None,
        "context": {},
        "exception": None,
    }
    start_perf_counter = time.perf_counter()
    #r = self.client.get("/", stream=True)
    r = requests.get("http://localhost:8000", stream=True)
    if r.status_code != 200:
        print(r)
    else:
        for i, chunk in enumerate(r.iter_content(chunk_size=None, decode_unicode=True)):
            pass
        request_meta["response_time"] = (
            time.perf_counter() - start_perf_counter
        ) * 1000
        # events.request.fire(**request_meta)

from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=100) as executor:
    while True:
        futs = [executor.submit(send_serve_requests) for _ in range(100)]
        for f in futs:
            f.result()

