import requests
import concurrent.futures
import time

input_size = 50


# This is the function each thread will run
def worker(x):
    return requests.post("http://localhost:8000/", data=b"1" * input_size).json()


def send_query(executor, num_queries=30):
    futures = {executor.submit(worker, x) for x in range(30)}
    for future in concurrent.futures.as_completed(futures):
        try:
            future.result()
        except Exception as exc:
            print(f"An exception occurred: {exc}")


# This is your driver function that sets up the thread pool and runs the workers
def main():
    exepcted_qps = 450
    queries_sent = 0

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=30)
    s = time.time()

    while True:
        # Submit the worker function to the executor for each number in range(30)
        send_query(executor, num_queries=30)
        queries_sent += 30

        if queries_sent >= exepcted_qps:
            elapsed = time.time() - s
            print(
                f"{queries_sent} queries Took {elapsed} seconds, "
                f"QPS: {queries_sent / elapsed}"
            )
            if time.time() - s < 1:
                time.sleep(1 - elapsed)
            s = time.time()
            queries_sent = 0


if __name__ == "__main__":
    main()
