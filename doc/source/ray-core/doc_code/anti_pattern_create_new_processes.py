# __anti_pattern_start__
import ray
from concurrent.futures import ProcessPoolExecutor, as_completed


@ray.remote
def generate_response(request):
    return "Response to " + request


def process_response(response):
    print(response)
    return "Processed " + response


def main():
    ray.init()
    responses = ray.get([generate_response.remote(f"request {i}") for i in range(4)])

    with ProcessPoolExecutor(max_workers=4) as executor:
        future_to_task = {}
        for idx, response in enumerate(responses):
            future_to_task[executor.submit(process_response, response)] = idx

        for future in as_completed(future_to_task):
            idx = future_to_task[future]
            response_entry = future.result()
            print(f"Response {idx} processed: {response_entry}")


if __name__ == "__main__":
    main()
# __anti_pattern_end__

# __better_approach_start__
import ray


@ray.remote
def generate_response(request):
    return "Response to " + request


@ray.remote
def process_response(response):
    print(response)
    return "Processed " + response


def main():
    ray.init()
    responses = ray.get([generate_response.remote(f"request {i}") for i in range(4)])
    response_results = ray.get(
        [process_response.remote(response) for response in responses]
    )

    for (idx, response_entry) in enumerate(response_results):
        print(f"Response {idx} processed: {response_entry}")


if __name__ == "__main__":
    main()

# __better_approach_end__
