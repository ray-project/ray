import ray
from ray import workflow


# Mock method to make a request.
def make_request(url: str) -> str:
    return "42"


@ray.remote
def get_size() -> int:
    return int(make_request("https://www.example.com/callA"))


@ray.remote
def small(result: int) -> str:
    return make_request("https://www.example.com/SmallFunc")


@ray.remote
def medium(result: int) -> str:
    return make_request("https://www.example.com/MediumFunc")


@ray.remote
def large(result: int) -> str:
    return make_request("https://www.example.com/LargeFunc")


@ray.remote
def decide(result: int) -> str:
    if result < 10:
        return workflow.continuation(small.bind(result))
    elif result < 100:
        return workflow.continuation(medium.bind(result))
    else:
        return workflow.continuation(large.bind(result))


if __name__ == "__main__":
    workflow.init()
    print(workflow.create(decide.bind(get_size.bind())).run())
