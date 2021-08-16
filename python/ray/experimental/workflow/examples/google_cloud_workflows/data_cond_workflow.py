import requests

from ray import workflow


@workflow.step
def make_request() -> int:
    return int(requests.get("https://www.example.com/callA").text)


@workflow.step
def small() -> str:
    return requests.get("https://www.example.com/SmallFunc").text


@workflow.step
def medium() -> str:
    return requests.get("https://www.example.com/MediumFunc").text


@workflow.step
def large() -> str:
    return requests.get("https://www.example.com/LargeFunc").text


@workflow.step
def decide(result: int) -> str:
    if result < 10:
        return small.step(result)
    elif result < 100:
        return medium.step(result)
    else:
        return large.step(result)


if __name__ == "__main__":
    workflow.init()
    print(decide.step(make_request.step()).run())
