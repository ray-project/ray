from ray import workflow


# Mock method to make a request.
def make_request(url: str) -> str:
    return "42"


@workflow.step
def get_size() -> int:
    return int(make_request("https://www.example.com/callA"))


@workflow.step
def small(result: int) -> str:
    return make_request("https://www.example.com/SmallFunc")


@workflow.step
def medium(result: int) -> str:
    return make_request("https://www.example.com/MediumFunc")


@workflow.step
def large(result: int) -> str:
    return make_request("https://www.example.com/LargeFunc")


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
    print(decide.step(get_size.step()).run())
