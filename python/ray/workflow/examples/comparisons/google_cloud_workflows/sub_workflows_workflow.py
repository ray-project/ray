import ray
from ray import workflow


@ray.remote
def hello(name: str) -> str:
    return workflow.continuation(format_name.bind(name))


@ray.remote
def format_name(name: str) -> str:
    return f"hello, {name}"


@ray.remote
def report(msg: str) -> None:
    print(msg)


if __name__ == "__main__":
    r1 = hello.bind("Kristof")
    r2 = report.bind(r1)
    workflow.create(r2).run()
