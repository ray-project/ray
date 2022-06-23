import ray
from ray import workflow


@ray.remote
def hello(msg: str) -> None:
    print(msg)


@ray.remote
def wait_all(*args) -> None:
    pass


if __name__ == "__main__":
    children = []
    for msg in ["hello world", "goodbye world"]:
        children.append(hello.bind(msg))
    workflow.create(wait_all.bind(*children)).run()
