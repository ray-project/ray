import ray
from ray import workflow


# TODO(ekl) should support something like runtime_env={"pip": ["whalesay"]}
@ray.remote
def hello(msg: str) -> None:
    print(msg)


if __name__ == "__main__":
    workflow.run(hello.bind("hello world"))
