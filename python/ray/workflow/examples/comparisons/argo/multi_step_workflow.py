import ray
from ray import workflow


@ray.remote
def hello(msg: str, *deps) -> None:
    print(msg)


@ray.remote
def wait_all(*args) -> None:
    pass


if __name__ == "__main__":
    h1 = hello.options(**workflow.options(name="hello1")).bind("hello1")
    h2a = hello.options(**workflow.options(name="hello2a")).bind("hello2a")
    h2b = hello.options(**workflow.options(name="hello2b")).bind("hello2b", h2a)
    workflow.create(wait_all.bind(h1, h2b)).run()
