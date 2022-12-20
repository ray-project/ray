import ray
from ray import workflow


@ray.remote
def hello(msg: str, *deps) -> None:
    print(msg)


@ray.remote
def wait_all(*args) -> None:
    pass


if __name__ == "__main__":
    h1 = hello.options(**workflow.options(task_id="hello1")).bind("hello1")
    h2a = hello.options(**workflow.options(task_id="hello2a")).bind("hello2a")
    h2b = hello.options(**workflow.options(task_id="hello2b")).bind("hello2b", h2a)
    workflow.run(wait_all.bind(h1, h2b))
