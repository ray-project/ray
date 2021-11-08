from ray import workflow


@workflow.step
def hello(msg: str, *deps) -> None:
    print(msg)


@workflow.step
def wait_all(*args) -> None:
    pass


if __name__ == "__main__":
    workflow.init()
    h1 = hello.options(name="hello1").step("hello1")
    h2a = hello.options(name="hello2a").step("hello2a")
    h2b = hello.options(name="hello2b").step("hello2b", h2a)
    wait_all.step(h1, h2b).run()
