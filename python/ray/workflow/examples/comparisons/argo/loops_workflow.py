from ray import workflow


@workflow.step
def hello(msg: str) -> None:
    print(msg)


@workflow.step
def wait_all(*args) -> None:
    pass


if __name__ == "__main__":
    workflow.init()
    children = []
    for msg in ["hello world", "goodbye world"]:
        children.append(hello.step(msg))
    wait_all.step(*children).run()
