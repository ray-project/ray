from ray import workflow


@workflow.step(runtime_env={"pip": ["whalesay"]})
def hello(msg: str) -> None:
    import whalesay
    whalesay.print(msg)


@workflow.step
def wait_all(*args) -> None:
    pass


if __name__ == "__main__":
    workflow.init()

    children = []
    for msg in ["hello world", "goodbye world"]:
        children.append(hello.step(msg))

    wait_all.step(*children).run()
