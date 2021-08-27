from ray import workflow


# TODO(ekl) should support something like runtime_env={"pip": ["whalesay"]}
@workflow.step
def hello(msg: str) -> None:
    print(msg)


if __name__ == "__main__":
    workflow.init()
    hello.step("hello world").run()
