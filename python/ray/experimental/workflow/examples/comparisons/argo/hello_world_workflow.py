import sys

from ray import workflow


@workflow.step(runtime_env={"pip": ["whalesay"]})
def hello(msg: str) -> None:
    import whalesay
    whalesay.print(msg)


if __name__ == "__main__":
    workflow.init()
    hello.step(sys.argv[1]).run()
