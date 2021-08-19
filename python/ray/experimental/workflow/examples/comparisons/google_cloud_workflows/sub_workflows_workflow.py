from ray import workflow


@workflow.step
def hello(name: str) -> str:
    return format_name.step(name)


@workflow.step
def format_name(name: str) -> str:
    return "hello, {}".format(name)


@workflow.step
def report(msg: str) -> None:
    print(msg)


if __name__ == "__main__":
    workflow.init()
    r1 = hello.step("Kristof")
    r2 = report.step(r1)
    r2.run()
