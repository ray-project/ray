from ray import workflow


@workflow.step
def compose_greeting(greeting: str, name: str) -> str:
    return greeting + ": " + name


@workflow.step
def main_workflow(name: str) -> str:
    return compose_greeting.step("Hello", name)


if __name__ == "__main__":
    workflow.init()
    wf = main_workflow.step("Alice")
    print(wf.run())
