import ray
from ray import workflow


@ray.remote
def compose_greeting(greeting: str, name: str) -> str:
    return greeting + ": " + name


@ray.remote
def main_workflow(name: str) -> str:
    return workflow.continuation(compose_greeting.bind("Hello", name))


if __name__ == "__main__":
    workflow.init()
    wf = workflow.create(main_workflow.bind("Alice"))
    print(wf.run())
