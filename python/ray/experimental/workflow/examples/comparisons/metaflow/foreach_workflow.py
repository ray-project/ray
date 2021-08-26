from typing import List

from ray import workflow


@workflow.step
def start():
    titles = ["Stranger Things", "House of Cards", "Narcos"]
    children = []
    for t in titles:
        children.append(a.step(t))
    return end.step(children)


@workflow.step
def a(title: str) -> str:
    return "{} processed".format(title)


@workflow.step
def end(results: List[str]) -> str:
    return "\n".join(results)


if __name__ == "__main__":
    workflow.init()
    start.step().run()
