from typing import List

import ray
from ray import workflow


@ray.remote
def start():
    titles = ["Stranger Things", "House of Cards", "Narcos"]
    children = [a.bind(t) for t in titles]
    return workflow.continuation(end.bind(children))


@ray.remote
def a(title: str) -> str:
    return f"{title} processed"


@ray.remote
def end(results: "List[ray.ObjectRef[str]]") -> str:
    return "\n".join(ray.get(results))


if __name__ == "__main__":
    workflow.init()
    workflow.create(start.bind()).run()
