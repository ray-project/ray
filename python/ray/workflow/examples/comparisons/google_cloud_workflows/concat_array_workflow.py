from typing import List

import ray
from ray import workflow


@ray.remote
def iterate(array: List[str], result: str, i: int) -> str:
    if i >= len(array):
        return result
    return workflow.continuation(iterate.bind(array, result + array[i], i + 1))


if __name__ == "__main__":
    print(workflow.run(iterate.bind(["foo", "ba", "r"], "", 0)))
