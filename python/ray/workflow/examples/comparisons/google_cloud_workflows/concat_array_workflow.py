from typing import List

from ray import workflow


@workflow.step
def iterate(array: List[str], result: str, i: int) -> str:
    if i >= len(array):
        return result
    return iterate.step(array, result + array[i], i + 1)


if __name__ == "__main__":
    workflow.init()
    print(iterate.step(["foo", "ba", "r"], "", 0).run())
