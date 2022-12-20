import ray
from ray import workflow


@ray.remote
def echo(msg: str, *deps) -> None:
    print(msg)


if __name__ == "__main__":
    A = echo.options(**workflow.options(task_id="A")).bind("A")
    B = echo.options(**workflow.options(task_id="B")).bind("B", A)
    C = echo.options(**workflow.options(task_id="C")).bind("C", A)
    D = echo.options(**workflow.options(task_id="D")).bind("D", A, B)
    workflow.run(D)
