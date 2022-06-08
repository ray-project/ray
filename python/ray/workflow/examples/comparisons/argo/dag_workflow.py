import ray
from ray import workflow


@ray.remote
def echo(msg: str, *deps) -> None:
    print(msg)


if __name__ == "__main__":
    A = echo.options(**workflow.options(name="A")).bind("A")
    B = echo.options(**workflow.options(name="B")).bind("B", A)
    C = echo.options(**workflow.options(name="C")).bind("C", A)
    D = echo.options(**workflow.options(name="D")).bind("D", A, B)
    workflow.create(D).run()
