from ray import workflow


@workflow.step
def echo(msg: str, *deps) -> None:
    print(msg)


if __name__ == "__main__":
    workflow.init()
    A = echo.options(name="A").step("A")
    B = echo.options(name="B").step("B", A)
    C = echo.options(name="C").step("C", A)
    D = echo.options(name="D").step("D", A, B)
    D.run()
