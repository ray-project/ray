from typing import Tuple, Optional

from ray import workflow


@workflow.step
def intentional_fail() -> str:
    raise RuntimeError("oops")


@workflow.step
def cry(error: Exception) -> None:
    print("Sadly", error)


@workflow.step
def celebrate(result: str) -> None:
    print("Success!", result)


@workflow.step
def send_email(result: str) -> None:
    print("Sending email", result)


@workflow.step
def exit_handler(res: Tuple[Optional[str], Optional[Exception]]) -> None:
    result, error = res
    email = send_email.step("Raw result: {}, {}".format(result, error))
    if error:
        handler = cry.step(error)
    else:
        handler = celebrate.step(result)
    return wait_all.step(handler, email)


@workflow.step
def wait_all(*deps):
    pass


if __name__ == "__main__":
    workflow.init()
    res = intentional_fail.options(catch_exceptions=True).step()
    print(exit_handler.step(res).run())
