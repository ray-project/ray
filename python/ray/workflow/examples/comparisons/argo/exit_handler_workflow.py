from typing import Tuple, Optional

import ray
from ray import workflow


@workflow.step
def intentional_fail() -> str:
    raise RuntimeError("oops")


@ray.remote
def cry(error: Exception) -> None:
    print("Sadly", error)


@ray.remote
def celebrate(result: str) -> None:
    print("Success!", result)


@ray.remote
def send_email(result: str) -> None:
    print("Sending email", result)


@workflow.step
def exit_handler(res: Tuple[Optional[str], Optional[Exception]]) -> None:
    result, error = res
    email = send_email.bind(f"Raw result: {result}, {error}")
    if error:
        handler = cry.bind(error)
    else:
        handler = celebrate.bind(result)
    return workflow.continuation(wait_all.bind(handler, email))


@ray.remote
def wait_all(*deps):
    return "done"


if __name__ == "__main__":
    workflow.init()
    res = intentional_fail.options(catch_exceptions=True).step()
    print(exit_handler.step(res).run())
