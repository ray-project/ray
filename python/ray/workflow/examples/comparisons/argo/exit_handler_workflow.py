from typing import Tuple, Optional

import ray
from ray import workflow


@ray.remote
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


@ray.remote
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
    res = intentional_fail.options(**workflow.options(catch_exceptions=True)).bind()
    print(workflow.create(exit_handler.bind(res)).run())
