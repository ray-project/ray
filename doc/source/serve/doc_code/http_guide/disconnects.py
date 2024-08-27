# flake8: noqa
# fmt: off

import ray
import sys
from typing import List

from ray._private.test_utils import wait_for_condition


# Overwrite print statement to make doc code testable
@ray.remote
class PrintStorage:
    def __init__(self):
        self.print_storage: List[str] = []

    def add(self, s: str):
        self.print_storage.append(s)

    def clear(self):
        self.print_storage.clear()

    def get(self) -> List[str]:
        return self.print_storage


print_storage_handle = PrintStorage.remote()


def print(string: str):
    ray.get(print_storage_handle.add.remote(string))
    sys.stdout.write(f"{string}\n")


# __start_basic_disconnect__
import asyncio
from ray import serve


@serve.deployment
async def startled():
    try:
        print("Replica received request!")
        await asyncio.sleep(10000)
    except asyncio.CancelledError:
        # Add custom behavior that should run
        # upon cancellation here.
        print("Request got cancelled!")
# __end_basic_disconnect__

serve.run(startled.bind())

import requests
from requests.exceptions import Timeout

# Intentionally time out request to test cancellation behavior
try:
    requests.get("http://localhost:8000", timeout=0.5)
except Timeout:
    pass

wait_for_condition(
    lambda: {"Replica received request!", "Request got cancelled!"}
    == set(ray.get(print_storage_handle.get.remote())),
    timeout=5,
)

sys.stdout.write(f"{ray.get(print_storage_handle.get.remote())}\n")

ray.get(print_storage_handle.clear.remote())


# __start_shielded_disconnect__
import asyncio
from ray import serve


@serve.deployment
class SnoringSleeper:
    async def snore(self):
        await asyncio.sleep(1)
        print("ZZZ")

    async def __call__(self):
        try:
            print("SnoringSleeper received request!")

            # Prevent the snore() method from being cancelled
            await asyncio.shield(self.snore())

        except asyncio.CancelledError:
            print("SnoringSleeper's request was cancelled!")


app = SnoringSleeper.bind()
# __end_shielded_disconnect__

serve.run(app)

import requests
from requests.exceptions import Timeout

# Intentionally time out request to test cancellation behavior
try:
    requests.get("http://localhost:8000", timeout=0.5)
except Timeout:
    pass

wait_for_condition(
    lambda: {
        "SnoringSleeper received request!",
        "SnoringSleeper's request was cancelled!",
        "ZZZ",
    }
    == set(ray.get(print_storage_handle.get.remote())),
    timeout=5,
)

sys.stdout.write(f"{ray.get(print_storage_handle.get.remote())}\n")

ray.get(print_storage_handle.clear.remote())
