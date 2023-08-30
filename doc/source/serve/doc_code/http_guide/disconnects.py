import ray
from typing import List

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

original_print = print
print = lambda s: ray.get(print_storage_handle.add.remote(s)) or original_print(s)

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

# Intentionally timeout request to test cancellation behavior
try:
    requests.get("http://localhost:8000", timeout=0.5)
except Timeout:
    pass

print_statements = ray.get(print_storage_handle.get.remote())

original_print(print_statements)

assert {
    "Replica received request!",
    "Request got cancelled!"
} == set(print_statements)

ray.get(print_storage_handle.clear.remote())

# __start_shielded_disconnect__
import asyncio
from ray import serve
from ray.serve.handle import RayServeHandle

@serve.deployment
class Guardian:

    def __init__(self, sleeper_handle: RayServeHandle):
        self.sleeper_handle = sleeper_handle

    async def __call__(self):
        try:
            print("Guardian received request!")

            # Shield downstream call from cancellation
            await asyncio.shield(self.sleeper_handle.remote())

        except asyncio.CancelledError:
            print("Guardian's request was cancelled!")

@serve.deployment
async def sleeper():
    print("Sleeper received request!")
    await asyncio.sleep(3)
    print("Sleeper deployment finished sleeping!")

app = Guardian.bind(sleeper.bind())
# __end_shielded_disconnect__

serve.run(app)

# Intentionally timeout request to test cancellation behavior
try:
    requests.get("http://localhost:8000", timeout=0.5)
except Timeout:
    pass

print_statements = ray.get(print_storage_handle.get.remote())

assert {
    "Guardian received request!",
    "Guardian's request was cancelled!",
    "Sleeper received request!",
    "Sleeper deployment finished sleeping!"
} == set(print_statements)

ray.get(print_storage_handle.clear.remote())
