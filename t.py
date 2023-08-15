import ray
import asyncio
import time
import concurrent
from ray.exceptions import TaskCancelledError

# ray.init()

@ray.remote
class Actor:
    async def f(self):
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            print("error?")
            return True
        except concurrent.futures.CancelledError:
            print("conccurent")
            return True
        return False
    
    async def g(self, ref):
        await asyncio.sleep(30)

    async def empty(self):
        pass

@ray.remote
def f():
    for _ in range(30):
        time.sleep(1)

ref = f.remote()
time.sleep(2)
ray.cancel(ref)
try:
    ray.get(ref)
except ray.exceptions.TaskCancelledError as e:
    print("sang, ", e)
except Exception as e:
    assert False

print("Test regular case.")
a = Actor.remote()
ref = a.f.remote()
ray.get(a.__ray_ready__.remote())
ray.cancel(ref)

try:
    ray.get(ref)
except ray.exceptions.RayTaskError:
    print("hoho")
    pass
else:
    assert False
print("[passed] Test regular case.")

# Cancel when it is queued on a client side.
print("Test client side cancel.")
ref_dep_not_resolved = a.g.remote(f.remote())
ray.cancel(ref_dep_not_resolved)
try:
    ray.get(ref_dep_not_resolved)
except TaskCancelledError:
    pass
else:
    assert False
print("[passed] Test client side cancel.")

# Cancel when it is queued on a server side.

print("Test server side cancel.")
a = Actor.options(max_concurrency=1).remote()
ref = a.f.remote()
ref2 = a.f.remote()
ray.get(a.__ray_ready__.remote())
ray.cancel(ref2)
ray.get(ref)
try:
    ray.get(ref2)
except ray.exceptions.RayTaskError:
    pass
else:
    assert False
print("[passed] Test server side cancel.")

# Cancel and test exception
pass

print("Test cancel after task finishes.")
# Cancel after task finishes
a = Actor.options(max_concurrency=1).remote()
ref = a.empty.remote()
ref2 = a.empty.remote()
time.sleep(1)
ray.cancel(ref)
ray.get(ref2)
ray.get(ref)
print("[passed] Test cancel after task finishes.")

# Cancelation while doing restart

# cancel remote task
# Make sure it works with backpressure 
# Recursive cancel
# Test cancel ref from a task that doesn't have a handle
# If it works well with pending calls 
