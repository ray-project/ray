import asyncio
import time

import ray
import ray.experimental.aio as rayaio

ray.init()


@ray.remote
def echo_task(n):
    time.sleep(0.5)
    return n


async def multistep_work():
    for i in range(0, 100):
        # await asyncio.sleep(1)
        resp = echo_task.remote(i)
        print(f"    ... Finished task {await resp}")


print("")
print("\033[1mStarting multistep work with asyncio:\033[0m")
num_breaks = 0
while True:
    try:
        asyncio.run(multistep_work())
    except KeyboardInterrupt:
        num_breaks += 1
        time = "time" if num_breaks == 1 else "times"
        if num_breaks == 3:
            print(f"Caught keyboard interrupt {num_breaks} {time}. "
                  "Exiting ...")
            break
        else:
            print(f"Caught keyboard interrupt {num_breaks} {time}. "
                  "Restarting work ...")


print("")
print("\033[1mStarting multistep work with coroutine checkpointing:\033[0m")
rayaio.queue_work("key", multistep_work())
num_breaks = 0
while True:
    try:
        asyncio.run(rayaio.execute_work("key"))
    except KeyboardInterrupt:
        num_breaks += 1
        time = "time" if num_breaks == 1 else "times"
        if num_breaks == 3:
            print(f"Caught keyboard interrupt {num_breaks} {time}. "
                  "Exiting ...")
            break
        else:
            print(f"Caught keyboard interrupt {num_breaks} {time}. "
                  "Restarting work ...")
