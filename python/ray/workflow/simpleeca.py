import asyncio
import random
import ray
import threading
import time

async def some_event_step(stepid):
    await asyncio.sleep(random.uniform(2, 4))
    return stepid

@ray.remote
class SimpleECA:
    def __init__(self):
        self.pool = []
        try:
           self.thread = threading.Thread(target=asyncio.run, args=(self.listener_loop(),))
           self.thread.daemon = True
           self.thread.start()
           print('thread started')
        except BaseException as err:
            print(err)
            print ("Error: unable to start thread")

    async def register_event(self, stepid):
        self.pool.append(stepid)
        return 'registered'

    async def listener_loop(self):
        while True:
            await asyncio.sleep(3)
            print(f"pool {self.pool}")
            if len(self.pool) > 0:
                listeners = set()
                for e in self.pool:
                    listeners.add(some_event_step(e))
                finished, unfinished = await asyncio.wait(listeners, timeout=5,
                    return_when=asyncio.FIRST_COMPLETED)
                for item in finished:
                    print(f"event {item.result()} arrived")
                    self.pool.remove(item.result())

    async def check_status(self):
        return self.pool.values()

ray.init(address='auto')

async def __main__(*args, **kwargs):
    eca = SimpleECA.remote()
    obj1 = await eca.register_event.remote('step_0001')
    print('step_0001 registered')
    await asyncio.sleep(2)
    obj2 = await eca.register_event.remote('step_0002')
    print('step_0002 registered')
    await asyncio.sleep(20)

asyncio.run(__main__())
