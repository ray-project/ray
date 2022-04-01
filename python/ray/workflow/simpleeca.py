import asyncio
import random
import ray

async def some_event_step(stepid):
    await asyncio.sleep(random.uniform(3, 10))
    print(f"{stepid} arrived")
    return

@ray.remote
class SimpleECA:
    def __init__(self):
        self.pool = []

    async def register_event(self, stepid):
        self.pool.append(stepid)
        # Question - how to move the listener loop outside of registration?
        import nest_asyncio
        nest_asyncio.apply()
        numunfinished = asyncio.run(self.listener_loop())

        return 'registered'

    async def listener_loop(self):
        if len(self.pool) > 0:
            listeners = set()
            for e in self.pool:
                listeners.add(some_event_step(e))
            while True:
                finished, unfinished = await asyncio.wait(listeners, timeout=5,
                    return_when=asyncio.FIRST_COMPLETED)
                print(f"{len(finished)} events arrived")
                if len(unfinished) > 0:
                    listeners = unfinished
                else:
                    break
        self.pool = []

    async def check_status(self):
        return self.pool.values()

ray.init(address='auto')

async def __main__(*args, **kwargs):
    eca = SimpleECA.remote()
    obj1 = await eca.register_event.remote('step_0001')
    obj2 = await eca.register_event.remote('step_0002')

asyncio.run(__main__())
