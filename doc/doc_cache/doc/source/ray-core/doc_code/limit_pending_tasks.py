# __without_backpressure_start__
import ray

ray.init()


@ray.remote
class Actor:
    async def heavy_compute(self):
        # taking a long time...
        # await asyncio.sleep(5)
        return


actor = Actor.remote()

NUM_TASKS = 1000
result_refs = []
# When NUM_TASKS is large enough, this will eventually OOM.
for _ in range(NUM_TASKS):
    result_refs.append(actor.heavy_compute.remote())
ray.get(result_refs)
# __without_backpressure_end__

# __with_backpressure_start__
MAX_NUM_PENDING_TASKS = 100
result_refs = []
for _ in range(NUM_TASKS):
    if len(result_refs) > MAX_NUM_PENDING_TASKS:
        # update result_refs to only
        # track the remaining tasks.
        ready_refs, result_refs = ray.wait(result_refs, num_returns=1)
        ray.get(ready_refs)

    result_refs.append(actor.heavy_compute.remote())

ray.get(result_refs)
# __with_backpressure_end__
