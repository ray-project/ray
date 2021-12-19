import asyncio
import ray.cloudpickle as pickle


async def run(coro):
    while True:
        try:
            fut = coro.send(None)
        except StopIteration as val:
            return val.value

        if hasattr(fut, "_object_ref"):
            object_ref = fut._object_ref
            actor = object_ref.actor()
            # assert actor is not None, \
            #     "Awaiting ObjectRef from Task is unimplemented! "\
            #     "Please only await on actors."
            if actor is not None:
                result = await actor.__ray_execute_coroutine__.remote(coro,
                                                                      object_ref)
                return result

        # Needs to wait on an object other than fut, because fut is already
        # awaited on.
        # TODO: Chain Future instead of Event, to handle cancellations?
        # new_future = loop.create_future()
        # asyncio._chain_future(fut, new_future)
        # yield
        assert asyncio.get_running_loop() is fut.get_loop()
        event = asyncio.Event()

        def future_done(f):
            event.set()

        fut.add_done_callback(future_done)
        await event.wait()


work = {}


def queue_work(key, coro):
    global work
    if key in work:
        raise ValueError(f"Duplicated work key: {key}")
    work[key] = pickle.dumps(coro)
    coro.close()


async def execute_work(key):
    # import pdb; pdb.set_trace()
    stored_coro = work.get(key)
    if stored_coro is None:
        raise ValueError(f"Work key not found: {key}")
    coro = pickle.loads(stored_coro)
    while True:
        try:
            fut = coro.send(None)
        except StopIteration as val:
            return val.value

        # Checkpoints work.
        if hasattr(fut, "_object_ref"):
            work[key] = pickle.dumps(coro)

        assert asyncio.get_running_loop() is fut.get_loop()
        event = asyncio.Event()

        def future_done(f):
            event.set()

        fut.add_done_callback(future_done)
        await event.wait()

        # Checkpoints work.
        if hasattr(fut, "_object_ref"):
            work[key] = pickle.dumps(coro)
