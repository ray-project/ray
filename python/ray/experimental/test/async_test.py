from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import asyncio
from collections import namedtuple
import hashlib
import math
import unittest
import time

import pytest
import random
import ray
import ray.experimental.async_api as async_api
from ray.experimental.plasma_eventloop import (PlasmaPoll, PlasmaEpoll,
                                               PlasmaSelectorEventLoop)

HashFlowNode = namedtuple('HashFlowNode', ['parents', 'delay', 'result'])


@pytest.fixture
def init():
    ray.init()
    yield
    ray.shutdown()


@pytest.fixture
def gen_tasks(time_scale=0.1):
    @ray.remote
    def f(n):
        time.sleep(n * time_scale)
        return n

    tasks = [f.remote(i) for i in range(5)]
    return tasks


@pytest.fixture
def delayed_gen_tasks(delay=5, time_scale=0.1):
    async def _gen(n):
        await asyncio.sleep(delay, loop=async_api.eventloop)

        @ray.remote
        def f(n):
            time.sleep(n * time_scale)
            return n

        return f.remote(n)

    return [_gen(i) for i in range(5)]


@pytest.fixture
def run_api():
    # get
    tasks = gen_tasks()
    fut = async_api.get(tasks)
    results = async_api.run_until_complete(fut)
    assert all(a == b for a, b in zip(results, ray.get(tasks)))

    # wait
    tasks = gen_tasks()
    fut = async_api.wait(tasks, num_returns=len(tasks))
    results, _ = async_api.run_until_complete(fut)
    assert set(results) == set(tasks)

    # wait_timeout
    tasks = gen_tasks(10)
    fut = async_api.wait(tasks, timeout=5, num_returns=len(tasks))
    results, _ = async_api.run_until_complete(fut)
    assert results[0] == tasks[0]

    # wait_timeout_complex
    tasks = delayed_gen_tasks(100, 5)
    fut = async_api.wait(tasks, timeout=5, num_returns=len(tasks))
    results, pendings = async_api.run_until_complete(fut)
    print(results, pendings)
    assert pendings == [None] * 5  # pytest allows list compare

    # get from coroutine/future
    async def get_obj():
        return ray.put("foobar")

    result = async_api.run_until_complete(async_api.get(get_obj()))
    assert result == "foobar"

    # get from coroutine/future chains
    obj_id = "qwerty"
    for _ in range(7):
        obj_id = ray.put([obj_id])

    async def recurrent_get(obj_id):
        if isinstance(obj_id, str):
            return obj_id
        obj_id = await async_api.get(obj_id)
        if isinstance(obj_id, list):
            return await (recurrent_get(obj_id[0]))
        return obj_id

    results = async_api.run_until_complete(recurrent_get(obj_id))
    assert results == "qwerty"


def test_api_poll(init):
    async_api.set_debug(True)
    async_api._init_eventloop('poll')
    run_api()
    async_api.cleanup()


def test_api_epoll(init):
    async_api.set_debug(True)
    async_api._init_eventloop('epoll')
    run_api()
    async_api.cleanup()


@pytest.fixture
def gen_hashflow(seed, width, depth):
    random.seed(seed)
    n = int(math.log2(width))
    inputs = [i.to_bytes(20, byteorder='big') for i in range(width)]
    stages = []
    for _ in range(depth):
        nodes = [
            HashFlowNode(
                parents=[random.randint(0, width - 1) for _ in range(n)],
                delay=random.random() * 0.1,
                result=None) for _ in range(width)
        ]
        stages.append(nodes)

    stages.append([
        HashFlowNode(
            parents=list(range(width)), delay=random.random(), result=None)
    ])

    return inputs, stages


@pytest.fixture
def calc_hashflow(inputs, delay=None):
    if delay is not None:
        time.sleep(delay)

    m = hashlib.sha256()

    for item in inputs:
        if isinstance(item, ray.local_scheduler.ObjectID):
            item = ray.get(item)
        m.update(item)

    return m.digest()


@pytest.fixture
def default_hashflow_solution(inputs, stages, use_delay=False):
    # forced to re-register the function
    # because Ray may be re-init in another test
    calc_hashflow_remote = ray.remote(calc_hashflow)
    inputs = list(map(ray.put, inputs))
    for i, stage in enumerate(stages):
        new_inputs = []
        for node in stage:
            node_inputs = [inputs[i] for i in node.parents]
            delay = node.delay if use_delay else None
            new_inputs.append(
                calc_hashflow_remote.remote(node_inputs, delay=delay))
        inputs = new_inputs

    return ray.get(inputs[0])


class PlasmaEventLoopUsePoll(PlasmaSelectorEventLoop):
    def __init__(self):
        self.selector = PlasmaPoll(ray.worker.global_worker)
        super().__init__(self.selector, worker=ray.worker.global_worker)

    def __enter__(self):
        self.set_debug(False)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class PlasmaEventLoopUseEpoll(PlasmaSelectorEventLoop):
    def __init__(self):
        self.selector = PlasmaEpoll(ray.worker.global_worker)
        super().__init__(self.selector, worker=ray.worker.global_worker)

    def __enter__(self):
        self.set_debug(False)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


@pytest.fixture
def wait_and_solve(inputs, node, use_delay, loop):
    # forced to re-register the function
    # because Ray may be re-init in another test
    calc_hashflow_remote = ray.remote(calc_hashflow)

    async def _wait_and_solve(a_inputs):
        r_inputs = await a_inputs
        delay = node.delay if use_delay else None
        return calc_hashflow_remote.remote(r_inputs, delay=delay)

    return asyncio.ensure_future(_wait_and_solve(inputs), loop=loop)


@pytest.fixture
def async_hashflow_solution_get(inputs, stages, use_delay=False):
    with PlasmaEventLoopUseEpoll() as loop:
        inputs = list(map(ray.put, inputs))
        for i, stage in enumerate(stages):
            new_inputs = []
            for node in stage:
                node_inputs = [inputs[i] for i in node.parents]
                async_inputs = loop.get(node_inputs)
                new_inputs.append(
                    wait_and_solve(async_inputs, node, use_delay, loop=loop))
            inputs = new_inputs

        result = loop.run_until_complete(inputs[0])
    return ray.get(result)


@pytest.fixture
def async_hashflow_solution_wait(inputs, stages, use_delay=False):
    async def return_first_item(coro):
        result = await coro
        return result[0]

    with PlasmaEventLoopUseEpoll() as loop:
        inputs = list(map(ray.put, inputs))
        for i, stage in enumerate(stages):
            new_inputs = []
            for node in stage:
                node_inputs = [inputs[i] for i in node.parents]
                async_inputs = loop.wait(
                    node_inputs, num_returns=len(node_inputs))
                ready = return_first_item(async_inputs)
                new_inputs.append(
                    wait_and_solve(ready, node, use_delay, loop=loop))
            inputs = new_inputs
        result = loop.run_until_complete(inputs[0])
    return ray.get(result)


class TestAsyncPlasma(unittest.TestCase):
    """This test creates a complex pseudo-random network to compute sha1sums.
    Any violence of concurrency in the async processing will change the result.
    """

    # It is the correct result of this test (in the form of sha1sum).
    answer = b'U\x16\xc5c\x0fa\xdcx\x03\x1e\xf7\xd8&{\xece' \
             b'\x85-.O\x12\xed\x11[\xdc\xe6\xcc\xdf\x90\x91\xc7\xf7'

    def setUp(self):
        # Start the Ray processes.
        ray.init(num_cpus=2)

    def tearDown(self):
        ray.worker.cleanup()

    def test_baseline(self):
        inputs, stages = gen_hashflow(0, 16, 16)
        ground_truth = default_hashflow_solution(inputs, stages, True)
        self.assertEqual(ground_truth, self.answer)

    def test_async_get(self):
        inputs, stages = gen_hashflow(0, 16, 16)
        result = async_hashflow_solution_get(inputs, stages, use_delay=True)
        self.assertEqual(result, self.answer)

    def test_async_wait(self):
        inputs, stages = gen_hashflow(0, 16, 16)
        result = async_hashflow_solution_wait(inputs, stages, use_delay=True)
        self.assertEqual(result, self.answer)
