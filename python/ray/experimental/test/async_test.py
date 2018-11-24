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

HashFlowNode = namedtuple('HashFlowNode', ['parents', 'delay', 'result'])

import logging

logging.basicConfig(level=logging.DEBUG)


@pytest.fixture
def init():
    ray.init()
    asyncio.get_event_loop().set_debug(False)
    asyncio.get_event_loop().run_until_complete(async_api.init())
    yield
    ray.shutdown()
    async_api.shutdown()


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
        await asyncio.sleep(delay)

        @ray.remote
        def f(n):
            time.sleep(n * time_scale)
            return n

        return f.remote(n)

    return [_gen(i) for i in range(5)]


def test_get(init):
    # get
    tasks = gen_tasks()
    fut = async_api.get(tasks)
    results = asyncio.get_event_loop().run_until_complete(fut)
    assert all(a == b for a, b in zip(results, ray.get(tasks)))


def test_get_benchmark(init):
    @ray.remote
    def f(n):
        time.sleep(0.001 * n)
        return 42

    async def test_async():
        sum_time = 0.
        for _ in range(100):
            tasks = [f.remote(n) for n in range(20)]
            start = time.time()
            await async_api.get(tasks)
            sum_time += time.time() - start
        return sum_time

    def baseline():
        sum_time = 0.
        for _ in range(100):
            tasks = [f.remote(n) for n in range(20)]
            start = time.time()
            ray.get(tasks)
            sum_time += time.time() - start
        return sum_time

    # warm up
    baseline()

    # async get
    sum_time = asyncio.get_event_loop().run_until_complete(test_async())
    print(sum_time)

    # get
    print(baseline())


def test_wait(init):
    # wait
    tasks = gen_tasks()
    fut = async_api.wait(tasks, num_returns=len(tasks))
    results, _ = asyncio.get_event_loop().run_until_complete(fut)
    assert set(results) == set(tasks)


def test_wait_timeout(init):
    # wait_timeout
    tasks = gen_tasks(10)
    fut = async_api.wait(tasks, timeout=5000, num_returns=len(tasks))
    results, _ = asyncio.get_event_loop().run_until_complete(fut)
    assert results[0] == tasks[0]


# def test_api(init):

#
#     # wait_timeout_complex
#     tasks = delayed_gen_tasks(10, 5)
#     fut = async_api.wait(tasks, timeout=5, num_returns=len(tasks))
#     results, pendings = asyncio.get_event_loop().run_until_complete(fut)
#     assert tasks == pendings  # pytest supports list compare
#
#     # get from coroutine/future
#     async def get_obj():
#         return ray.put("foobar")
#
#     result = asyncio.get_event_loop().run_until_complete(
#         async_api.get(get_obj()))
#     assert result == "foobar"
#
#     # get from coroutine/future chains
#     obj_id = "qwerty"
#     for _ in range(7):
#         obj_id = ray.put([obj_id])
#
#     async def recurrent_get(obj_id):
#         if isinstance(obj_id, str):
#             return obj_id
#         obj_id = await async_api.get(obj_id)
#         if isinstance(obj_id, list):
#             return await (recurrent_get(obj_id[0]))
#         return obj_id
#
#     results = asyncio.get_event_loop().run_until_complete(
#         recurrent_get(obj_id))
#     assert results == "qwerty"
#
#
#
# @pytest.fixture
# def gen_hashflow(seed, width, depth):
#     random.seed(seed)
#     n = int(math.log2(width))
#     inputs = [i.to_bytes(20, byteorder='big') for i in range(width)]
#     stages = []
#     for _ in range(depth):
#         nodes = [
#             HashFlowNode(
#                 parents=[random.randint(0, width - 1) for _ in range(n)],
#                 delay=random.random() * 0.1,
#                 result=None) for _ in range(width)
#         ]
#         stages.append(nodes)
#
#     stages.append([
#         HashFlowNode(
#             parents=list(range(width)), delay=random.random(), result=None)
#     ])
#
#     return inputs, stages
#
#
# @pytest.fixture
# def calc_hashflow(inputs, delay=None):
#     if delay is not None:
#         time.sleep(delay)
#
#     m = hashlib.sha256()
#
#     for item in inputs:
#         if isinstance(item, ray.ObjectID):
#             item = ray.get(item)
#         m.update(item)
#
#     return m.digest()
#
#
# @pytest.fixture
# def default_hashflow_solution(inputs, stages, use_delay=False):
#     # forced to re-register the function
#     # because Ray may be re-init in another test
#     calc_hashflow_remote = ray.remote(calc_hashflow)
#     inputs = list(map(ray.put, inputs))
#     for i, stage in enumerate(stages):
#         new_inputs = []
#         for node in stage:
#             node_inputs = [inputs[i] for i in node.parents]
#             delay = node.delay if use_delay else None
#             new_inputs.append(
#                 calc_hashflow_remote.remote(node_inputs, delay=delay))
#         inputs = new_inputs
#
#     return ray.get(inputs[0])
#
#
#
# @pytest.fixture
# def wait_and_solve(inputs, node, use_delay, loop):
#     # forced to re-register the function
#     # because Ray may be re-init in another test
#     calc_hashflow_remote = ray.remote(calc_hashflow)
#
#     async def _wait_and_solve(a_inputs):
#         r_inputs = await a_inputs
#         delay = node.delay if use_delay else None
#         return calc_hashflow_remote.remote(r_inputs, delay=delay)
#
#     return asyncio.ensure_future(_wait_and_solve(inputs), loop=loop)
#
#
# @pytest.fixture
# def async_hashflow_solution_get(inputs, stages, use_delay=False):
#     with PlasmaEventLoopUsePoll() as loop:
#         inputs = list(map(ray.put, inputs))
#         for i, stage in enumerate(stages):
#             new_inputs = []
#             for node in stage:
#                 node_inputs = [inputs[i] for i in node.parents]
#                 async_inputs = loop.get(node_inputs)
#                 new_inputs.append(
#                     wait_and_solve(async_inputs, node, use_delay, loop=loop))
#             inputs = new_inputs
#
#         result = loop.run_until_complete(inputs[0])
#     return ray.get(result)
#
#
# @pytest.fixture
# def async_hashflow_solution_wait(inputs, stages, use_delay=False):
#     async def return_first_item(coro):
#         result = await coro
#         return result[0]
#
#     with PlasmaEventLoopUsePoll() as loop:
#         inputs = list(map(ray.put, inputs))
#         for i, stage in enumerate(stages):
#             new_inputs = []
#             for node in stage:
#                 node_inputs = [inputs[i] for i in node.parents]
#                 async_inputs = loop.wait(
#                     node_inputs, num_returns=len(node_inputs))
#                 ready = return_first_item(async_inputs)
#                 new_inputs.append(
#                     wait_and_solve(ready, node, use_delay, loop=loop))
#             inputs = new_inputs
#         result = loop.run_until_complete(inputs[0])
#     return ray.get(result)


# class TestAsyncPlasma(unittest.TestCase):
#     """This test creates a complex pseudo-random network to compute sha1sums.
#     Any violence of concurrency in the async processing will change the result.
#     """
#
#     # It is the correct result of this test (in the form of sha1sum).
#     answer = b'3Z_\xc9\x8e/\x9b\xc0\xb3\xa9\xdc}\xdd' \
#              b'\xe5\xbe\xf3\x92\xfe%\xa6X&t;\x0fmJ@\xf1C\xcdF'
#
#     def setUp(self):
#         # Start the Ray processes.
#         ray.init(num_cpus=4)
#
#     def tearDown(self):
#         ray.disconnect()
#
#     def test_baseline(self):
#         inputs, stages = gen_hashflow(0, 4, 16)
#         ground_truth = default_hashflow_solution(inputs, stages, True)
#         print(ground_truth)
#         self.assertEqual(ground_truth, self.answer)
#
#     def test_async_get(self):
#         inputs, stages = gen_hashflow(0, 4, 16)
#         result = async_hashflow_solution_get(inputs, stages, use_delay=True)
#         self.assertEqual(result, self.answer)
#
#     def test_async_wait(self):
#         inputs, stages = gen_hashflow(0, 4, 16)
#         result = async_hashflow_solution_wait(inputs, stages, use_delay=True)
#         self.assertEqual(result, self.answer)
