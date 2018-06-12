from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import asyncio
from collections import namedtuple
import math
import unittest
import random
import ray
from ..plasma_eventloop import PlasmaPoll, PlasmaSelectorEventLoop

HashFlowNode = namedtuple('HashFlowNode', ['parents', 'delay', 'result'])


def gen_hashflow(seed, width, depth):
    random.seed(seed)
    n = int(math.log2(width))
    inputs = [i.to_bytes(20, byteorder='big') for i in range(width)]
    stages = []
    for _ in range(depth):
        nodes = [
            HashFlowNode(parents=[random.randint(0, width - 1) for _ in range(n)], delay=random.random(), result=None)
            for _ in range(width)]
        stages.append(nodes)
    
    stages.append([HashFlowNode(parents=list(range(width)), delay=random.random(), result=None)])
    
    return inputs, stages


@ray.remote
def calc_hashflow(inputs, delay=None):
    import time
    import hashlib
    
    if delay is not None:
        time.sleep(delay)
    
    m = hashlib.sha256()
    
    for item in inputs:
        if isinstance(item, ray.local_scheduler.ObjectID):
            item = ray.get(item)
        m.update(item)
    
    return m.digest()


def default_hashflow_solution(inputs, stages, use_delay=False):
    inputs = list(map(ray.put, inputs))
    for i, stage in enumerate(stages):
        new_inputs = []
        for node in stage:
            node_inputs = [inputs[i] for i in node.parents]
            delay = node.delay if use_delay else None
            new_inputs.append(calc_hashflow.remote(node_inputs, delay=delay))
        inputs = new_inputs
    
    return ray.get(inputs[0])


def wait_and_solve(inputs, node, use_delay, loop):
    @asyncio.coroutine
    def _wait_and_solve(a_inputs):
        r_inputs = yield from a_inputs
        delay = node.delay if use_delay else None
        return calc_hashflow.remote(r_inputs, delay=delay)
    
    return asyncio.ensure_future(_wait_and_solve(inputs), loop=loop)


def async_hashflow_solution_get(inputs, stages, use_delay=False):
    selector = PlasmaPoll(ray.worker.global_worker)
    loop = PlasmaSelectorEventLoop(selector, worker=ray.worker.global_worker)
    loop.set_debug(True)
    
    inputs = list(map(ray.put, inputs))
    for i, stage in enumerate(stages):
        new_inputs = []
        for node in stage:
            node_inputs = [inputs[i] for i in node.parents]
            async_inputs = loop.get(node_inputs)
            new_inputs.append(wait_and_solve(async_inputs, node, use_delay, loop=loop))
        inputs = new_inputs
    
    result = loop.run_until_complete(inputs[0])
    loop.close()
    return ray.get(result)


def async_hashflow_solution_wait(inputs, stages, use_delay=False):
    selector = PlasmaPoll(ray.worker.global_worker)
    loop = PlasmaSelectorEventLoop(selector, worker=ray.worker.global_worker)
    loop.set_debug(True)
    
    @asyncio.coroutine
    def return_first_item(coro):
        result = yield from coro
        return result[0]
    
    inputs = list(map(ray.put, inputs))
    for i, stage in enumerate(stages):
        new_inputs = []
        for node in stage:
            node_inputs = [inputs[i] for i in node.parents]
            async_inputs = loop.wait(node_inputs, num_returns=len(node_inputs))
            ready = return_first_item(async_inputs)
            new_inputs.append(wait_and_solve(ready, node, use_delay, loop=loop))
        inputs = new_inputs
    
    result = loop.run_until_complete(inputs[0])
    loop.close()
    return ray.get(result)


class TestAsyncPlasmaBasic(unittest.TestCase):
    def setUp(self):
        # Start the Ray processes.
        ray.init(num_cpus=2)
    
    def tearDown(self):
        ray.worker.cleanup()
    
    def test_get(self):
        selector = PlasmaPoll(ray.worker.global_worker)
        loop = PlasmaSelectorEventLoop(selector, worker=ray.worker.global_worker)
        loop.set_debug(True)
        
        @ray.remote
        def f(n):
            import time
            time.sleep(n)
            return n
        
        tasks = [f.remote(i) for i in range(5)]
        
        results = loop.run_until_complete(loop.get(tasks))
        self.assertListEqual(results, ray.get(tasks))
        
        loop.close()
    
    def test_wait(self):
        selector = PlasmaPoll(ray.worker.global_worker)
        loop = PlasmaSelectorEventLoop(selector, worker=ray.worker.global_worker)
        loop.set_debug(True)
        
        @ray.remote
        def f(n):
            import time
            time.sleep(n)
            return n
        
        tasks = [f.remote(i) for i in range(5)]
        results, _ = loop.run_until_complete(loop.wait(tasks, num_returns=len(tasks)))
        self.assertEqual(set(results), set(tasks))
        loop.close()


class TestAsyncPlasmaWait(unittest.TestCase):
    answer = b'U\x16\xc5c\x0fa\xdcx\x03\x1e\xf7\xd8&{\xece\x85-.O\x12\xed\x11[\xdc\xe6\xcc\xdf\x90\x91\xc7\xf7'
    
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
