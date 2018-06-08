from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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


class TestAsyncPlasma(unittest.TestCase):
    def setUp(self):
        # Start the Ray processes.
        ray.init(num_cpus=2)
    
    def tearDown(self):
        ray.worker.cleanup()
    
    def test_baseline(self):
        inputs, stages = gen_hashflow(0, 16, 16)
        
        ground_truth = default_hashflow_solution(inputs, stages, False)
        assert ground_truth == b'U\x16\xc5c\x0fa\xdcx\x03\x1e\xf7\xd8&{\xece\x85-.O\x12\xed\x11[\xdc\xe6\xcc\xdf\x90\x91\xc7\xf7'


if __name__ == "__main__":
    unittest.main(verbosity=2)
