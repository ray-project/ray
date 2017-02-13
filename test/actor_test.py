from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import time
import ray

class ActorTest(unittest.TestCase):
  def testDefineActor(self):

    ray.init()

    @ray.actor
    class Test(object):
      def __init__(self, x):
        self.x = x
      def f(self, y):
        return 1

    t = Test(2)

    self.assertEqual(ray.get(t.f(0)), 1)

    ray.worker.cleanup()

  def testActorCounter(self):

    ray.init()
    
    @ray.actor
    class Counter(object):
      def __init__(self):
        self.value = 0
      def increase(self):
        self.value += 1
      def value(self):
        return self.value

    c1 = Counter()
    c1.increase()
    self.assertEqual(ray.get(c1.value()), 1)

    c2 = Counter()
    c2.increase()
    c2.increase()
    self.assertEqual(ray.get(c2.value()), 2)

    ray.worker.cleanup()

if __name__ == "__main__":
  unittest.main(verbosity=2)
