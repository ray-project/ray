from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import time
import ray

class ActorTest(unittest.TestCase):
  def testDefineActor(self):

    ray.init()
	
    time.sleep(1)

    @ray.actor
    class Test(object):
      def __init__(self, x):
        self.x = x
      def f(self, y):
        return 1

    t = Test(2)

    ray.worker.cleanup()

if __name__ == "__main__":
  unittest.main(verbosity=2)
