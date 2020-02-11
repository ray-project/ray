import unittest
import numpy as np

import ray
from ray.rllib.utils.memory import ray_get_and_free


class UtilsTest(unittest.TestCase):
    def testGetAndFree(self):
        ray.init(num_cpus=1)

        @ray.remote
        class Sampler:
            def sample(self):
                return [1, 2, 3, 4, 5]

        sampler = Sampler.remote()

        obj_id = sampler.sample.remote()
        sample = ray.get(obj_id)
        print("Sample: {}".format(sample))
        del sample

        ray.internal.free(obj_id, delete_plasma_only=True)
        print(ray.worker.global_worker.core_worker.get_all_reference_counts())

        sample = ray.get(obj_id) # Will block forever here
        print("Refetched sample: {}".format(sample))


if __name__ == "__main__":
    ray.init(num_cpus=1)
    unittest.main(verbosity=2)
