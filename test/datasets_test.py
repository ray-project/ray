import os
import tarfile
import unittest
import ray
import ray.services as services
import ray.datasets.imagenet as imagenet

class ImageNetTest(unittest.TestCase):

  def testImageNetLoading(self):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_path = os.path.join(test_dir, "test_worker.py")
    services.start_singlenode_cluster(return_drivers=False, num_workers_per_objstore=5, worker_path=test_path)

    chunk_name = os.path.join(test_dir, "..", "data", "mini.tar")
    tar = tarfile.open(chunk_name, mode= "r")
    chunk = imagenet.load_chunk(tar, size=(256, 256))
    self.assertEqual(chunk.shape, (2, 256, 256, 3))

    services.cleanup()

if __name__ == "__main__":
    unittest.main()
