import os
import tarfile
import unittest
import ray
import ray.services as services
import ray.datasets.imagenet as imagenet

class ImageNetTest(unittest.TestCase):

  def testImageNetLoading(self):
    worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_worker.py")
    services.start_ray_local(num_workers=5, worker_path=worker_path)

    chunk_name = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/mini.tar")
    tar = tarfile.open(chunk_name, mode= "r")
    chunk = imagenet.load_chunk(tar, size=(256, 256))
    self.assertEqual(chunk.shape, (2, 256, 256, 3))

    services.cleanup()

if __name__ == "__main__":
    unittest.main()
