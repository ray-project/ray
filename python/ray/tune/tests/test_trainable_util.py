import os
import pickle
import shutil
import unittest

import ray.utils

from ray.tune.utils.trainable import TrainableUtil


class TrainableUtilTest(unittest.TestCase):
    def setUp(self):
        self.checkpoint_dir = os.path.join(ray.utils.get_user_temp_dir(),
                                           "tune", "MyTrainable123")
        TrainableUtil.make_checkpoint_dir(self.checkpoint_dir)

    def tearDown(self):
        self.addCleanup(shutil.rmtree, self.checkpoint_dir)

    def testFindCheckpointDir(self):
        checkpoint_path = os.path.join(self.checkpoint_dir, "my/nested/chkpt")
        os.makedirs(checkpoint_path)
        found_dir = TrainableUtil.find_checkpoint_dir(checkpoint_path)
        self.assertEquals(self.checkpoint_dir, found_dir)

        with self.assertRaises(FileNotFoundError):
            parent = os.path.dirname(found_dir)
            TrainableUtil.find_checkpoint_dir(parent)

    def testPickleCheckpoint(self):
        for i in range(5):
            path = os.path.join(self.checkpoint_dir, str(i))
            with open(path, "w") as f:
                f.write(str(i))

        checkpoint_path = os.path.join(self.checkpoint_dir, "0")

        data_dict = TrainableUtil.pickle_checkpoint(checkpoint_path)
        loaded = pickle.loads(data_dict)

        checkpoint_name = os.path.basename(checkpoint_path)
        self.assertEqual(loaded["checkpoint_name"], checkpoint_name)

        for i in range(5):
            path = os.path.join(self.checkpoint_dir, str(i))
            self.assertEquals(loaded["data"][str(i)], open(path, "rb").read())
