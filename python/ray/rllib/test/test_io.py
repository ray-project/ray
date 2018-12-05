from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import os
import shutil
import tempfile
import unittest

import ray
from ray.rllib.evaluation import SampleBatch
from ray.rllib.io import IOContext, JsonWriter, JsonReader
from ray.rllib.io.json_writer import _to_json

SAMPLES = SampleBatch(
    {"actions": np.array([1, 2, 3]), "obs": np.array([4, 5, 6])})


def make_sample_batch(i):
    return SampleBatch(
        {"actions": np.array([i, i, i]), "obs": np.array([i, i, i])})


class JsonIOTest(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def testWriteSimple(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        writer = JsonWriter(
            ioctx, "logdir", max_file_size=1000, compress_columns=["obs"])
        self.assertEqual(len(os.listdir(self.test_dir)), 0)
        writer.write(SAMPLES)
        writer.write(SAMPLES)
        self.assertEqual(len(os.listdir(self.test_dir)), 1)

    def testWriteFileURI(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        writer = JsonWriter(
            ioctx, "file:" + self.test_dir, max_file_size=1000,
            compress_columns=["obs"])
        self.assertEqual(len(os.listdir(self.test_dir)), 0)
        writer.write(SAMPLES)
        writer.write(SAMPLES)
        self.assertEqual(len(os.listdir(self.test_dir)), 1)

    def testWritePaginate(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        writer = JsonWriter(
            ioctx, self.test_dir, max_file_size=5000, compress_columns=["obs"])
        self.assertEqual(len(os.listdir(self.test_dir)), 0)
        for _ in range(100):
            writer.write(SAMPLES)
        self.assertEqual(len(os.listdir(self.test_dir)), 12)

    def testReadWrite(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        writer = JsonWriter(
            ioctx, self.test_dir, max_file_size=5000, compress_columns=["obs"])
        for i in range(100):
            writer.write(make_sample_batch(i))
        reader = JsonReader(ioctx, self.test_dir + "/*.json")
        seen_a = set()
        seen_o = set()
        for i in range(1000):
            batch = reader.next()
            seen_a.add(batch["actions"][0])
            seen_o.add(batch["obs"][0])
        self.assertGreater(len(seen_a), 90)
        self.assertLess(len(seen_a), 101)
        self.assertGreater(len(seen_o), 90)
        self.assertLess(len(seen_o), 101)

    def testSkipsOverEmptyLinesAndFiles(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        open(self.test_dir + "/empty", "w").close()
        with open(self.test_dir + "/f1", "w") as f:
            f.write("\n")
            f.write("\n")
            f.write(_to_json(make_sample_batch(0), []))
        with open(self.test_dir + "/f2", "w") as f:
            f.write(_to_json(make_sample_batch(1), []))
            f.write("\n")
        reader = JsonReader(ioctx, [
            self.test_dir + "/empty",
            self.test_dir + "/f1",
            "file:" + self.test_dir + "/f2",
        ])
        seen_a = set()
        for i in range(100):
            batch = reader.next()
            seen_a.add(batch["actions"][0])
        self.assertEqual(len(seen_a), 2)

    def testAbortOnAllEmptyInputs(self):
        ioctx = IOContext(self.test_dir, {}, 0, None)
        open(self.test_dir + "/empty", "w").close()
        reader = JsonReader(ioctx, [
            self.test_dir + "/empty",
        ])
        self.assertRaises(ValueError, lambda: reader.next())
        with open(self.test_dir + "/empty1", "w") as f:
            for _ in range(100):
                f.write("\n")
        with open(self.test_dir + "/empty2", "w") as f:
            for _ in range(100):
                f.write("\n")
        reader = JsonReader(ioctx, [
            self.test_dir + "/empty1",
            self.test_dir + "/empty2",
        ])
        self.assertRaises(ValueError, lambda: reader.next())


if __name__ == "__main__":
    unittest.main(verbosity=2)
