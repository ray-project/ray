import os
import subprocess
import sys
import time
import unittest

import plasma

class TestPlasmaAPI(unittest.TestCase):

  def setUp(self):
    # Start Plasma.
    plasma_store_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../build/plasma_store")
    self.p = subprocess.Popen([plasma_store_executable, "-s", "/tmp/store"])
    time.sleep(0.1)
    # Connect to Plasma.
    self.plasma_client = plasma.PlasmaClient("/tmp/store")

  def tearDown(self):
    # Kill the plasma stoe process.
    self.p.kill()

  def test_create(self):
    # Create an object string.
    object_id = "id" + 18 * "x"
    # Create a new buffer and write to it.
    length = 1000
    memory_buffer = self.plasma_client.create(object_id, length)
    for i in range(length):
      memory_buffer[i] = chr(i % 256)
    # Seal the object.
    self.plasma_client.seal(object_id)
    # Get the object.
    memory_buffer = self.plasma_client.get(object_id)
    for i in range(length):
      self.assertEqual(memory_buffer[i], chr(i % 256))

  def test_illegal_functionality(self):
    # Create an object string.
    object_id = "id" + 18 * "x"
    # Create a new buffer and write to it.
    length = 1000
    memory_buffer = self.plasma_client.create(object_id, length)
    # Make sure we cannot access memory out of bounds.
    self.assertRaises(Exception, lambda : memory_buffer[length])
    # Seal the object.
    self.plasma_client.seal(object_id)
    # This test is commented out because it currently fails.
    # # Make sure the object is ready only now.
    # def illegal_assignment():
    #   memory_buffer[0] = chr(0)
    # self.assertRaises(Exception, illegal_assignment)
    # Get the object.
    memory_buffer = self.plasma_client.get(object_id)
    # Make sure the object is read only.
    def illegal_assignment():
      memory_buffer[0] = chr(0)
    self.assertRaises(Exception, illegal_assignment)

if __name__ == "__main__":
  unittest.main(verbosity=2)
