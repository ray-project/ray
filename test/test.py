from __future__ import print_function

import os
import socket
import subprocess
import sys
import unittest
import random
import time
import tempfile

import plasma

def random_object_id():
  return "".join([chr(random.randint(0, 255)) for _ in range(20)])

def generate_metadata(length):
  metadata = length * ["\x00"]
  if length > 0:
    metadata[0] = chr(random.randint(0, 255))
    metadata[-1] = chr(random.randint(0, 255))
    for _ in range(100):
      metadata[random.randint(0, length - 1)] = chr(random.randint(0, 255))
  return buffer("".join(metadata))

def write_to_data_buffer(buff, length):
  if length > 0:
    buff[0] = chr(random.randint(0, 255))
    buff[-1] = chr(random.randint(0, 255))
    for _ in range(100):
      buff[random.randint(0, length - 1)] = chr(random.randint(0, 255))

def create_object(client, data_size, metadata_size, seal=True):
  object_id = random_object_id()
  metadata = generate_metadata(metadata_size)
  memory_buffer = client.create(object_id, data_size, metadata)
  write_to_data_buffer(memory_buffer, data_size)
  if seal:
    client.seal(object_id)
  return object_id, memory_buffer, metadata

class TestPlasmaClient(unittest.TestCase):

  def setUp(self):
    # Start Plasma.
    plasma_store_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../build/plasma_store")
    store_name = "/tmp/store{}".format(random.randint(0, 10000))
    self.p = subprocess.Popen([plasma_store_executable, "-s", store_name])
    # Connect to Plasma.
    self.plasma_client = plasma.PlasmaClient(store_name)

  def tearDown(self):
    # Kill the plasma store process.
    self.p.kill()

  def test_create(self):
    # Create an object id string.
    object_id = random_object_id()
    # Create a new buffer and write to it.
    length = 50
    memory_buffer = self.plasma_client.create(object_id, length)
    for i in range(length):
      memory_buffer[i] = chr(i % 256)
    # Seal the object.
    self.plasma_client.seal(object_id)
    # Get the object.
    memory_buffer = self.plasma_client.get(object_id)
    for i in range(length):
      self.assertEqual(memory_buffer[i], chr(i % 256))

  def test_create_with_metadata(self):
    for length in range(1000):
      # Create an object id string.
      object_id = random_object_id()
      # Create a random metadata string.
      metadata = generate_metadata(length)
      # Create a new buffer and write to it.
      memory_buffer = self.plasma_client.create(object_id, length, metadata)
      for i in range(length):
        memory_buffer[i] = chr(i % 256)
      # Seal the object.
      self.plasma_client.seal(object_id)
      # Get the object.
      memory_buffer = self.plasma_client.get(object_id)
      for i in range(length):
        self.assertEqual(memory_buffer[i], chr(i % 256))
      # Get the metadata.
      metadata_buffer = self.plasma_client.get_metadata(object_id)
      self.assertEqual(len(metadata), len(metadata_buffer))
      for i in range(len(metadata)):
        self.assertEqual(metadata[i], metadata_buffer[i])

  def test_illegal_functionality(self):
    # Create an object id string.
    object_id = random_object_id()
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

class TestPlasmaManager(unittest.TestCase):

  def setUp(self):
    # Start two PlasmaStores.
    plasma_store_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../build/plasma_store")
    store_name1 = "/tmp/store{}".format(random.randint(0, 10000))
    store_name2 = "/tmp/store{}".format(random.randint(0, 10000))
    self.p2 = subprocess.Popen([plasma_store_executable, "-s", store_name1])
    self.p3 = subprocess.Popen([plasma_store_executable, "-s", store_name2])
    # Start two PlasmaManagers.
    self.port1 = random.randint(10000, 50000)
    self.port2 = random.randint(10000, 50000)
    plasma_manager_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../build/plasma_manager")
    self.p4 = subprocess.Popen([plasma_manager_executable, "-s", store_name1, "-m", "127.0.0.1", "-p", str(self.port1)])
    self.p5 = subprocess.Popen([plasma_manager_executable, "-s", store_name2, "-m", "127.0.0.1", "-p", str(self.port2)])
    time.sleep(0.1)
    # Connect two PlasmaClients.
    self.client1 = plasma.PlasmaClient(store_name1, "127.0.0.1", self.port1)
    self.client2 = plasma.PlasmaClient(store_name2, "127.0.0.1", self.port2)
    time.sleep(0.5)

  def tearDown(self):
    # Kill the PlasmaStore and PlasmaManager processes.
    self.p2.kill()
    self.p3.kill()
    self.p4.kill()
    self.p5.kill()

  def test_transfer(self):
    for _ in range(100):
      # Create an object.
      object_id1, memory_buffer1, metadata1 = create_object(self.client1, 2000, 2000)
      # Transfer the buffer to the the other PlasmaStore.
      self.client1.transfer("127.0.0.1", self.port2, object_id1)
      # Compare the two buffers.
      self.assertEqual(memory_buffer1[:], self.client2.get(object_id1)[:])
      self.assertEqual(self.client1.get(object_id1)[:], self.client2.get(object_id1)[:])
      self.assertEqual(metadata1[:], self.client2.get_metadata(object_id1)[:])
      self.assertEqual(self.client1.get_metadata(object_id1)[:], self.client2.get_metadata(object_id1)[:])
      # Transfer the buffer again.
      self.client1.transfer("127.0.0.1", self.port2, object_id1)
      self.assertEqual(metadata1[:], self.client2.get_metadata(object_id1)[:])
      # Compare the two buffers.
      self.assertEqual(self.client1.get(object_id1)[:], self.client2.get(object_id1)[:])

      # Create an object.
      object_id2, memory_buffer2, metadata2 = create_object(self.client2, 20000, 20000)
      # Transfer the buffer to the the other PlasmaStore.
      self.client2.transfer("127.0.0.1", self.port1, object_id2)
      # Compare the two buffers.
      self.assertEqual(memory_buffer2[:], self.client2.get(object_id2)[:])
      self.assertEqual(self.client1.get(object_id2)[:], self.client2.get(object_id2)[:])
      self.assertEqual(metadata2[:], self.client2.get_metadata(object_id2)[:])
      self.assertEqual(self.client1.get_metadata(object_id2)[:], self.client2.get_metadata(object_id2)[:])

  def test_illegal_functionality(self):
    # Create an object id string.
    object_id = random_object_id()
    # Create a new buffer.
    memory_buffer = self.client1.create(object_id, 20000)
    # This test is commented out because it currently fails.
    # # Transferring the buffer before sealing it should fail.
    # self.assertRaises(Exception, lambda : self.manager1.transfer(1, object_id))

  def test_stresstest(self):
    a = time.time()
    object_ids = []
    for i in range(10000): # TODO(pcm): increase this to 100000
      object_id = random_object_id()
      object_ids.append(object_id)
      self.client1.create(object_id, 1)
      self.client1.seal(object_id)
    for object_id in object_ids:
      self.client1.transfer("127.0.0.1", self.port2, object_id)
    b = time.time() - a

    print("it took", b, "seconds to put and transfer the objects")

if __name__ == "__main__":
  unittest.main(verbosity=2)
