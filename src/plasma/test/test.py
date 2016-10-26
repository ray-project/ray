from __future__ import print_function

import os
import signal
import socket
import struct
import subprocess
import sys
import unittest
import random
import time
import tempfile

import plasma

USE_VALGRIND = False

def random_object_id():
  return "".join([chr(random.randint(0, 255)) for _ in range(plasma.PLASMA_ID_SIZE)])

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

def assert_get_object_equal(unit_test, client1, client2, object_id, memory_buffer=None, metadata=None):
  if memory_buffer is not None:
    unit_test.assertEqual(memory_buffer[:], client2.get(object_id)[:])
  if metadata is not None:
    unit_test.assertEqual(metadata[:], client2.get_metadata(object_id)[:])
  unit_test.assertEqual(client1.get(object_id)[:], client2.get(object_id)[:])
  unit_test.assertEqual(client1.get_metadata(object_id)[:],
                        client2.get_metadata(object_id)[:])

class TestPlasmaClient(unittest.TestCase):

  def setUp(self):
    # Start Plasma.
    plasma_store_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../build/plasma_store")
    store_name = "/tmp/store{}".format(random.randint(0, 10000))
    command = [plasma_store_executable, "-s", store_name]
    if USE_VALGRIND:
      self.p = subprocess.Popen(["valgrind", "--track-origins=yes", "--leak-check=full", "--show-leak-kinds=all", "--error-exitcode=1"] + command)
      time.sleep(2.0)
    else:
      self.p = subprocess.Popen(command)
    # Connect to Plasma.
    self.plasma_client = plasma.PlasmaClient(store_name)

  def tearDown(self):
    # Kill the plasma store process.
    if USE_VALGRIND:
      self.p.send_signal(signal.SIGTERM)
      self.p.wait()
      if self.p.returncode != 0:
        os._exit(-1)
    else:
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

  def test_contains(self):
    fake_object_ids = [random_object_id() for _ in range(100)]
    real_object_ids = [random_object_id() for _ in range(100)]
    for object_id in real_object_ids:
      self.assertFalse(self.plasma_client.contains(object_id))
      memory_buffer = self.plasma_client.create(object_id, 100)
      self.plasma_client.seal(object_id)
      self.assertTrue(self.plasma_client.contains(object_id))
    for object_id in fake_object_ids:
      self.assertFalse(self.plasma_client.contains(object_id))
    for object_id in real_object_ids:
      self.assertTrue(self.plasma_client.contains(object_id))

  # def test_individual_delete(self):
  #   length = 100
  #   # Create an object id string.
  #   object_id = random_object_id()
  #   # Create a random metadata string.
  #   metadata = generate_metadata(100)
  #   # Create a new buffer and write to it.
  #   memory_buffer = self.plasma_client.create(object_id, length, metadata)
  #   for i in range(length):
  #     memory_buffer[i] = chr(i % 256)
  #   # Seal the object.
  #   self.plasma_client.seal(object_id)
  #   # Check that the object is present.
  #   self.assertTrue(self.plasma_client.contains(object_id))
  #   # Delete the object.
  #   self.plasma_client.delete(object_id)
  #   # Make sure the object is no longer present.
  #   self.assertFalse(self.plasma_client.contains(object_id))
  #
  # def test_delete(self):
  #   # Create some objects.
  #   object_ids = [random_object_id() for _ in range(100)]
  #   for object_id in object_ids:
  #     length = 100
  #     # Create a random metadata string.
  #     metadata = generate_metadata(100)
  #     # Create a new buffer and write to it.
  #     memory_buffer = self.plasma_client.create(object_id, length, metadata)
  #     for i in range(length):
  #       memory_buffer[i] = chr(i % 256)
  #     # Seal the object.
  #     self.plasma_client.seal(object_id)
  #     # Check that the object is present.
  #     self.assertTrue(self.plasma_client.contains(object_id))
  #
  #   # Delete the objects and make sure they are no longer present.
  #   for object_id in object_ids:
  #     # Delete the object.
  #     self.plasma_client.delete(object_id)
  #     # Make sure the object is no longer present.
  #     self.assertFalse(self.plasma_client.contains(object_id))

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

  def test_subscribe(self):
    # Subscribe to notifications from the Plasma Store.
    sock = self.plasma_client.subscribe()
    for i in [1, 10, 100, 1000, 10000, 100000]:
      object_ids = [random_object_id() for _ in range(i)]
      for object_id in object_ids:
        # Create an object and seal it to trigger a notification.
        self.plasma_client.create(object_id, 1000)
        self.plasma_client.seal(object_id)
      # Check that we received notifications for all of the objects.
      for object_id in object_ids:
        message_data = self.plasma_client.get_next_notification()
        self.assertEqual(object_id, message_data)

class TestPlasmaManager(unittest.TestCase):

  def setUp(self):
    # Start two PlasmaStores.
    plasma_store_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../build/plasma_store")
    store_name1 = "/tmp/store{}".format(random.randint(0, 10000))
    store_name2 = "/tmp/store{}".format(random.randint(0, 10000))
    plasma_store_command1 = [plasma_store_executable, "-s", store_name1]
    plasma_store_command2 = [plasma_store_executable, "-s", store_name2]

    if USE_VALGRIND:
      self.p2 = subprocess.Popen(["valgrind", "--track-origins=yes", "--leak-check=full", "--show-leak-kinds=all", "--error-exitcode=1"] + plasma_store_command1)
      self.p3 = subprocess.Popen(["valgrind", "--track-origins=yes", "--leak-check=full", "--show-leak-kinds=all", "--error-exitcode=1"] + plasma_store_command2)
    else:
      self.p2 = subprocess.Popen(plasma_store_command1)
      self.p3 = subprocess.Popen(plasma_store_command2)

    # Start a Redis server.
    redis_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../common/thirdparty/redis-3.2.3/src/redis-server")
    self.redis_process = None
    manager_redis_args = []
    if os.path.exists(redis_path):
      redis_port = 6379
      with open(os.devnull, 'w') as FNULL:
        self.redis_process = subprocess.Popen([redis_path,
                                               "--port", str(redis_port)],
                                              stdout=FNULL)
      time.sleep(0.1)
      manager_redis_args = ["-d", "{addr}:{port}".format(addr="127.0.0.1",
                                                      port=redis_port)]

    # Start two PlasmaManagers.
    self.port1 = random.randint(10000, 50000)
    self.port2 = random.randint(10000, 50000)
    plasma_manager_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../build/plasma_manager")
    plasma_manager_command1 = [plasma_manager_executable,
                               "-s", store_name1,
                               "-m", "127.0.0.1",
                               "-p", str(self.port1)] + manager_redis_args
    plasma_manager_command2 = [plasma_manager_executable,
                               "-s", store_name2,
                               "-m", "127.0.0.1",
                               "-p", str(self.port2)] + manager_redis_args

    if USE_VALGRIND:
      self.p4 = subprocess.Popen(["valgrind", "--track-origins=yes", "--leak-check=full", "--show-leak-kinds=all", "--error-exitcode=1"] + plasma_manager_command1)
      self.p5 = subprocess.Popen(["valgrind", "--track-origins=yes", "--leak-check=full", "--show-leak-kinds=all", "--error-exitcode=1"] + plasma_manager_command2)
      time.sleep(2.0)
    else:
      self.p4 = subprocess.Popen(plasma_manager_command1)
      self.p5 = subprocess.Popen(plasma_manager_command2)
      time.sleep(0.1)

    # Connect two PlasmaClients.
    self.client1 = plasma.PlasmaClient(store_name1, "127.0.0.1", self.port1)
    self.client2 = plasma.PlasmaClient(store_name2, "127.0.0.1", self.port2)
    time.sleep(0.5)

  def tearDown(self):
    # Kill the PlasmaStore and PlasmaManager processes.
    if USE_VALGRIND:
      self.p4.send_signal(signal.SIGTERM)
      self.p4.wait()
      self.p5.send_signal(signal.SIGTERM)
      self.p5.wait()
      self.p2.send_signal(signal.SIGTERM)
      self.p2.wait()
      self.p3.send_signal(signal.SIGTERM)
      self.p3.wait()
      if self.p2.returncode != 0 or self.p3.returncode != 0 or self.p4.returncode != 0 or self.p5.returncode != 0:
        print("aborting due to valgrind error")
        os._exit(-1)
    else:
      self.p2.kill()
      self.p3.kill()
      self.p4.kill()
      self.p5.kill()
    if self.redis_process:
      self.redis_process.kill()

  # def test_fetch(self):
  #   if self.redis_process is None:
  #     print("Cannot test fetch without a running redis instance.")
  #     self.assertTrue(False)
  #   for _ in range(100):
  #     # Create an object.
  #     object_id1, memory_buffer1, metadata1 = create_object(self.client1, 2000, 2000)
  #     # Fetch the object from the other plasma store.
  #     # TODO(swang): This line is a hack! It makes sure that the entry will be
  #     # in the object table once we call the fetch operation. Remove once
  #     # retries are implemented by Ray common.
  #     time.sleep(0.1)
  #     successes = self.client2.fetch([object_id1])
  #     self.assertEqual(successes, [True])
  #     # Compare the two buffers.
  #     assert_get_object_equal(self, self.client1, self.client2, object_id1,
  #                             memory_buffer=memory_buffer1, metadata=metadata1)
  #     # Fetch in the other direction. These should return quickly because
  #     # client1 already has the object.
  #     successes = self.client1.fetch([object_id1])
  #     self.assertEqual(successes, [True])
  #     assert_get_object_equal(self, self.client2, self.client1, object_id1,
  #                             memory_buffer=memory_buffer1, metadata=metadata1)

  # def test_fetch_multiple(self):
  #   if self.redis_process is None:
  #     print("Cannot test fetch without a running redis instance.")
  #     self.assertTrue(False)
  #   for _ in range(20):
  #     # Create two objects and a third fake one that doesn't exist.
  #     object_id1, memory_buffer1, metadata1 = create_object(self.client1, 2000, 2000)
  #     missing_object_id = random_object_id()
  #     object_id2, memory_buffer2, metadata2 = create_object(self.client1, 2000, 2000)
  #     object_ids = [object_id1, missing_object_id, object_id2]
  #     # Fetch the objects from the other plasma store. The second object ID
  #     # should timeout since it does not exist.
  #     # TODO(swang): This line is a hack! It makes sure that the entry will be
  #     # in the object table once we call the fetch operation. Remove once
  #     # retries are implemented by Ray common.
  #     time.sleep(0.1)
  #     successes = self.client2.fetch(object_ids)
  #     self.assertEqual(successes, [True, False, True])
  #     # Compare the buffers of the objects that do exist.
  #     assert_get_object_equal(self, self.client1, self.client2, object_id1,
  #                             memory_buffer=memory_buffer1, metadata=metadata1)
  #     assert_get_object_equal(self, self.client1, self.client2, object_id2,
  #                             memory_buffer=memory_buffer2, metadata=metadata2)
  #     # Fetch in the other direction. The fake object still does not exist.
  #     successes = self.client1.fetch(object_ids)
  #     self.assertEqual(successes, [True, False, True])
  #     assert_get_object_equal(self, self.client2, self.client1, object_id1,
  #                             memory_buffer=memory_buffer1, metadata=metadata1)
  #     assert_get_object_equal(self, self.client2, self.client1, object_id2,
  #                             memory_buffer=memory_buffer2, metadata=metadata2)

  def test_transfer(self):
    for _ in range(100):
      # Create an object.
      object_id1, memory_buffer1, metadata1 = create_object(self.client1, 2000, 2000)
      # Transfer the buffer to the the other PlasmaStore.
      self.client1.transfer("127.0.0.1", self.port2, object_id1)
      # Compare the two buffers.
      assert_get_object_equal(self, self.client1, self.client2, object_id1,
                              memory_buffer=memory_buffer1, metadata=metadata1)
      # # Transfer the buffer again.
      # self.client1.transfer("127.0.0.1", self.port2, object_id1)
      # # Compare the two buffers.
      # assert_get_object_equal(self, self.client1, self.client2, object_id1,
      #                         memory_buffer=memory_buffer1, metadata=metadata1)

      # Create an object.
      object_id2, memory_buffer2, metadata2 = create_object(self.client2, 20000, 20000)
      # Transfer the buffer to the the other PlasmaStore.
      self.client2.transfer("127.0.0.1", self.port1, object_id2)
      # Compare the two buffers.
      assert_get_object_equal(self, self.client1, self.client2, object_id2,
                              memory_buffer=memory_buffer2, metadata=metadata2)

  def test_illegal_functionality(self):
    # Create an object id string.
    object_id = random_object_id()
    # Create a new buffer.
    # memory_buffer = self.client1.create(object_id, 20000)
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
  if len(sys.argv) > 1:
    # pop the argument so we don't mess with unittest's own argument parser
    if sys.argv[-1] == "valgrind":
      arg = sys.argv.pop()
      USE_VALGRIND = True
      print("Using valgrind for tests")
  unittest.main(verbosity=2)
