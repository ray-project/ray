from __future__ import print_function

import numpy as np
import os
import random
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import plasma

USE_VALGRIND = False
PLASMA_STORE_MEMORY = 1000000000

def random_object_id():
  return np.random.bytes(20)

def generate_metadata(length):
  metadata = length * ["\x00"]
  if length > 0:
    metadata[0] = chr(random.randint(0, 255))
    metadata[-1] = chr(random.randint(0, 255))
    for _ in range(100):
      metadata[random.randint(0, length - 1)] = chr(random.randint(0, 255))
  return bytearray("".join(metadata))

def write_to_data_buffer(buff, length):
  if length > 0:
    buff[0] = chr(random.randint(0, 255))
    buff[-1] = chr(random.randint(0, 255))
    for _ in range(100):
      buff[random.randint(0, length - 1)] = chr(random.randint(0, 255))

def create_object_with_id(client, object_id, data_size, metadata_size, seal=True):
  metadata = generate_metadata(metadata_size)
  memory_buffer = client.create(object_id, data_size, metadata)
  write_to_data_buffer(memory_buffer, data_size)
  if seal:
    client.seal(object_id)
  return memory_buffer, metadata

def create_object(client, data_size, metadata_size, seal=True):
  object_id = random_object_id()
  memory_buffer, metadata = create_object_with_id(client, object_id, data_size, metadata_size, seal=seal)
  return object_id, memory_buffer, metadata

def assert_get_object_equal(unit_test, client1, client2, object_id, memory_buffer=None, metadata=None):
  if memory_buffer is not None:
    unit_test.assertEqual(memory_buffer[:], client2.get(object_id)[:])
  if metadata is not None:
    unit_test.assertEqual(metadata[:], client2.get_metadata(object_id)[:])
  unit_test.assertEqual(client1.get(object_id)[:], client2.get(object_id)[:])
  unit_test.assertEqual(client1.get_metadata(object_id)[:],
                        client2.get_metadata(object_id)[:])

# Check if the redis-server binary is present.
redis_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../common/thirdparty/redis/src/redis-server")
if not os.path.exists(redis_path):
  raise Exception("You do not have the redis-server binary. Run `make test` in the plasma directory to get it.")

class TestPlasmaClient(unittest.TestCase):

  def setUp(self):
    # Start Plasma store.
    plasma_store_name, self.p = plasma.start_plasma_store(use_valgrind=USE_VALGRIND)
    # Connect to Plasma.
    self.plasma_client = plasma.PlasmaClient(plasma_store_name, None, 64)
    # For the eviction test
    self.plasma_client2 = plasma.PlasmaClient(plasma_store_name, None, 0)

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

  def test_create_existing(self):
    # This test is partially used to test the code path in which we create an
    # object with an ID that already exists
    length = 100
    for _ in range(1000):
      object_id = random_object_id()
      self.plasma_client.create(object_id, length, generate_metadata(length))
      try:
        val = self.plasma_client.create(object_id, length, generate_metadata(length))
      except Exception:
        pass

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

  def test_evict(self):
    client = self.plasma_client2
    object_id1 = random_object_id()
    b1 = client.create(object_id1, 1000)
    client.seal(object_id1)
    del b1
    self.assertEqual(client.evict(1), 1000)

    object_id2 = random_object_id()
    object_id3 = random_object_id()
    b2 = client.create(object_id2, 999)
    b3 = client.create(object_id3, 998)
    client.seal(object_id3)
    del b3
    self.assertEqual(client.evict(1000), 998)

    object_id4 = random_object_id()
    b4 = client.create(object_id4, 997)
    client.seal(object_id4)
    del b4
    client.seal(object_id2)
    del b2
    self.assertEqual(client.evict(1), 997)
    self.assertEqual(client.evict(1), 999)

    object_id5 = random_object_id()
    object_id6 = random_object_id()
    object_id7 = random_object_id()
    b5 = client.create(object_id5, 996)
    b6 = client.create(object_id6, 995)
    b7 = client.create(object_id7, 994)
    client.seal(object_id5)
    client.seal(object_id6)
    client.seal(object_id7)
    del b5
    del b6
    del b7
    self.assertEqual(client.evict(2000), 996 + 995 + 994)

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
    store_name1, self.p2 = plasma.start_plasma_store(use_valgrind=USE_VALGRIND)
    store_name2, self.p3 = plasma.start_plasma_store(use_valgrind=USE_VALGRIND)
    # Start a Redis server.
    redis_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../common/thirdparty/redis/src/redis-server")
    redis_port = 6379
    with open(os.devnull, "w") as FNULL:
      self.redis_process = subprocess.Popen([redis_path,
                                             "--port", str(redis_port)],
                                             stdout=FNULL)
    time.sleep(0.1)
    # Start two PlasmaManagers.
    redis_address = "{}:{}".format("127.0.0.1", redis_port)
    manager_name1, self.p4, self.port1 = plasma.start_plasma_manager(store_name1, redis_address, use_valgrind=USE_VALGRIND)
    manager_name2, self.p5, self.port2 = plasma.start_plasma_manager(store_name2, redis_address, use_valgrind=USE_VALGRIND)
    # Connect two PlasmaClients.
    self.client1 = plasma.PlasmaClient(store_name1, manager_name1)
    self.client2 = plasma.PlasmaClient(store_name2, manager_name2)

  def tearDown(self):
    # Kill the PlasmaStore and PlasmaManager processes.
    if USE_VALGRIND:
      time.sleep(1) # give processes opportunity to finish work
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
    self.redis_process.kill()

  def test_fetch(self):
    if self.redis_process is None:
      print("Cannot test fetch without a running redis instance.")
      self.assertTrue(False)
    for _ in range(100):
      # Create an object.
      object_id1, memory_buffer1, metadata1 = create_object(self.client1, 2000, 2000)
      # Fetch the object from the other plasma store.
      # TODO(swang): This line is a hack! It makes sure that the entry will be
      # in the object table once we call the fetch operation. Remove once
      # retries are implemented by Ray common.
      time.sleep(0.1)
      successes = self.client2.fetch([object_id1])
      self.assertEqual(successes, [True])
      # Compare the two buffers.
      assert_get_object_equal(self, self.client1, self.client2, object_id1,
                              memory_buffer=memory_buffer1, metadata=metadata1)
      # Fetch in the other direction. These should return quickly because
      # client1 already has the object.
      successes = self.client1.fetch([object_id1])
      self.assertEqual(successes, [True])
      assert_get_object_equal(self, self.client2, self.client1, object_id1,
                              memory_buffer=memory_buffer1, metadata=metadata1)

  def test_fetch_multiple(self):
    if self.redis_process is None:
      print("Cannot test fetch without a running redis instance.")
      self.assertTrue(False)
    for _ in range(20):
      # Create two objects and a third fake one that doesn't exist.
      object_id1, memory_buffer1, metadata1 = create_object(self.client1, 2000, 2000)
      missing_object_id = random_object_id()
      object_id2, memory_buffer2, metadata2 = create_object(self.client1, 2000, 2000)
      object_ids = [object_id1, missing_object_id, object_id2]
      # Fetch the objects from the other plasma store. The second object ID
      # should timeout since it does not exist.
      # TODO(swang): This line is a hack! It makes sure that the entry will be
      # in the object table once we call the fetch operation. Remove once
      # retries are implemented by Ray common.
      time.sleep(0.1)
      successes = self.client2.fetch(object_ids)
      self.assertEqual(successes, [True, False, True])
      # Compare the buffers of the objects that do exist.
      assert_get_object_equal(self, self.client1, self.client2, object_id1,
                              memory_buffer=memory_buffer1, metadata=metadata1)
      assert_get_object_equal(self, self.client1, self.client2, object_id2,
                              memory_buffer=memory_buffer2, metadata=metadata2)
      # Fetch in the other direction. The fake object still does not exist.
      successes = self.client1.fetch(object_ids)
      self.assertEqual(successes, [True, False, True])
      assert_get_object_equal(self, self.client2, self.client1, object_id1,
                              memory_buffer=memory_buffer1, metadata=metadata1)
      assert_get_object_equal(self, self.client2, self.client1, object_id2,
                              memory_buffer=memory_buffer2, metadata=metadata2)

    # Check that calling fetch with the same object ID fails.
    object_id = random_object_id()
    self.assertRaises(Exception, lambda : self.client1.fetch([object_id, object_id]))

  def test_fetch2(self):
    if self.redis_process is None:
      print("Cannot test fetch without a running redis instance.")
      self.assertTrue(False)
    for _ in range(10):
      # Create an object.
      object_id1, memory_buffer1, metadata1 = create_object(self.client1, 2000, 2000)
      self.client1.fetch2([object_id1])
      self.assertEqual(self.client1.contains(object_id1), True)
      self.assertEqual(self.client2.contains(object_id1), False)
      # Fetch the object from the other plasma manager.
      # TODO(rkn): Right now we must wait for the object table to be updated.
      while not self.client2.contains(object_id1):
        self.client2.fetch2([object_id1])
      # Compare the two buffers.
      assert_get_object_equal(self, self.client1, self.client2, object_id1,
                              memory_buffer=memory_buffer1, metadata=metadata1)

    # Test that we can call fetch on object IDs that don't exist yet.
    object_id2 = random_object_id()
    self.client1.fetch2([object_id2])
    self.assertEqual(self.client1.contains(object_id2), False)
    memory_buffer2, metadata2 = create_object_with_id(self.client2, object_id2, 2000, 2000)
    # # Check that the object has been fetched.
    # self.assertEqual(self.client1.contains(object_id2), True)
    # Compare the two buffers.
    # assert_get_object_equal(self, self.client1, self.client2, object_id2,
    #                         memory_buffer=memory_buffer2, metadata=metadata2)

    # Test calling the same fetch request a bunch of times.
    object_id3 = random_object_id()
    self.assertEqual(self.client1.contains(object_id3), False)
    self.assertEqual(self.client2.contains(object_id3), False)
    for _ in range(10):
      self.client1.fetch2([object_id3])
      self.client2.fetch2([object_id3])
    memory_buffer3, metadata3 = create_object_with_id(self.client1, object_id3, 2000, 2000)
    for _ in range(10):
      self.client1.fetch2([object_id3])
      self.client2.fetch2([object_id3])
    #TODO(rkn): Right now we must wait for the object table to be updated.
    while not self.client2.contains(object_id3):
      self.client2.fetch2([object_id3])
    assert_get_object_equal(self, self.client1, self.client2, object_id3,
                            memory_buffer=memory_buffer3, metadata=metadata3)

  def test_fetch2_multiple(self):
    if self.redis_process is None:
      print("Cannot test fetch without a running redis instance.")
      self.assertTrue(False)
    for _ in range(20):
      # Create two objects and a third fake one that doesn't exist.
      object_id1, memory_buffer1, metadata1 = create_object(self.client1, 2000, 2000)
      missing_object_id = random_object_id()
      object_id2, memory_buffer2, metadata2 = create_object(self.client1, 2000, 2000)
      object_ids = [object_id1, missing_object_id, object_id2]
      # Fetch the objects from the other plasma store. The second object ID
      # should timeout since it does not exist.
      # TODO(rkn): Right now we must wait for the object table to be updated.
      while (not self.client2.contains(object_id1)) or (not self.client2.contains(object_id2)):
        self.client2.fetch2(object_ids)
      # Compare the buffers of the objects that do exist.
      assert_get_object_equal(self, self.client1, self.client2, object_id1,
                              memory_buffer=memory_buffer1, metadata=metadata1)
      assert_get_object_equal(self, self.client1, self.client2, object_id2,
                              memory_buffer=memory_buffer2, metadata=metadata2)
      # Fetch in the other direction. The fake object still does not exist.
      self.client1.fetch2(object_ids)
      assert_get_object_equal(self, self.client2, self.client1, object_id1,
                              memory_buffer=memory_buffer1, metadata=metadata1)
      assert_get_object_equal(self, self.client2, self.client1, object_id2,
                              memory_buffer=memory_buffer2, metadata=metadata2)

    # Check that we can call fetch with duplicated object IDs.
    object_id3 = random_object_id()
    self.client1.fetch2([object_id3, object_id3])
    object_id4, memory_buffer4, metadata4 = create_object(self.client1, 2000, 2000)
    time.sleep(0.1)
    # TODO(rkn): Right now we must wait for the object table to be updated.
    while not self.client2.contains(object_id4):
      self.client2.fetch2([object_id3, object_id3, object_id4, object_id4])
    assert_get_object_equal(self, self.client2, self.client1, object_id4,
                            memory_buffer=memory_buffer4, metadata=metadata4)

  def test_wait(self):
    # Test timeout.
    obj_id0 = random_object_id()
    self.client1.wait([obj_id0], timeout=100, num_returns=1)
    # If we get here, the test worked.

    # Test wait if local objects available.
    obj_id1 = random_object_id()
    self.client1.create(obj_id1, 1000)
    self.client1.seal(obj_id1)
    ready, waiting = self.client1.wait([obj_id1], timeout=100, num_returns=1)
    self.assertEqual(len(ready), 1)
    self.assertEqual(ready[0], obj_id1)
    self.assertEqual(len(waiting), 0)

    # Test wait if only one object available and only one object waited for.
    obj_id2 = random_object_id()
    self.client1.create(obj_id2, 1000)
    # Don't seal.
    ready, waiting = self.client1.wait([obj_id2, obj_id1], timeout=100, num_returns=1)
    self.assertEqual(len(ready), 1)
    self.assertEqual(ready[0], obj_id1)
    self.assertEqual(len(waiting), 1)
    self.assertEqual(waiting[0], obj_id2)

    # Test wait if object is sealed later.
    obj_id3 = random_object_id()

    def finish():
      self.client2.create(obj_id3, 1000)
      self.client2.seal(obj_id3)
      self.client2.transfer("127.0.0.1", self.port1, obj_id3)

    t = threading.Timer(0.1, finish)
    t.start()
    ready, waiting = self.client1.wait([obj_id3, obj_id2, obj_id1], timeout=1000, num_returns=2)
    self.assertEqual(len(ready), 2)
    self.assertTrue((ready[0] == obj_id1 and ready[1] == obj_id3) or (ready[0] == obj_id3 and ready[1] == obj_id1))
    self.assertEqual(len(waiting), 1)
    self.assertTrue(waiting[0] == obj_id2)

    # Test if the appropriate number of objects is shown if some objects are not ready
    ready, wait = self.client1.wait([obj_id3, obj_id2, obj_id1], 100, 3)
    self.assertEqual(len(ready), 2)
    self.assertTrue((ready[0] == obj_id1 and ready[1] == obj_id3) or (ready[0] == obj_id3 and ready[1] == obj_id1))
    self.assertEqual(len(waiting), 1)
    self.assertTrue(waiting[0] == obj_id2)

    # Don't forget to seal obj_id2.
    self.client1.seal(obj_id2)

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
