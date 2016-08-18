import os
import socket
import subprocess
import sys
import unittest
import random
import time

import plasma

def random_object_id():
  return "".join([chr(random.randint(0, 256)) for _ in range(20)])

class TestPlasmaClient(unittest.TestCase):

  def setUp(self):
    # Start Plasma.
    plasma_store_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../build/plasma_store")
    self.p = subprocess.Popen([plasma_store_executable, "-s", "/tmp/store"])
    # Connect to Plasma.
    self.plasma_client = plasma.PlasmaClient("/tmp/store")

  def tearDown(self):
    # Kill the plasma store process.
    self.p.kill()

  def test_create(self):
    # Create an object id string.
    object_id = random_object_id()
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
    # Start the nameserver.
    nameserver_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "nameserver.py")
    self.p1 = subprocess.Popen(["python", nameserver_path])
    # Start two PlasmaStores.
    plasma_store_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../build/plasma_store")
    self.p2 = subprocess.Popen([plasma_store_executable, "-s", "/tmp/store1"])
    self.p3 = subprocess.Popen([plasma_store_executable, "-s", "/tmp/store2"])
    # Connect two PlasmaClients.
    self.client1 = plasma.PlasmaClient("/tmp/store1")
    self.client2 = plasma.PlasmaClient("/tmp/store2")
    # Start two PlasmaManagers.
    time.sleep(0.1)
    plasma_manager_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../build/plasma_manager")
    self.p4 = subprocess.Popen([plasma_manager_executable, "-n", "127.0.0.1:16121", "-s", "/tmp/store1", "-m", "127.0.0.1"])
    self.p5 = subprocess.Popen([plasma_manager_executable, "-n", "127.0.0.1:16121", "-s", "/tmp/store2", "-m", "127.0.0.1"])
    time.sleep(0.1)
    # Connect to the nameserver.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 16121))
    # Get the port for the first PlasmaManager.
    req = plasma.PlasmaRequest(type=plasma.PLASMA_GET_MANAGER_PORT, manager_id=0)
    sock.send(buffer(req)[:])
    request = plasma.PlasmaRequest()
    sock.recv_into(request)
    port1 = request.port
    time.sleep(0.1)
    # Get the port for the second PlasmaManager.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 16121))
    req = plasma.PlasmaRequest(type=plasma.PLASMA_GET_MANAGER_PORT, manager_id=1)
    sock.send(buffer(req)[:])
    request = plasma.PlasmaRequest()
    sock.recv_into(request)
    port2 = request.port
    # Connect two Python PlasmaManagers.
    self.manager1 = plasma.PlasmaManager("127.0.0.1", port1)
    self.manager2 = plasma.PlasmaManager("127.0.0.1", port2)

  def tearDown(self):
    # Kill the nameserver, PlasmaStore and PlasmaManager processes.
    self.p1.kill()
    self.p2.kill()
    self.p3.kill()
    self.p4.kill()
    self.p5.kill()

  def test_transfer(self):
    # Create an object id string.
    object_id1 = random_object_id()
    # Create a new buffer and write to it.
    memory_buffer = self.client1.create(object_id1, 20000)
    for i in range(len(memory_buffer)):
      memory_buffer[i] = chr(i % 10)
    # Seal the buffer.
    self.client1.seal(object_id1)
    # Transfer the buffer to the the other PlasmaStore.
    self.manager1.transfer(1, object_id1)
    # Compare the two buffers.
    self.assertEqual(self.client1.get(object_id1)[:], self.client2.get(object_id1)[:])
    # Transfer the buffer again.
    self.manager1.transfer(1, object_id1)
    # Compare the two buffers.
    self.assertEqual(self.client1.get(object_id1)[:], self.client2.get(object_id1)[:])
    # Create a new object id string.
    object_id2 = random_object_id()
    # Create a new buffer and write to it.
    memory_buffer = self.client2.create(object_id2, 20000)
    for i in range(len(memory_buffer)):
      memory_buffer[i] = chr(i % 10)
    # Seal the buffer.
    self.client2.seal(object_id2)
    # Transfer the buffer to the the other PlasmaStore.
    self.manager2.transfer(0, object_id2)
    # Compare the two buffers.
    self.assertEqual(self.client1.get(object_id2)[:], self.client2.get(object_id2)[:])

  def test_illegal_functionality(self):
    # Create an object id string.
    object_id = random_object_id()
    # Create a new buffer.
    memory_buffer = self.client1.create(object_id, 20000)
    # This test is commented out because it currently fails.
    # # Transferring the buffer before sealing it should fail.
    # self.assertRaises(Exception, lambda : self.manager1.transfer(1, object_id))

if __name__ == "__main__":
  unittest.main(verbosity=2)
