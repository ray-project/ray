from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import os
import random
import signal
import sys
import threading
import time
import unittest

import ray.plasma as plasma
from ray.plasma.utils import (random_object_id, generate_metadata,
                              create_object_with_id, create_object)
from ray import services

USE_VALGRIND = False
PLASMA_STORE_MEMORY = 1000000000


def assert_get_object_equal(unit_test, client1, client2, object_id,
                            memory_buffer=None, metadata=None):
    client1_buff = client1.get([object_id])[0]
    client2_buff = client2.get([object_id])[0]
    client1_metadata = client1.get_metadata([object_id])[0]
    client2_metadata = client2.get_metadata([object_id])[0]
    unit_test.assertEqual(len(client1_buff), len(client2_buff))
    unit_test.assertEqual(len(client1_metadata), len(client2_metadata))
    # Check that the buffers from the two clients are the same.
    unit_test.assertTrue(plasma.buffers_equal(client1_buff, client2_buff))
    # Check that the metadata buffers from the two clients are the same.
    unit_test.assertTrue(plasma.buffers_equal(client1_metadata,
                                              client2_metadata))
    # If a reference buffer was provided, check that it is the same as well.
    if memory_buffer is not None:
        unit_test.assertTrue(plasma.buffers_equal(memory_buffer, client1_buff))
    # If reference metadata was provided, check that it is the same as well.
    if metadata is not None:
        unit_test.assertTrue(plasma.buffers_equal(metadata, client1_metadata))


class TestPlasmaClient(unittest.TestCase):

    def setUp(self):
        # Start Plasma store.
        plasma_store_name, self.p = plasma.start_plasma_store(
            use_valgrind=USE_VALGRIND)
        # Connect to Plasma.
        self.plasma_client = plasma.PlasmaClient(plasma_store_name, None, 64)
        # For the eviction test
        self.plasma_client2 = plasma.PlasmaClient(plasma_store_name, None, 0)

    def tearDown(self):
        # Check that the Plasma store is still alive.
        self.assertEqual(self.p.poll(), None)
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
        memory_buffer = self.plasma_client.get([object_id])[0]
        for i in range(length):
            self.assertEqual(memory_buffer[i], chr(i % 256))

    def test_create_with_metadata(self):
        for length in range(1000):
            # Create an object id string.
            object_id = random_object_id()
            # Create a random metadata string.
            metadata = generate_metadata(length)
            # Create a new buffer and write to it.
            memory_buffer = self.plasma_client.create(object_id, length,
                                                      metadata)
            for i in range(length):
                memory_buffer[i] = chr(i % 256)
            # Seal the object.
            self.plasma_client.seal(object_id)
            # Get the object.
            memory_buffer = self.plasma_client.get([object_id])[0]
            for i in range(length):
                self.assertEqual(memory_buffer[i], chr(i % 256))
            # Get the metadata.
            metadata_buffer = self.plasma_client.get_metadata([object_id])[0]
            self.assertEqual(len(metadata), len(metadata_buffer))
            for i in range(len(metadata)):
                self.assertEqual(chr(metadata[i]), metadata_buffer[i])

    def test_create_existing(self):
        # This test is partially used to test the code path in which we create
        # an object with an ID that already exists
        length = 100
        for _ in range(1000):
            object_id = random_object_id()
            self.plasma_client.create(object_id, length,
                                      generate_metadata(length))
            try:
                self.plasma_client.create(object_id, length,
                                          generate_metadata(length))
            except plasma.plasma_object_exists_error as e:
                pass
            else:
                self.assertTrue(False)

    def test_get(self):
        num_object_ids = 100
        # Test timing out of get with various timeouts.
        for timeout in [0, 10, 100, 1000]:
            object_ids = [random_object_id() for _ in range(num_object_ids)]
            results = self.plasma_client.get(object_ids, timeout_ms=timeout)
            self.assertEqual(results, num_object_ids * [None])

        data_buffers = []
        metadata_buffers = []
        for i in range(num_object_ids):
            if i % 2 == 0:
                data_buffer, metadata_buffer = create_object_with_id(
                    self.plasma_client, object_ids[i], 2000, 2000)
                data_buffers.append(data_buffer)
                metadata_buffers.append(metadata_buffer)

        # Test timing out from some but not all get calls with various
        # timeouts.
        for timeout in [0, 10, 100, 1000]:
            data_results = self.plasma_client.get(object_ids,
                                                  timeout_ms=timeout)
            for i in range(num_object_ids):
                if i % 2 == 0:
                    self.assertTrue(plasma.buffers_equal(data_buffers[i // 2],
                                                         data_results[i]))
                else:
                    self.assertIsNone(results[i])

    def test_store_full(self):
        # The store is started with 1GB, so make sure that create throws an
        # exception when it is full.
        def assert_create_raises_plasma_full(unit_test, size):
            partial_size = np.random.randint(size)
            try:
                _, memory_buffer, _ = create_object(unit_test.plasma_client,
                                                    partial_size,
                                                    size - partial_size)
            except plasma.plasma_out_of_memory_error as e:
                pass
            else:
                # For some reason the above didn't throw an exception, so fail.
                unit_test.assertTrue(False)

        # Create a list to keep some of the buffers in scope.
        memory_buffers = []
        _, memory_buffer, _ = create_object(self.plasma_client, 5 * 10 ** 8, 0)
        memory_buffers.append(memory_buffer)
        # Remaining space is 5 * 10 ** 8. Make sure that we can't create an
        # object of size 5 * 10 ** 8 + 1, but we can create one of size
        # 2 * 10 ** 8.
        assert_create_raises_plasma_full(self, 5 * 10 ** 8 + 1)
        _, memory_buffer, _ = create_object(self.plasma_client, 2 * 10 ** 8, 0)
        del memory_buffer
        _, memory_buffer, _ = create_object(self.plasma_client, 2 * 10 ** 8, 0)
        del memory_buffer
        assert_create_raises_plasma_full(self, 5 * 10 ** 8 + 1)

        _, memory_buffer, _ = create_object(self.plasma_client, 2 * 10 ** 8, 0)
        memory_buffers.append(memory_buffer)
        # Remaining space is 3 * 10 ** 8.
        assert_create_raises_plasma_full(self, 3 * 10 ** 8 + 1)

        _, memory_buffer, _ = create_object(self.plasma_client, 10 ** 8, 0)
        memory_buffers.append(memory_buffer)
        # Remaining space is 2 * 10 ** 8.
        assert_create_raises_plasma_full(self, 2 * 10 ** 8 + 1)

    def test_contains(self):
        fake_object_ids = [random_object_id() for _ in range(100)]
        real_object_ids = [random_object_id() for _ in range(100)]
        for object_id in real_object_ids:
            self.assertFalse(self.plasma_client.contains(object_id))
            self.plasma_client.create(object_id, 100)
            self.plasma_client.seal(object_id)
            self.assertTrue(self.plasma_client.contains(object_id))
        for object_id in fake_object_ids:
            self.assertFalse(self.plasma_client.contains(object_id))
        for object_id in real_object_ids:
            self.assertTrue(self.plasma_client.contains(object_id))

    def test_hash(self):
        # Check the hash of an object that doesn't exist.
        object_id1 = random_object_id()
        self.plasma_client.hash(object_id1)

        length = 1000
        # Create a random object, and check that the hash function always
        # returns the same value.
        metadata = generate_metadata(length)
        memory_buffer = self.plasma_client.create(object_id1, length, metadata)
        for i in range(length):
            memory_buffer[i] = chr(i % 256)
        self.plasma_client.seal(object_id1)
        self.assertEqual(self.plasma_client.hash(object_id1),
                         self.plasma_client.hash(object_id1))

        # Create a second object with the same value as the first, and check
        # that their hashes are equal.
        object_id2 = random_object_id()
        memory_buffer = self.plasma_client.create(object_id2, length, metadata)
        for i in range(length):
            memory_buffer[i] = chr(i % 256)
        self.plasma_client.seal(object_id2)
        self.assertEqual(self.plasma_client.hash(object_id1),
                         self.plasma_client.hash(object_id2))

        # Create a third object with a different value from the first two, and
        # check that its hash is different.
        object_id3 = random_object_id()
        metadata = generate_metadata(length)
        memory_buffer = self.plasma_client.create(object_id3, length, metadata)
        for i in range(length):
            memory_buffer[i] = chr((i + 1) % 256)
        self.plasma_client.seal(object_id3)
        self.assertNotEqual(self.plasma_client.hash(object_id1),
                            self.plasma_client.hash(object_id3))

        # Create a fourth object with the same value as the third, but
        # different metadata. Check that its hash is different from any of the
        # previous three.
        object_id4 = random_object_id()
        metadata4 = generate_metadata(length)
        memory_buffer = self.plasma_client.create(object_id4, length,
                                                  metadata4)
        for i in range(length):
            memory_buffer[i] = chr((i + 1) % 256)
        self.plasma_client.seal(object_id4)
        self.assertNotEqual(self.plasma_client.hash(object_id1),
                            self.plasma_client.hash(object_id4))
        self.assertNotEqual(self.plasma_client.hash(object_id3),
                            self.plasma_client.hash(object_id4))

    def test_many_hashes(self):
        hashes = []
        length = 2 ** 10

        for i in range(256):
            object_id = random_object_id()
            memory_buffer = self.plasma_client.create(object_id, length)
            for j in range(length):
                memory_buffer[j] = chr(i)
            self.plasma_client.seal(object_id)
            hashes.append(self.plasma_client.hash(object_id))

        # Create objects of varying length. Each pair has two bits different.
        for i in range(length):
            object_id = random_object_id()
            memory_buffer = self.plasma_client.create(object_id, length)
            for j in range(length):
                memory_buffer[j] = chr(0)
            memory_buffer[i] = chr(1)
            self.plasma_client.seal(object_id)
            hashes.append(self.plasma_client.hash(object_id))

        # Create objects of varying length, all with value 0.
        for i in range(length):
            object_id = random_object_id()
            memory_buffer = self.plasma_client.create(object_id, i)
            for j in range(i):
                memory_buffer[j] = chr(0)
            self.plasma_client.seal(object_id)
            hashes.append(self.plasma_client.hash(object_id))

        # Check that all hashes were unique.
        self.assertEqual(len(set(hashes)), 256 + length + length)

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
    #     memory_buffer = self.plasma_client.create(object_id, length,
    #                                               metadata)
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
        self.assertRaises(Exception, lambda: memory_buffer[length])
        # Seal the object.
        self.plasma_client.seal(object_id)
        # This test is commented out because it currently fails.
        # # Make sure the object is ready only now.
        # def illegal_assignment():
        #   memory_buffer[0] = chr(0)
        # self.assertRaises(Exception, illegal_assignment)
        # Get the object.
        memory_buffer = self.plasma_client.get([object_id])[0]

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
        self.plasma_client.subscribe()
        for i in [1, 10, 100, 1000, 10000, 100000]:
            object_ids = [random_object_id() for _ in range(i)]
            metadata_sizes = [np.random.randint(1000) for _ in range(i)]
            data_sizes = [np.random.randint(1000) for _ in range(i)]
            for j in range(i):
                self.plasma_client.create(
                    object_ids[j], size=data_sizes[j],
                    metadata=bytearray(np.random.bytes(metadata_sizes[j])))
                self.plasma_client.seal(object_ids[j])
            # Check that we received notifications for all of the objects.
            for j in range(i):
                notification_info = self.plasma_client.get_next_notification()
                recv_objid, recv_dsize, recv_msize = notification_info
                self.assertEqual(object_ids[j], recv_objid)
                self.assertEqual(data_sizes[j], recv_dsize)
                self.assertEqual(metadata_sizes[j], recv_msize)

    def test_subscribe_deletions(self):
        # Subscribe to notifications from the Plasma Store. We use
        # plasma_client2 to make sure that all used objects will get evicted
        # properly.
        self.plasma_client2.subscribe()
        for i in [1, 10, 100, 1000, 10000, 100000]:
            object_ids = [random_object_id() for _ in range(i)]
            # Add 1 to the sizes to make sure we have nonzero object sizes.
            metadata_sizes = [np.random.randint(1000) + 1 for _ in range(i)]
            data_sizes = [np.random.randint(1000) + 1 for _ in range(i)]
            for j in range(i):
                x = self.plasma_client2.create(
                    object_ids[j], size=data_sizes[j],
                    metadata=bytearray(np.random.bytes(metadata_sizes[j])))
                self.plasma_client2.seal(object_ids[j])
            del x
            # Check that we received notifications for creating all of the
            # objects.
            for j in range(i):
                notification_info = self.plasma_client2.get_next_notification()
                recv_objid, recv_dsize, recv_msize = notification_info
                self.assertEqual(object_ids[j], recv_objid)
                self.assertEqual(data_sizes[j], recv_dsize)
                self.assertEqual(metadata_sizes[j], recv_msize)

            # Check that we receive notifications for deleting all objects, as
            # we evict them.
            for j in range(i):
                self.assertEqual(self.plasma_client2.evict(1),
                                 data_sizes[j] + metadata_sizes[j])
                notification_info = self.plasma_client2.get_next_notification()
                recv_objid, recv_dsize, recv_msize = notification_info
                self.assertEqual(object_ids[j], recv_objid)
                self.assertEqual(-1, recv_dsize)
                self.assertEqual(-1, recv_msize)

        # Test multiple deletion notifications. The first 9 object IDs have
        # size 0, and the last has a nonzero size. When Plasma evicts 1 byte,
        # it will evict all objects, so we should receive deletion
        # notifications for each.
        num_object_ids = 10
        object_ids = [random_object_id() for _ in range(num_object_ids)]
        metadata_sizes = [0] * (num_object_ids - 1)
        data_sizes = [0] * (num_object_ids - 1)
        metadata_sizes.append(np.random.randint(1000))
        data_sizes.append(np.random.randint(1000))
        for i in range(num_object_ids):
            x = self.plasma_client2.create(
                object_ids[i], size=data_sizes[i],
                metadata=bytearray(np.random.bytes(metadata_sizes[i])))
            self.plasma_client2.seal(object_ids[i])
        del x
        for i in range(num_object_ids):
            notification_info = self.plasma_client2.get_next_notification()
            recv_objid, recv_dsize, recv_msize = notification_info
            self.assertEqual(object_ids[i], recv_objid)
            self.assertEqual(data_sizes[i], recv_dsize)
            self.assertEqual(metadata_sizes[i], recv_msize)
        self.assertEqual(self.plasma_client2.evict(1),
                         data_sizes[-1] + metadata_sizes[-1])
        for i in range(num_object_ids):
            notification_info = self.plasma_client2.get_next_notification()
            recv_objid, recv_dsize, recv_msize = notification_info
            self.assertEqual(object_ids[i], recv_objid)
            self.assertEqual(-1, recv_dsize)
            self.assertEqual(-1, recv_msize)


class TestPlasmaManager(unittest.TestCase):

    def setUp(self):
        # Start two PlasmaStores.
        store_name1, self.p2 = plasma.start_plasma_store(
            use_valgrind=USE_VALGRIND)
        store_name2, self.p3 = plasma.start_plasma_store(
            use_valgrind=USE_VALGRIND)
        # Start a Redis server.
        redis_address, _ = services.start_redis("127.0.0.1")
        # Start two PlasmaManagers.
        manager_name1, self.p4, self.port1 = plasma.start_plasma_manager(
            store_name1, redis_address, use_valgrind=USE_VALGRIND)
        manager_name2, self.p5, self.port2 = plasma.start_plasma_manager(
            store_name2, redis_address, use_valgrind=USE_VALGRIND)
        # Connect two PlasmaClients.
        self.client1 = plasma.PlasmaClient(store_name1, manager_name1)
        self.client2 = plasma.PlasmaClient(store_name2, manager_name2)

        # Store the processes that will be explicitly killed during tearDown so
        # that a test case can remove ones that will be killed during the test.
        # NOTE: If this specific order is changed, valgrind will fail.
        self.processes_to_kill = [self.p4, self.p5, self.p2, self.p3]

    def tearDown(self):
        # Check that the processes are still alive.
        for process in self.processes_to_kill:
            self.assertEqual(process.poll(), None)

        # Kill the Plasma store and Plasma manager processes.
        if USE_VALGRIND:
            # Give processes opportunity to finish work.
            time.sleep(1)
            for process in self.processes_to_kill:
                process.send_signal(signal.SIGTERM)
                process.wait()
                if process.returncode != 0:
                    print("aborting due to valgrind error")
                    os._exit(-1)
        else:
            for process in self.processes_to_kill:
                process.kill()

        # Clean up the Redis server.
        services.cleanup()

    def test_fetch(self):
        for _ in range(10):
            # Create an object.
            object_id1, memory_buffer1, metadata1 = create_object(self.client1,
                                                                  2000,
                                                                  2000)
            self.client1.fetch([object_id1])
            self.assertEqual(self.client1.contains(object_id1), True)
            self.assertEqual(self.client2.contains(object_id1), False)
            # Fetch the object from the other plasma manager.
            # TODO(rkn): Right now we must wait for the object table to be
            # updated.
            while not self.client2.contains(object_id1):
                self.client2.fetch([object_id1])
            # Compare the two buffers.
            assert_get_object_equal(self, self.client1, self.client2,
                                    object_id1,
                                    memory_buffer=memory_buffer1,
                                    metadata=metadata1)

        # Test that we can call fetch on object IDs that don't exist yet.
        object_id2 = random_object_id()
        self.client1.fetch([object_id2])
        self.assertEqual(self.client1.contains(object_id2), False)
        memory_buffer2, metadata2 = create_object_with_id(self.client2,
                                                          object_id2,
                                                          2000, 2000)
        # # Check that the object has been fetched.
        # self.assertEqual(self.client1.contains(object_id2), True)
        # Compare the two buffers.
        # assert_get_object_equal(self, self.client1, self.client2, object_id2,
        #                         memory_buffer=memory_buffer2,
        #                         metadata=metadata2)

        # Test calling the same fetch request a bunch of times.
        object_id3 = random_object_id()
        self.assertEqual(self.client1.contains(object_id3), False)
        self.assertEqual(self.client2.contains(object_id3), False)
        for _ in range(10):
            self.client1.fetch([object_id3])
            self.client2.fetch([object_id3])
        memory_buffer3, metadata3 = create_object_with_id(self.client1,
                                                          object_id3,
                                                          2000, 2000)
        for _ in range(10):
            self.client1.fetch([object_id3])
            self.client2.fetch([object_id3])
        # TODO(rkn): Right now we must wait for the object table to be updated.
        while not self.client2.contains(object_id3):
            self.client2.fetch([object_id3])
        assert_get_object_equal(self, self.client1, self.client2, object_id3,
                                memory_buffer=memory_buffer3,
                                metadata=metadata3)

    def test_fetch_multiple(self):
        for _ in range(20):
            # Create two objects and a third fake one that doesn't exist.
            object_id1, memory_buffer1, metadata1 = create_object(self.client1,
                                                                  2000,
                                                                  2000)
            missing_object_id = random_object_id()
            object_id2, memory_buffer2, metadata2 = create_object(self.client1,
                                                                  2000,
                                                                  2000)
            object_ids = [object_id1, missing_object_id, object_id2]
            # Fetch the objects from the other plasma store. The second object
            # ID should timeout since it does not exist.
            # TODO(rkn): Right now we must wait for the object table to be
            # updated.
            while ((not self.client2.contains(object_id1)) or
                   (not self.client2.contains(object_id2))):
                self.client2.fetch(object_ids)
            # Compare the buffers of the objects that do exist.
            assert_get_object_equal(self, self.client1, self.client2,
                                    object_id1, memory_buffer=memory_buffer1,
                                    metadata=metadata1)
            assert_get_object_equal(self, self.client1, self.client2,
                                    object_id2, memory_buffer=memory_buffer2,
                                    metadata=metadata2)
            # Fetch in the other direction. The fake object still does not
            # exist.
            self.client1.fetch(object_ids)
            assert_get_object_equal(self, self.client2, self.client1,
                                    object_id1, memory_buffer=memory_buffer1,
                                    metadata=metadata1)
            assert_get_object_equal(self, self.client2, self.client1,
                                    object_id2, memory_buffer=memory_buffer2,
                                    metadata=metadata2)

        # Check that we can call fetch with duplicated object IDs.
        object_id3 = random_object_id()
        self.client1.fetch([object_id3, object_id3])
        object_id4, memory_buffer4, metadata4 = create_object(self.client1,
                                                              2000,
                                                              2000)
        time.sleep(0.1)
        # TODO(rkn): Right now we must wait for the object table to be updated.
        while not self.client2.contains(object_id4):
            self.client2.fetch([object_id3, object_id3, object_id4,
                                object_id4])
        assert_get_object_equal(self, self.client2, self.client1, object_id4,
                                memory_buffer=memory_buffer4,
                                metadata=metadata4)

    def test_wait(self):
        # Test timeout.
        obj_id0 = random_object_id()
        self.client1.wait([obj_id0], timeout=100, num_returns=1)
        # If we get here, the test worked.

        # Test wait if local objects available.
        obj_id1 = random_object_id()
        self.client1.create(obj_id1, 1000)
        self.client1.seal(obj_id1)
        ready, waiting = self.client1.wait([obj_id1], timeout=100,
                                           num_returns=1)
        self.assertEqual(set(ready), set([obj_id1]))
        self.assertEqual(waiting, [])

        # Test wait if only one object available and only one object waited
        # for.
        obj_id2 = random_object_id()
        self.client1.create(obj_id2, 1000)
        # Don't seal.
        ready, waiting = self.client1.wait([obj_id2, obj_id1], timeout=100,
                                           num_returns=1)
        self.assertEqual(set(ready), set([obj_id1]))
        self.assertEqual(set(waiting), set([obj_id2]))

        # Test wait if object is sealed later.
        obj_id3 = random_object_id()

        def finish():
            self.client2.create(obj_id3, 1000)
            self.client2.seal(obj_id3)

        t = threading.Timer(0.1, finish)
        t.start()
        ready, waiting = self.client1.wait([obj_id3, obj_id2, obj_id1],
                                           timeout=1000, num_returns=2)
        self.assertEqual(set(ready), set([obj_id1, obj_id3]))
        self.assertEqual(set(waiting), set([obj_id2]))

        # Test if the appropriate number of objects is shown if some objects
        # are not ready.
        ready, waiting = self.client1.wait([obj_id3, obj_id2, obj_id1], 100, 3)
        self.assertEqual(set(ready), set([obj_id1, obj_id3]))
        self.assertEqual(set(waiting), set([obj_id2]))

        # Don't forget to seal obj_id2.
        self.client1.seal(obj_id2)

        # Test calling wait a bunch of times.
        object_ids = []
        # TODO(rkn): Increasing n to 100 (or larger) will cause failures. The
        # problem appears to be that the number of timers added to the manager
        # event loop slow down the manager so much that some of the
        # asynchronous Redis commands timeout triggering fatal failure
        # callbacks.
        n = 40
        for i in range(n * (n + 1) // 2):
            if i % 2 == 0:
                object_id, _, _ = create_object(self.client1, 200, 200)
            else:
                object_id, _, _ = create_object(self.client2, 200, 200)
            object_ids.append(object_id)
        # Try waiting for all of the object IDs on the first client.
        waiting = object_ids
        retrieved = []
        for i in range(1, n + 1):
            ready, waiting = self.client1.wait(waiting, timeout=1000,
                                               num_returns=i)
            self.assertEqual(len(ready), i)
            retrieved += ready
        self.assertEqual(set(retrieved), set(object_ids))
        ready, waiting = self.client1.wait(object_ids, timeout=1000,
                                           num_returns=len(object_ids))
        self.assertEqual(set(ready), set(object_ids))
        self.assertEqual(waiting, [])
        # Try waiting for all of the object IDs on the second client.
        waiting = object_ids
        retrieved = []
        for i in range(1, n + 1):
            ready, waiting = self.client2.wait(waiting, timeout=1000,
                                               num_returns=i)
            self.assertEqual(len(ready), i)
            retrieved += ready
        self.assertEqual(set(retrieved), set(object_ids))
        ready, waiting = self.client2.wait(object_ids, timeout=1000,
                                           num_returns=len(object_ids))
        self.assertEqual(set(ready), set(object_ids))
        self.assertEqual(waiting, [])

        # Make sure that wait returns when the requested number of object IDs
        # are available and does not wait for all object IDs to be available.
        object_ids = [random_object_id() for _ in range(9)] + [20 * b'\x00']
        object_ids_perm = object_ids[:]
        random.shuffle(object_ids_perm)
        for i in range(10):
            if i % 2 == 0:
                create_object_with_id(self.client1, object_ids_perm[i], 2000,
                                      2000)
            else:
                create_object_with_id(self.client2, object_ids_perm[i], 2000,
                                      2000)
            ready, waiting = self.client1.wait(object_ids, num_returns=(i + 1))
            self.assertEqual(set(ready), set(object_ids_perm[:(i + 1)]))
            self.assertEqual(set(waiting), set(object_ids_perm[(i + 1):]))

    def test_transfer(self):
        num_attempts = 100
        for _ in range(100):
            # Create an object.
            object_id1, memory_buffer1, metadata1 = create_object(self.client1,
                                                                  2000,
                                                                  2000)
            # Transfer the buffer to the the other Plasma store. There is a
            # race condition on the create and transfer of the object, so keep
            # trying until the object appears on the second Plasma store.
            for i in range(num_attempts):
                self.client1.transfer("127.0.0.1", self.port2, object_id1)
                buff = self.client2.get([object_id1], timeout_ms=100)[0]
                if buff is not None:
                    break
            self.assertNotEqual(buff, None)
            del buff

            # Compare the two buffers.
            assert_get_object_equal(self, self.client1, self.client2,
                                    object_id1, memory_buffer=memory_buffer1,
                                    metadata=metadata1)
            # # Transfer the buffer again.
            # self.client1.transfer("127.0.0.1", self.port2, object_id1)
            # # Compare the two buffers.
            # assert_get_object_equal(self, self.client1, self.client2,
            #                         object_id1,
            #                         memory_buffer=memory_buffer1,
            #                         metadata=metadata1)

            # Create an object.
            object_id2, memory_buffer2, metadata2 = create_object(self.client2,
                                                                  20000, 20000)
            # Transfer the buffer to the the other Plasma store. There is a
            # race condition on the create and transfer of the object, so keep
            # trying until the object appears on the second Plasma store.
            for i in range(num_attempts):
                self.client2.transfer("127.0.0.1", self.port1, object_id2)
                buff = self.client1.get([object_id2], timeout_ms=100)[0]
                if buff is not None:
                    break
            self.assertNotEqual(buff, None)
            del buff

            # Compare the two buffers.
            assert_get_object_equal(self, self.client1, self.client2,
                                    object_id2, memory_buffer=memory_buffer2,
                                    metadata=metadata2)

    def test_illegal_functionality(self):
        # Create an object id string.
        # object_id = random_object_id()
        # Create a new buffer.
        # memory_buffer = self.client1.create(object_id, 20000)
        # This test is commented out because it currently fails.
        # # Transferring the buffer before sealing it should fail.
        # self.assertRaises(Exception,
        #                   lambda : self.manager1.transfer(1, object_id))
        pass

    def test_stresstest(self):
        a = time.time()
        object_ids = []
        for i in range(10000):  # TODO(pcm): increase this to 100000.
            object_id = random_object_id()
            object_ids.append(object_id)
            self.client1.create(object_id, 1)
            self.client1.seal(object_id)
        for object_id in object_ids:
            self.client1.transfer("127.0.0.1", self.port2, object_id)
        b = time.time() - a

        print("it took", b, "seconds to put and transfer the objects")


class TestPlasmaManagerRecovery(unittest.TestCase):

    def setUp(self):
        # Start a Plasma store.
        self.store_name, self.p2 = plasma.start_plasma_store(
            use_valgrind=USE_VALGRIND)
        # Start a Redis server.
        self.redis_address, _ = services.start_redis("127.0.0.1")
        # Start a PlasmaManagers.
        manager_name, self.p3, self.port1 = plasma.start_plasma_manager(
            self.store_name,
            self.redis_address,
            use_valgrind=USE_VALGRIND)
        # Connect a PlasmaClient.
        self.client = plasma.PlasmaClient(self.store_name, manager_name)

        # Store the processes that will be explicitly killed during tearDown so
        # that a test case can remove ones that will be killed during the test.
        # NOTE: The plasma managers must be killed before the plasma store
        # since plasma store death will bring down the managers.
        self.processes_to_kill = [self.p3, self.p2]

    def tearDown(self):
        # Check that the processes are still alive.
        for process in self.processes_to_kill:
            self.assertEqual(process.poll(), None)

        # Kill the Plasma store and Plasma manager processes.
        if USE_VALGRIND:
            # Give processes opportunity to finish work.
            time.sleep(1)
            for process in self.processes_to_kill:
                process.send_signal(signal.SIGTERM)
                process.wait()
                if process.returncode != 0:
                    print("aborting due to valgrind error")
                    os._exit(-1)
        else:
            for process in self.processes_to_kill:
                process.kill()

        # Clean up the Redis server.
        services.cleanup()

    def test_delayed_start(self):
        num_objects = 10
        # Create some objects using one client.
        object_ids = [random_object_id() for _ in range(num_objects)]
        for i in range(10):
            create_object_with_id(self.client, object_ids[i], 2000, 2000)

        # Wait until the objects have been sealed in the store.
        ready, waiting = self.client.wait(object_ids, num_returns=num_objects)
        self.assertEqual(set(ready), set(object_ids))
        self.assertEqual(waiting, [])

        # Start a second plasma manager attached to the same store.
        manager_name, self.p5, self.port2 = plasma.start_plasma_manager(
            self.store_name, self.redis_address, use_valgrind=USE_VALGRIND)
        self.processes_to_kill = [self.p5] + self.processes_to_kill

        # Check that the second manager knows about existing objects.
        client2 = plasma.PlasmaClient(self.store_name, manager_name)
        ready, waiting = [], object_ids
        while True:
            ready, waiting = client2.wait(object_ids, num_returns=num_objects,
                                          timeout=0)
            if len(ready) == len(object_ids):
                break

        self.assertEqual(set(ready), set(object_ids))
        self.assertEqual(waiting, [])


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Pop the argument so we don't mess with unittest's own argument
        # parser.
        if sys.argv[-1] == "valgrind":
            arg = sys.argv.pop()
            USE_VALGRIND = True
            print("Using valgrind for tests")
    unittest.main(verbosity=2)
