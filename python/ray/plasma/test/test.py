from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
from numpy.testing import assert_equal
import os
import random
import signal
import subprocess
import sys
import threading
import time
import unittest

# The ray import must come before the pyarrow import because ray modifies the
# python path so that the right version of pyarrow is found.
import ray
from ray.plasma.utils import (random_object_id, create_object_with_id,
                              create_object)
import ray.ray_constants as ray_constants
from ray import services
import pyarrow as pa
import pyarrow.plasma as plasma

USE_VALGRIND = False
PLASMA_STORE_MEMORY = 1000000000


def random_name():
    return str(random.randint(0, 99999999))


def assert_get_object_equal(unit_test,
                            client1,
                            client2,
                            object_id,
                            memory_buffer=None,
                            metadata=None):
    client1_buff = client1.get_buffers([object_id])[0]
    client2_buff = client2.get_buffers([object_id])[0]
    client1_metadata = client1.get_metadata([object_id])[0]
    client2_metadata = client2.get_metadata([object_id])[0]
    unit_test.assertEqual(len(client1_buff), len(client2_buff))
    unit_test.assertEqual(len(client1_metadata), len(client2_metadata))
    # Check that the buffers from the two clients are the same.
    assert_equal(
        np.frombuffer(client1_buff, dtype="uint8"),
        np.frombuffer(client2_buff, dtype="uint8"))
    # Check that the metadata buffers from the two clients are the same.
    assert_equal(
        np.frombuffer(client1_metadata, dtype="uint8"),
        np.frombuffer(client2_metadata, dtype="uint8"))
    # If a reference buffer was provided, check that it is the same as well.
    if memory_buffer is not None:
        assert_equal(
            np.frombuffer(memory_buffer, dtype="uint8"),
            np.frombuffer(client1_buff, dtype="uint8"))
    # If reference metadata was provided, check that it is the same as well.
    if metadata is not None:
        assert_equal(
            np.frombuffer(metadata, dtype="uint8"),
            np.frombuffer(client1_metadata, dtype="uint8"))


DEFAULT_PLASMA_STORE_MEMORY = 10**9


def start_plasma_store(plasma_store_memory=DEFAULT_PLASMA_STORE_MEMORY,
                       use_valgrind=False,
                       use_profiler=False,
                       stdout_file=None,
                       stderr_file=None):
    """Start a plasma store process.
    Args:
        use_valgrind (bool): True if the plasma store should be started inside
            of valgrind. If this is True, use_profiler must be False.
        use_profiler (bool): True if the plasma store should be started inside
            a profiler. If this is True, use_valgrind must be False.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
    Return:
        A tuple of the name of the plasma store socket and the process ID of
            the plasma store process.
    """
    if use_valgrind and use_profiler:
        raise Exception("Cannot use valgrind and profiler at the same time.")
    plasma_store_executable = os.path.join(pa.__path__[0],
                                           "plasma_store_server")
    plasma_store_name = "/tmp/plasma_store{}".format(random_name())
    command = [
        plasma_store_executable, "-s", plasma_store_name, "-m",
        str(plasma_store_memory)
    ]
    if use_valgrind:
        pid = subprocess.Popen(
            [
                "valgrind", "--track-origins=yes", "--leak-check=full",
                "--show-leak-kinds=all", "--leak-check-heuristics=stdstring",
                "--error-exitcode=1"
            ] + command,
            stdout=stdout_file,
            stderr=stderr_file)
        time.sleep(1.0)
    elif use_profiler:
        pid = subprocess.Popen(
            ["valgrind", "--tool=callgrind"] + command,
            stdout=stdout_file,
            stderr=stderr_file)
        time.sleep(1.0)
    else:
        pid = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
        time.sleep(0.1)
    return plasma_store_name, pid


# Plasma client tests were moved into arrow


class TestPlasmaManager(unittest.TestCase):
    def setUp(self):
        # Start two PlasmaStores.
        store_name1, self.p2 = start_plasma_store(use_valgrind=USE_VALGRIND)
        store_name2, self.p3 = start_plasma_store(use_valgrind=USE_VALGRIND)
        # Start a Redis server.
        redis_address, _ = services.start_redis("127.0.0.1")
        # Start two PlasmaManagers.
        manager_name1, self.p4, self.port1 = ray.plasma.start_plasma_manager(
            store_name1, redis_address, use_valgrind=USE_VALGRIND)
        manager_name2, self.p5, self.port2 = ray.plasma.start_plasma_manager(
            store_name2, redis_address, use_valgrind=USE_VALGRIND)
        # Connect two PlasmaClients.
        self.client1 = plasma.connect(store_name1, manager_name1, 64)
        self.client2 = plasma.connect(store_name2, manager_name2, 64)

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
            object_id1, memory_buffer1, metadata1 = create_object(
                self.client1, 2000, 2000)
            self.client1.fetch([object_id1])
            self.assertEqual(self.client1.contains(object_id1), True)
            self.assertEqual(self.client2.contains(object_id1), False)
            # Fetch the object from the other plasma manager.
            # TODO(rkn): Right now we must wait for the object table to be
            # updated.
            while not self.client2.contains(object_id1):
                self.client2.fetch([object_id1])
            # Compare the two buffers.
            assert_get_object_equal(
                self,
                self.client1,
                self.client2,
                object_id1,
                memory_buffer=memory_buffer1,
                metadata=metadata1)

        # Test that we can call fetch on object IDs that don't exist yet.
        object_id2 = random_object_id()
        self.client1.fetch([object_id2])
        self.assertEqual(self.client1.contains(object_id2), False)
        memory_buffer2, metadata2 = create_object_with_id(
            self.client2, object_id2, 2000, 2000)
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
        memory_buffer3, metadata3 = create_object_with_id(
            self.client1, object_id3, 2000, 2000)
        for _ in range(10):
            self.client1.fetch([object_id3])
            self.client2.fetch([object_id3])
        # TODO(rkn): Right now we must wait for the object table to be updated.
        while not self.client2.contains(object_id3):
            self.client2.fetch([object_id3])
        assert_get_object_equal(
            self,
            self.client1,
            self.client2,
            object_id3,
            memory_buffer=memory_buffer3,
            metadata=metadata3)

    def test_fetch_multiple(self):
        for _ in range(20):
            # Create two objects and a third fake one that doesn't exist.
            object_id1, memory_buffer1, metadata1 = create_object(
                self.client1, 2000, 2000)
            missing_object_id = random_object_id()
            object_id2, memory_buffer2, metadata2 = create_object(
                self.client1, 2000, 2000)
            object_ids = [object_id1, missing_object_id, object_id2]
            # Fetch the objects from the other plasma store. The second object
            # ID should timeout since it does not exist.
            # TODO(rkn): Right now we must wait for the object table to be
            # updated.
            while ((not self.client2.contains(object_id1))
                   or (not self.client2.contains(object_id2))):
                self.client2.fetch(object_ids)
            # Compare the buffers of the objects that do exist.
            assert_get_object_equal(
                self,
                self.client1,
                self.client2,
                object_id1,
                memory_buffer=memory_buffer1,
                metadata=metadata1)
            assert_get_object_equal(
                self,
                self.client1,
                self.client2,
                object_id2,
                memory_buffer=memory_buffer2,
                metadata=metadata2)
            # Fetch in the other direction. The fake object still does not
            # exist.
            self.client1.fetch(object_ids)
            assert_get_object_equal(
                self,
                self.client2,
                self.client1,
                object_id1,
                memory_buffer=memory_buffer1,
                metadata=metadata1)
            assert_get_object_equal(
                self,
                self.client2,
                self.client1,
                object_id2,
                memory_buffer=memory_buffer2,
                metadata=metadata2)

        # Check that we can call fetch with duplicated object IDs.
        object_id3 = random_object_id()
        self.client1.fetch([object_id3, object_id3])
        object_id4, memory_buffer4, metadata4 = create_object(
            self.client1, 2000, 2000)
        time.sleep(0.1)
        # TODO(rkn): Right now we must wait for the object table to be updated.
        while not self.client2.contains(object_id4):
            self.client2.fetch(
                [object_id3, object_id3, object_id4, object_id4])
        assert_get_object_equal(
            self,
            self.client2,
            self.client1,
            object_id4,
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
        ready, waiting = self.client1.wait(
            [obj_id1], timeout=100, num_returns=1)
        self.assertEqual(set(ready), {obj_id1})
        self.assertEqual(waiting, [])

        # Test wait if only one object available and only one object waited
        # for.
        obj_id2 = random_object_id()
        self.client1.create(obj_id2, 1000)
        # Don't seal.
        ready, waiting = self.client1.wait(
            [obj_id2, obj_id1], timeout=100, num_returns=1)
        self.assertEqual(set(ready), {obj_id1})
        self.assertEqual(set(waiting), {obj_id2})

        # Test wait if object is sealed later.
        obj_id3 = random_object_id()

        def finish():
            self.client2.create(obj_id3, 1000)
            self.client2.seal(obj_id3)

        t = threading.Timer(0.1, finish)
        t.start()
        ready, waiting = self.client1.wait(
            [obj_id3, obj_id2, obj_id1], timeout=1000, num_returns=2)
        self.assertEqual(set(ready), {obj_id1, obj_id3})
        self.assertEqual(set(waiting), {obj_id2})

        # Test if the appropriate number of objects is shown if some objects
        # are not ready.
        ready, waiting = self.client1.wait([obj_id3, obj_id2, obj_id1], 100, 3)
        self.assertEqual(set(ready), {obj_id1, obj_id3})
        self.assertEqual(set(waiting), {obj_id2})

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
            ready, waiting = self.client1.wait(
                waiting, timeout=1000, num_returns=i)
            self.assertEqual(len(ready), i)
            retrieved += ready
        self.assertEqual(set(retrieved), set(object_ids))
        ready, waiting = self.client1.wait(
            object_ids, timeout=1000, num_returns=len(object_ids))
        self.assertEqual(set(ready), set(object_ids))
        self.assertEqual(waiting, [])
        # Try waiting for all of the object IDs on the second client.
        waiting = object_ids
        retrieved = []
        for i in range(1, n + 1):
            ready, waiting = self.client2.wait(
                waiting, timeout=1000, num_returns=i)
            self.assertEqual(len(ready), i)
            retrieved += ready
        self.assertEqual(set(retrieved), set(object_ids))
        ready, waiting = self.client2.wait(
            object_ids, timeout=1000, num_returns=len(object_ids))
        self.assertEqual(set(ready), set(object_ids))
        self.assertEqual(waiting, [])

        # Make sure that wait returns when the requested number of object IDs
        # are available and does not wait for all object IDs to be available.
        object_ids = [random_object_id() for _ in range(9)] + \
                     [plasma.ObjectID(ray_constants.ID_SIZE * b'\x00')]
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
            object_id1, memory_buffer1, metadata1 = create_object(
                self.client1, 2000, 2000)
            # Transfer the buffer to the the other Plasma store. There is a
            # race condition on the create and transfer of the object, so keep
            # trying until the object appears on the second Plasma store.
            for i in range(num_attempts):
                self.client1.transfer("127.0.0.1", self.port2, object_id1)
                buff = self.client2.get_buffers(
                    [object_id1], timeout_ms=100)[0]
                if buff is not None:
                    break
            self.assertNotEqual(buff, None)
            del buff

            # Compare the two buffers.
            assert_get_object_equal(
                self,
                self.client1,
                self.client2,
                object_id1,
                memory_buffer=memory_buffer1,
                metadata=metadata1)
            # # Transfer the buffer again.
            # self.client1.transfer("127.0.0.1", self.port2, object_id1)
            # # Compare the two buffers.
            # assert_get_object_equal(self, self.client1, self.client2,
            #                         object_id1,
            #                         memory_buffer=memory_buffer1,
            #                         metadata=metadata1)

            # Create an object.
            object_id2, memory_buffer2, metadata2 = create_object(
                self.client2, 20000, 20000)
            # Transfer the buffer to the the other Plasma store. There is a
            # race condition on the create and transfer of the object, so keep
            # trying until the object appears on the second Plasma store.
            for i in range(num_attempts):
                self.client2.transfer("127.0.0.1", self.port1, object_id2)
                buff = self.client1.get_buffers(
                    [object_id2], timeout_ms=100)[0]
                if buff is not None:
                    break
            self.assertNotEqual(buff, None)
            del buff

            # Compare the two buffers.
            assert_get_object_equal(
                self,
                self.client1,
                self.client2,
                object_id2,
                memory_buffer=memory_buffer2,
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
        self.store_name, self.p2 = start_plasma_store(
            use_valgrind=USE_VALGRIND)
        # Start a Redis server.
        self.redis_address, _ = services.start_redis("127.0.0.1")
        # Start a PlasmaManagers.
        manager_name, self.p3, self.port1 = ray.plasma.start_plasma_manager(
            self.store_name, self.redis_address, use_valgrind=USE_VALGRIND)
        # Connect a PlasmaClient.
        self.client = plasma.connect(self.store_name, manager_name, 64)

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
        manager_name, self.p5, self.port2 = ray.plasma.start_plasma_manager(
            self.store_name, self.redis_address, use_valgrind=USE_VALGRIND)
        self.processes_to_kill = [self.p5] + self.processes_to_kill

        # Check that the second manager knows about existing objects.
        client2 = plasma.connect(self.store_name, manager_name, 64)
        ready, waiting = [], object_ids
        while True:
            ready, waiting = client2.wait(
                object_ids, num_returns=num_objects, timeout=0)
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
