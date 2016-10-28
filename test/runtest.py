import unittest
import ray
import numpy as np
import time
import string
import sys
from collections import namedtuple

import test_functions
import ray.array.remote as ra
import ray.array.distributed as da

def assert_equal(obj1, obj2):
  if type(obj1).__module__ == np.__name__ or type(obj2).__module__ == np.__name__:
    if (hasattr(obj1, "shape") and obj1.shape == ()) or (hasattr(obj2, "shape") and obj2.shape == ()):
      # This is a special case because currently np.testing.assert_equal fails
      # because we do not properly handle different numerical types.
      assert obj1 == obj2, "Objects {} and {} are different.".format(obj1, obj2)
    else:
      np.testing.assert_equal(obj1, obj2)
  elif hasattr(obj1, "__dict__") and hasattr(obj2, "__dict__"):
    special_keys = ["_pytype_"]
    assert set(obj1.__dict__.keys() + special_keys) == set(obj2.__dict__.keys() + special_keys), "Objects {} and {} are different.".format(obj1, obj2)
    for key in obj1.__dict__.keys():
      if key not in special_keys:
        assert_equal(obj1.__dict__[key], obj2.__dict__[key])
  elif type(obj1) is dict or type(obj2) is dict:
    assert_equal(obj1.keys(), obj2.keys())
    for key in obj1.keys():
      assert_equal(obj1[key], obj2[key])
  elif type(obj1) is list or type(obj2) is list:
    assert len(obj1) == len(obj2), "Objects {} and {} are lists with different lengths.".format(obj1, obj2)
    for i in range(len(obj1)):
      assert_equal(obj1[i], obj2[i])
  elif type(obj1) is tuple or type(obj2) is tuple:
    assert len(obj1) == len(obj2), "Objects {} and {} are tuples with different lengths.".format(obj1, obj2)
    for i in range(len(obj1)):
      assert_equal(obj1[i], obj2[i])
  else:
    assert obj1 == obj2, "Objects {} and {} are different.".format(obj1, obj2)

PRIMITIVE_OBJECTS = [0, 0.0, 0.9, 0L, 1L << 62, "a", string.printable, "\u262F",
                     u"hello world", u"\xff\xfe\x9c\x001\x000\x00", None, True,
                     False, [], (), {}, np.int8(3), np.int32(4), np.int64(5),
                     np.uint8(3), np.uint32(4), np.uint64(5), np.float32(1.9),
                     np.float64(1.9), np.zeros([100, 100]),
                     np.random.normal(size=[100, 100]), np.array(["hi", 3]),
                     np.array(["hi", 3], dtype=object),
                     np.array([["hi", u"hi"], [1.3, 1L]])]

COMPLEX_OBJECTS = [#[[[[[[[[[[[[]]]]]]]]]]]],
                   {"obj{}".format(i): np.random.normal(size=[100, 100]) for i in range(10)},
                   #{(): {(): {(): {(): {(): {(): {(): {(): {(): {(): {(): {(): {}}}}}}}}}}}}},
                   #((((((((((),),),),),),),),),),
                   #{"a": {"b": {"c": {"d": {}}}}}
                   ]

class Foo(object):
  def __init__(self):
    pass

class Bar(object):
  def __init__(self):
    for i, val in enumerate(PRIMITIVE_OBJECTS + COMPLEX_OBJECTS):
      setattr(self, "field{}".format(i), val)

class Baz(object):
  def __init__(self):
    self.foo = Foo()
    self.bar = Bar()
  def method(self, arg):
    pass

class Qux(object):
  def __init__(self):
    self.objs = [Foo(), Bar(), Baz()]

class SubQux(Qux):
  def __init__(self):
    Qux.__init__(self)

class CustomError(Exception):
  pass

Point = namedtuple("Point", ["x", "y"])
NamedTupleExample = namedtuple("Example", "field1, field2, field3, field4, field5")

CUSTOM_OBJECTS = [Exception("Test object."), CustomError(), Point(11, y=22),
                  Foo(), Bar(), Baz(), # Qux(), SubQux(),
                  NamedTupleExample(1, 1.0, "hi", np.zeros([3, 5]), [1, 2, 3])]

BASE_OBJECTS = PRIMITIVE_OBJECTS + COMPLEX_OBJECTS + CUSTOM_OBJECTS

LIST_OBJECTS = [[obj] for obj in BASE_OBJECTS]
TUPLE_OBJECTS = [(obj,) for obj in BASE_OBJECTS]
# The check that type(obj).__module__ != "numpy" should be unnecessary, but
# otherwise this seems to fail on Mac OS X on Travis.
DICT_OBJECTS = ([{obj: obj} for obj in PRIMITIVE_OBJECTS if obj.__hash__ is not None and type(obj).__module__ != "numpy"] +
# DICT_OBJECTS = ([{obj: obj} for obj in BASE_OBJECTS if obj.__hash__ is not None] +
                [{0: obj} for obj in BASE_OBJECTS])

RAY_TEST_OBJECTS = BASE_OBJECTS + LIST_OBJECTS + TUPLE_OBJECTS + DICT_OBJECTS

# Check that the correct version of cloudpickle is installed.
try:
  import cloudpickle
  cloudpickle.dumps(Point)
except AttributeError:
  cloudpickle_command = "sudo pip install --upgrade git+git://github.com/cloudpipe/cloudpickle.git@0d225a4695f1f65ae1cbb2e0bbc145e10167cce4"
  raise Exception("You have an older version of cloudpickle that is not able to serialize namedtuples. Try running \n\n{}\n\n".format(cloudpickle_command))

class SerializationTest(unittest.TestCase):

  def testRecursiveObjects(self):
    ray.init(start_ray_local=True, num_workers=0)

    class ClassA(object):
      pass

    ray.register_class(ClassA)

    # Make a list that contains itself.
    l = []
    l.append(l)
    # Make an object that contains itself as a field.
    a1 = ClassA()
    a1.field = a1
    # Make two objects that contain each other as fields.
    a2 = ClassA()
    a3 = ClassA()
    a2.field = a3
    a3.field = a2
    # Make a dictionary that contains itself.
    d1 = {}
    d1["key"] = d1
    # Create a list of recursive objects.
    recursive_objects = [l, a1, a2, a3, d1]

    # Check that exceptions are thrown when we serialize the recursive objects.
    for obj in recursive_objects:
      self.assertRaises(Exception, lambda : ray.put(obj))

    ray.worker.cleanup()

class ObjStoreTest(unittest.TestCase):

  # Test setting up object stores, transfering data between them and retrieving data to a client
  def testObjStore(self):
    node_ip_address = "127.0.0.1"
    scheduler_address = ray.services.start_ray_local(num_objstores=2, num_workers=0, worker_path=None)
    ray.connect(node_ip_address, scheduler_address, mode=ray.SCRIPT_MODE)
    objstore_addresses = [objstore_info["address"] for objstore_info in ray.scheduler_info()["objstores"]]
    w1 = ray.worker.Worker()
    w2 = ray.worker.Worker()
    ray.reusables._cached_reusables = [] # This is a hack to make the test run.
    ray.connect(node_ip_address, scheduler_address, objstore_address=objstore_addresses[0], mode=ray.SCRIPT_MODE, worker=w1)
    ray.reusables._cached_reusables = [] # This is a hack to make the test run.
    ray.connect(node_ip_address, scheduler_address, objstore_address=objstore_addresses[1], mode=ray.SCRIPT_MODE, worker=w2)

    for cls in [Foo, Bar, Baz, Qux, SubQux, Exception, CustomError, Point, NamedTupleExample]:
      ray.register_class(cls)

    # putting and getting an object shouldn't change it
    for data in RAY_TEST_OBJECTS:
      objectid = ray.put(data, w1)
      result = ray.get(objectid, w1)
      assert_equal(result, data)

    # putting an object, shipping it to another worker, and getting it shouldn't change it
    for data in RAY_TEST_OBJECTS:
      objectid = ray.put(data, w1)
      result = ray.get(objectid, w2)
      assert_equal(result, data)

    # putting an object, shipping it to another worker, and getting it shouldn't change it
    for data in RAY_TEST_OBJECTS:
      objectid = ray.put(data, w2)
      result = ray.get(objectid, w1)
      assert_equal(result, data)

    # This test fails. See https://github.com/ray-project/ray/issues/159.
    # getting multiple times shouldn't matter
    # for data in [np.zeros([10, 20]), np.random.normal(size=[45, 25]), np.zeros([10, 20], dtype=np.dtype("float64")), np.zeros([10, 20], dtype=np.dtype("float32")), np.zeros([10, 20], dtype=np.dtype("int64")), np.zeros([10, 20], dtype=np.dtype("int32"))]:
    #   objectid = worker.put(data, w1)
    #   result = worker.get(objectid, w2)
    #   result = worker.get(objectid, w2)
    #   result = worker.get(objectid, w2)
    #   assert_equal(result, data)

    # Getting a buffer after modifying it before it finishes should return updated buffer
    objectid = ray.libraylib.get_objectid(w1.handle)
    buf = ray.libraylib.allocate_buffer(w1.handle, objectid, 100)
    buf[0][0] = 1
    ray.libraylib.finish_buffer(w1.handle, objectid, buf[1], 0)
    completedbuffer = ray.libraylib.get_buffer(w1.handle, objectid)
    self.assertEqual(completedbuffer[0][0], 1)

    # We started multiple drivers manually, so we will disconnect them manually.
    ray.disconnect(worker=w1)
    ray.disconnect(worker=w2)
    ray.worker.cleanup()

class WorkerTest(unittest.TestCase):

  def testPutGet(self):
    ray.init(start_ray_local=True, num_workers=0)

    for i in range(100):
      value_before = i * 10 ** 6
      objectid = ray.put(value_before)
      value_after = ray.get(objectid)
      self.assertEqual(value_before, value_after)

    for i in range(100):
      value_before = i * 10 ** 6 * 1.0
      objectid = ray.put(value_before)
      value_after = ray.get(objectid)
      self.assertEqual(value_before, value_after)

    for i in range(100):
      value_before = "h" * i
      objectid = ray.put(value_before)
      value_after = ray.get(objectid)
      self.assertEqual(value_before, value_after)

    for i in range(100):
      value_before = [1] * i
      objectid = ray.put(value_before)
      value_after = ray.get(objectid)
      self.assertEqual(value_before, value_after)

    ray.worker.cleanup()

class APITest(unittest.TestCase):

  def testPassingArgumentsByValue(self):
    ray.init(start_ray_local=True, num_workers=0)

    # The types that can be passed by value are defined by
    # is_argument_serializable in serialization.py.
    class Foo(object):
      pass
    CAN_PASS_BY_VALUE = [1, 1L, 1.0, True, False, None, [1L, 1.0, True, None],
                         ([1, 2, 3], {False: [1.0, u"hi", ()]}), 100 * ["a"]]
    CANNOT_PASS_BY_VALUE = [int, np.int64(0), np.float64(0), Foo(), [Foo()],
                            (Foo()), {0: Foo()}, [[[int]]], 101 * [1],
                            np.zeros(10)]

    for obj in CAN_PASS_BY_VALUE:
      self.assertTrue(ray.serialization.is_argument_serializable(obj))
      self.assertEqual(obj, ray.serialization.deserialize_argument(ray.serialization.serialize_argument_if_possible(obj)))

    for obj in CANNOT_PASS_BY_VALUE:
      self.assertFalse(ray.serialization.is_argument_serializable(obj))
      self.assertEqual(None, ray.serialization.serialize_argument_if_possible(obj))

    ray.worker.cleanup()

  def testRegisterClass(self):
    ray.init(start_ray_local=True, num_workers=0)

    # Check that putting an object of a class that has not been registered
    # throws an exception.
    class TempClass(object):
      pass
    self.assertRaises(Exception, lambda : ray.put(Foo))
    # Check that registering a class that Ray cannot serialize efficiently
    # raises an exception.
    self.assertRaises(Exception, lambda : ray.register_class(type(True)))
    # Check that registering the same class with pickle works.
    ray.register_class(type(float), pickle=True)
    self.assertEqual(ray.get(ray.put(float)), float)

    ray.worker.cleanup()

  def testKeywordArgs(self):
    reload(test_functions)
    ray.init(start_ray_local=True, num_workers=1)

    x = test_functions.keyword_fct1.remote(1)
    self.assertEqual(ray.get(x), "1 hello")
    x = test_functions.keyword_fct1.remote(1, "hi")
    self.assertEqual(ray.get(x), "1 hi")
    x = test_functions.keyword_fct1.remote(1, b="world")
    self.assertEqual(ray.get(x), "1 world")

    x = test_functions.keyword_fct2.remote(a="w", b="hi")
    self.assertEqual(ray.get(x), "w hi")
    x = test_functions.keyword_fct2.remote(b="hi", a="w")
    self.assertEqual(ray.get(x), "w hi")
    x = test_functions.keyword_fct2.remote(a="w")
    self.assertEqual(ray.get(x), "w world")
    x = test_functions.keyword_fct2.remote(b="hi")
    self.assertEqual(ray.get(x), "hello hi")
    x = test_functions.keyword_fct2.remote("w")
    self.assertEqual(ray.get(x), "w world")
    x = test_functions.keyword_fct2.remote("w", "hi")
    self.assertEqual(ray.get(x), "w hi")

    x = test_functions.keyword_fct3.remote(0, 1, c="w", d="hi")
    self.assertEqual(ray.get(x), "0 1 w hi")
    x = test_functions.keyword_fct3.remote(0, 1, d="hi", c="w")
    self.assertEqual(ray.get(x), "0 1 w hi")
    x = test_functions.keyword_fct3.remote(0, 1, c="w")
    self.assertEqual(ray.get(x), "0 1 w world")
    x = test_functions.keyword_fct3.remote(0, 1, d="hi")
    self.assertEqual(ray.get(x), "0 1 hello hi")
    x = test_functions.keyword_fct3.remote(0, 1)
    self.assertEqual(ray.get(x), "0 1 hello world")

    ray.worker.cleanup()

  def testVariableNumberOfArgs(self):
    reload(test_functions)
    ray.init(start_ray_local=True, num_workers=1)

    x = test_functions.varargs_fct1.remote(0, 1, 2)
    self.assertEqual(ray.get(x), "0 1 2")
    x = test_functions.varargs_fct2.remote(0, 1, 2)
    self.assertEqual(ray.get(x), "1 2")

    self.assertTrue(test_functions.kwargs_exception_thrown)
    self.assertTrue(test_functions.varargs_and_kwargs_exception_thrown)

    ray.worker.cleanup()

  def testNoArgs(self):
    reload(test_functions)
    ray.init(start_ray_local=True, num_workers=1)

    test_functions.no_op.remote()
    time.sleep(0.2)
    task_info = ray.task_info()
    self.assertEqual(len(task_info["failed_tasks"]), 0)
    self.assertEqual(len(task_info["running_tasks"]), 0)

    ray.worker.cleanup()

  def testDefiningRemoteFunctions(self):
    ray.init(start_ray_local=True, num_workers=3)

    # Test that we can define a remote function in the shell.
    @ray.remote
    def f(x):
      return x + 1
    self.assertEqual(ray.get(f.remote(0)), 1)

    # Test that we can redefine the remote function.
    @ray.remote
    def f(x):
      return x + 10
    self.assertEqual(ray.get(f.remote(0)), 10)

    # Test that we can close over plain old data.
    data = [np.zeros([3, 5]), (1, 2, "a"), [0.0, 1.0, 2L], 2L, {"a": np.zeros(3)}]
    @ray.remote
    def g():
      return data
    ray.get(g.remote())

    # Test that we can close over modules.
    @ray.remote
    def h():
      return np.zeros([3, 5])
    assert_equal(ray.get(h.remote()), np.zeros([3, 5]))
    @ray.remote
    def j():
      return time.time()
    ray.get(j.remote())

    # Test that we can define remote functions that call other remote functions.
    @ray.remote
    def k(x):
      return x + 1
    @ray.remote
    def l(x):
      return ray.get(k.remote(x))
    @ray.remote
    def m(x):
      return ray.get(l.remote(x))
    self.assertEqual(ray.get(k.remote(1)), 2)
    self.assertEqual(ray.get(l.remote(1)), 2)
    self.assertEqual(ray.get(m.remote(1)), 2)

    ray.worker.cleanup()

  def testGetMultiple(self):
    ray.init(start_ray_local=True, num_workers=0)
    object_ids = [ray.put(i) for i in range(10)]
    self.assertEqual(ray.get(object_ids), range(10))
    ray.worker.cleanup()

  def testWait(self):
    ray.init(start_ray_local=True, num_workers=1)

    @ray.remote
    def f(delay):
      time.sleep(delay)
      return 1

    objectids = [f.remote(1.0), f.remote(0.5), f.remote(0.5), f.remote(0.5)]
    ready_ids, remaining_ids = ray.wait(objectids)
    self.assertTrue(len(ready_ids) == 1)
    self.assertTrue(len(remaining_ids) == 3)
    ready_ids, remaining_ids = ray.wait(objectids, num_returns=4)
    self.assertEqual(ready_ids, objectids)
    self.assertEqual(remaining_ids, [])

    objectids = [f.remote(0.5), f.remote(0.5), f.remote(0.5), f.remote(0.5)]
    start_time = time.time()
    ready_ids, remaining_ids = ray.wait(objectids, timeout=1.75, num_returns=4)
    self.assertTrue(time.time() - start_time < 2)
    self.assertEqual(len(ready_ids), 3)
    self.assertEqual(len(remaining_ids), 1)
    ray.wait(objectids)
    objectids = [f.remote(1.0), f.remote(0.5), f.remote(0.5), f.remote(0.5)]
    start_time = time.time()
    ready_ids, remaining_ids = ray.wait(objectids, timeout=5)
    self.assertTrue(time.time() - start_time < 5)
    self.assertEqual(len(ready_ids), 1)
    self.assertEqual(len(remaining_ids), 3)

    ray.worker.cleanup()

  def testCachingReusables(self):
    # Test that we can define reusable variables before the driver is connected.
    def foo_initializer():
      return 1
    def bar_initializer():
      return []
    def bar_reinitializer(bar):
      return []
    ray.reusables.foo = ray.Reusable(foo_initializer)
    ray.reusables.bar = ray.Reusable(bar_initializer, bar_reinitializer)

    @ray.remote
    def use_foo():
      return ray.reusables.foo
    @ray.remote
    def use_bar():
      ray.reusables.bar.append(1)
      return ray.reusables.bar

    ray.init(start_ray_local=True, num_workers=2)

    self.assertEqual(ray.get(use_foo.remote()), 1)
    self.assertEqual(ray.get(use_foo.remote()), 1)
    self.assertEqual(ray.get(use_bar.remote()), [1])
    self.assertEqual(ray.get(use_bar.remote()), [1])

    ray.worker.cleanup()

  def testCachingFunctionsToRun(self):
    # Test that we export functions to run on all workers before the driver is connected.
    def f(worker):
      sys.path.append(1)
    ray.worker.global_worker.run_function_on_all_workers(f)
    def f(worker):
      sys.path.append(2)
    ray.worker.global_worker.run_function_on_all_workers(f)
    def g(worker):
      sys.path.append(3)
    ray.worker.global_worker.run_function_on_all_workers(g)
    def f(worker):
      sys.path.append(4)
    ray.worker.global_worker.run_function_on_all_workers(f)

    ray.init(start_ray_local=True, num_workers=2)

    @ray.remote
    def get_state():
      time.sleep(1)
      return sys.path[-4], sys.path[-3], sys.path[-2], sys.path[-1]

    res1 = get_state.remote()
    res2 = get_state.remote()
    self.assertEqual(ray.get(res1), (1, 2, 3, 4))
    self.assertEqual(ray.get(res2), (1, 2, 3, 4))

    # Clean up the path on the workers.
    def f(worker):
      sys.path.pop()
      sys.path.pop()
      sys.path.pop()
      sys.path.pop()
    ray.worker.global_worker.run_function_on_all_workers(f)

    ray.worker.cleanup()

  def testRunningFunctionOnAllWorkers(self):
    ray.init(start_ray_local=True, num_workers=1)

    def f(worker):
      sys.path.append("fake_directory")
    ray.worker.global_worker.run_function_on_all_workers(f)
    @ray.remote
    def get_path():
      return sys.path
    self.assertEqual("fake_directory", ray.get(get_path.remote())[-1])
    def f(worker):
      sys.path.pop(-1)
    ray.worker.global_worker.run_function_on_all_workers(f)
    self.assertTrue("fake_directory" not in ray.get(get_path.remote()))

    ray.worker.cleanup()

  def testComputationGraph(self):
    ray.init(start_ray_local=True, num_workers=1)

    @ray.remote
    def f(x):
      return x
    @ray.remote
    def g(x, y):
      return x, y
    a = f.remote(1)
    b = f.remote(1)
    c = g.remote(a, b)
    c = g.remote(a, 1)
    # Make sure that we can produce a computation_graph visualization.
    ray.visualize_computation_graph(view=False)

    ray.worker.cleanup()

class ReferenceCountingTest(unittest.TestCase):

  def testDeallocation(self):
    reload(test_functions)
    for module in [ra.core, ra.random, ra.linalg, da.core, da.random, da.linalg]:
      reload(module)
    ray.init(start_ray_local=True, num_workers=1)

    def check_not_deallocated(object_ids):
      reference_counts = ray.scheduler_info()["reference_counts"]
      for object_id in object_ids:
        self.assertGreater(reference_counts[object_id.id], 0)

    def check_everything_deallocated():
      reference_counts = ray.scheduler_info()["reference_counts"]
      self.assertEqual(reference_counts, len(reference_counts) * [-1])

    z = da.zeros.remote([da.BLOCK_SIZE, 2 * da.BLOCK_SIZE])
    time.sleep(0.1)
    objectid_val = z.id
    time.sleep(0.1)
    check_not_deallocated([z])
    del z
    time.sleep(0.1)
    check_everything_deallocated()

    x = ra.zeros.remote([10, 10])
    y = ra.zeros.remote([10, 10])
    z = ra.dot.remote(x, y)
    objectid_val = x.id
    time.sleep(0.1)
    check_not_deallocated([x, y, z])
    del x
    time.sleep(0.1)
    check_not_deallocated([y, z])
    del y
    time.sleep(0.1)
    check_not_deallocated([z])
    del z
    time.sleep(0.1)
    check_everything_deallocated()

    z = da.zeros.remote([4 * da.BLOCK_SIZE])
    time.sleep(0.1)
    check_not_deallocated(ray.get(z).objectids.tolist())
    del z
    time.sleep(0.1)
    check_everything_deallocated()

    ray.worker.cleanup()

  def testGet(self):
    ray.init(start_ray_local=True, num_workers=3)

    for cls in [Foo, Bar, Baz, Qux, SubQux, Exception, CustomError, Point, NamedTupleExample]:
      ray.register_class(cls)

    # Remote objects should be deallocated when the corresponding ObjectID goes
    # out of scope, and all results of ray.get called on the ID go out of scope.
    for val in RAY_TEST_OBJECTS:
      x = ray.put(val)
      objectid = x.id
      xval = ray.get(x)
      del x, xval
      self.assertEqual(ray.scheduler_info()["reference_counts"][objectid], -1)

    # Remote objects that do not contain numpy arrays should be deallocated when
    # the corresponding ObjectID goes out of scope, even if ray.get has been
    # called on the ObjectID.
    for val in [True, False, None, 1, 1.0, 1L, "hi", u"hi", [1, 2, 3], (1, 2, 3), [(), {(): ()}]]:
      x = ray.put(val)
      objectid = x.id
      xval = ray.get(x)
      del x
      self.assertEqual(ray.scheduler_info()["reference_counts"][objectid], -1)

    # Remote objects that contain numpy arrays should not be deallocated when
    # the corresponding ObjectID goes out of scope, if ray.get has been called
    # on the ObjectID and the result of that call is still in scope.
    for val in [np.zeros(10), [np.zeros(10)], (((np.zeros(10)),),), {(): np.zeros(10)}, [1, 2, 3, np.zeros(1)]]:
      x = ray.put(val)
      objectid = x.id
      xval = ray.get(x)
      del x
      self.assertEqual(ray.scheduler_info()["reference_counts"][objectid], 1)

    # Getting an object multiple times should not be a problem. And the remote
    # object should not be deallocated until both of the results are out of scope.
    for val in [np.zeros(10), [np.zeros(10)], (((np.zeros(10)),),), {(): np.zeros(10)}, [1, 2, 3, np.zeros(1)]]:
      x = ray.put(val)
      objectid = x.id
      xval1 = ray.get(x)
      xval2 = ray.get(x)
      del xval1
      # Make sure we can still access xval2.
      xval2
      del xval2
      self.assertEqual(ray.scheduler_info()["reference_counts"][objectid], 1)
      xval3 = ray.get(x)
      xval4 = ray.get(x)
      xval5 = ray.get(x)
      del x
      del xval4, xval5
      # Make sure we can still access xval3.
      xval3
      self.assertEqual(ray.scheduler_info()["reference_counts"][objectid], 1)
      del xval3
      self.assertEqual(ray.scheduler_info()["reference_counts"][objectid], -1)

    # Getting an object multiple times and assigning it to the same name should
    # work. This was a problem in https://github.com/ray-project/ray/issues/159.
    for val in [np.zeros(10), [np.zeros(10)], (((np.zeros(10)),),), {(): np.zeros(10)}, [1, 2, 3, np.zeros(1)]]:
      x = ray.put(val)
      objectid = x.id
      xval = ray.get(x)
      xval = ray.get(x)
      xval = ray.get(x)
      xval = ray.get(x)
      self.assertEqual(ray.scheduler_info()["reference_counts"][objectid], 1)
      del x
      self.assertEqual(ray.scheduler_info()["reference_counts"][objectid], 1)
      del xval
      self.assertEqual(ray.scheduler_info()["reference_counts"][objectid], -1)

    ray.worker.cleanup()

class PythonModeTest(unittest.TestCase):

  def testPythonMode(self):
    reload(test_functions)
    ray.init(start_ray_local=True, driver_mode=ray.PYTHON_MODE)

    @ray.remote
    def f():
      return np.ones([3, 4, 5])
    xref = f.remote()
    assert_equal(xref, np.ones([3, 4, 5])) # remote functions should return by value
    assert_equal(xref, ray.get(xref)) # ray.get should be the identity
    y = np.random.normal(size=[11, 12])
    assert_equal(y, ray.put(y)) # ray.put should be the identity

    # make sure objects are immutable, this example is why we need to copy
    # arguments before passing them into remote functions in python mode
    aref = test_functions.python_mode_f.remote()
    assert_equal(aref, np.array([0, 0]))
    bref = test_functions.python_mode_g.remote(aref)
    assert_equal(aref, np.array([0, 0])) # python_mode_g should not mutate aref
    assert_equal(bref, np.array([1, 0]))

    ray.worker.cleanup()

  def testReusableVariablesInPythonMode(self):
    reload(test_functions)
    ray.init(start_ray_local=True, driver_mode=ray.PYTHON_MODE)

    def l_init():
      return []
    def l_reinit(l):
      return []
    ray.reusables.l = ray.Reusable(l_init, l_reinit)

    @ray.remote
    def use_l():
      l = ray.reusables.l
      l.append(1)
      return l

    # Get the local copy of the reusable variable. This should be stateful.
    l = ray.reusables.l
    assert_equal(l, [])

    # Make sure the remote function does what we expect.
    assert_equal(ray.get(use_l.remote()), [1])
    assert_equal(ray.get(use_l.remote()), [1])

    # Make sure the local copy of the reusable variable has not been mutated.
    assert_equal(l, [])
    l = ray.reusables.l
    assert_equal(l, [])

    # Make sure that running a remote function does not reset the state of the
    # local copy of the reusable variable.
    l.append(2)
    assert_equal(ray.get(use_l.remote()), [1])
    assert_equal(l, [2])

    ray.worker.cleanup()

class PythonCExtensionTest(unittest.TestCase):

  def testReferenceCountNone(self):
    ray.init(start_ray_local=True, num_workers=1)

    # Make sure that we aren't accidentally messing up Python's reference counts.
    @ray.remote
    def f():
      return sys.getrefcount(None)
    first_count = ray.get(f.remote())
    second_count = ray.get(f.remote())
    self.assertEqual(first_count, second_count)

    ray.worker.cleanup()

  def testReferenceCountTrue(self):
    ray.init(start_ray_local=True, num_workers=1)

    # Make sure that we aren't accidentally messing up Python's reference counts.
    @ray.remote
    def f():
      return sys.getrefcount(True)
    first_count = ray.get(f.remote())
    second_count = ray.get(f.remote())
    self.assertEqual(first_count, second_count)

    ray.worker.cleanup()

  def testReferenceCountFalse(self):
    ray.init(start_ray_local=True, num_workers=1)

    # Make sure that we aren't accidentally messing up Python's reference counts.
    @ray.remote
    def f():
      return sys.getrefcount(False)
    first_count = ray.get(f.remote())
    second_count = ray.get(f.remote())
    self.assertEqual(first_count, second_count)

    ray.worker.cleanup()

class ReusablesTest(unittest.TestCase):

  def testReusables(self):
    ray.init(start_ray_local=True, num_workers=1)

    # Test that we can add a variable to the key-value store.

    def foo_initializer():
      return 1
    def foo_reinitializer(foo):
      return foo

    ray.reusables.foo = ray.Reusable(foo_initializer, foo_reinitializer)
    self.assertEqual(ray.reusables.foo, 1)

    @ray.remote
    def use_foo():
      return ray.reusables.foo
    self.assertEqual(ray.get(use_foo.remote()), 1)
    self.assertEqual(ray.get(use_foo.remote()), 1)
    self.assertEqual(ray.get(use_foo.remote()), 1)

    # Test that we can add a variable to the key-value store, mutate it, and reset it.

    def bar_initializer():
      return [1, 2, 3]

    ray.reusables.bar = ray.Reusable(bar_initializer)

    @ray.remote
    def use_bar():
      ray.reusables.bar.append(4)
      return ray.reusables.bar
    self.assertEqual(ray.get(use_bar.remote()), [1, 2, 3, 4])
    self.assertEqual(ray.get(use_bar.remote()), [1, 2, 3, 4])
    self.assertEqual(ray.get(use_bar.remote()), [1, 2, 3, 4])

    # Test that we can use the reinitializer.

    def baz_initializer():
      return np.zeros([4])
    def baz_reinitializer(baz):
      for i in range(len(baz)):
        baz[i] = 0
      return baz

    ray.reusables.baz = ray.Reusable(baz_initializer, baz_reinitializer)

    @ray.remote
    def use_baz(i):
      baz = ray.reusables.baz
      baz[i] = 1
      return baz
    assert_equal(ray.get(use_baz.remote(0)), np.array([1, 0, 0, 0]))
    assert_equal(ray.get(use_baz.remote(1)), np.array([0, 1, 0, 0]))
    assert_equal(ray.get(use_baz.remote(2)), np.array([0, 0, 1, 0]))
    assert_equal(ray.get(use_baz.remote(3)), np.array([0, 0, 0, 1]))

    # Make sure the reinitializer is actually getting called. Note that this is
    # not the correct usage of a reinitializer because it does not reset qux to
    # its original state. This is just for testing.

    def qux_initializer():
      return 0
    def qux_reinitializer(x):
      return x + 1

    ray.reusables.qux = ray.Reusable(qux_initializer, qux_reinitializer)

    @ray.remote
    def use_qux():
      return ray.reusables.qux
    self.assertEqual(ray.get(use_qux.remote()), 0)
    self.assertEqual(ray.get(use_qux.remote()), 1)
    self.assertEqual(ray.get(use_qux.remote()), 2)

    ray.worker.cleanup()

  def testUsingReusablesOnDriver(self):
    ray.init(start_ray_local=True, num_workers=1)

    # Test that we can add a variable to the key-value store.

    def foo_initializer():
      return []
    def foo_reinitializer(foo):
      return []

    ray.reusables.foo = ray.Reusable(foo_initializer, foo_reinitializer)

    @ray.remote
    def use_foo():
      foo = ray.reusables.foo
      foo.append(1)
      return foo

    # Check that running a remote function does not reset the reusable variable
    # on the driver.
    foo = ray.reusables.foo
    self.assertEqual(foo, [])
    foo.append(2)
    self.assertEqual(foo, [2])
    foo.append(3)
    self.assertEqual(foo, [2, 3])

    self.assertEqual(ray.get(use_foo.remote()), [1])
    self.assertEqual(ray.get(use_foo.remote()), [1])
    self.assertEqual(ray.get(use_foo.remote()), [1])

    # Check that the copy of foo on the driver has not changed.
    self.assertEqual(foo, [2, 3])
    foo = ray.reusables.foo
    self.assertEqual(foo, [2, 3])

    ray.worker.cleanup()

class ClusterAttachingTest(unittest.TestCase):

  def testAttachingToCluster(self):
    node_ip_address = "127.0.0.1"
    scheduler_port = np.random.randint(40000, 50000)
    scheduler_address = "{}:{}".format(node_ip_address, scheduler_port)
    ray.services.start_scheduler(scheduler_address, cleanup=True)
    time.sleep(0.1)
    ray.services.start_node(scheduler_address, node_ip_address, num_workers=1, cleanup=True)

    ray.init(node_ip_address=node_ip_address, scheduler_address=scheduler_address)

    @ray.remote
    def f(x):
      return x + 1
    self.assertEqual(ray.get(f.remote(0)), 1)

    ray.worker.cleanup()

  def testAttachingToClusterWithMultipleObjectStores(self):
    node_ip_address = "127.0.0.1"
    scheduler_port = np.random.randint(40000, 50000)
    scheduler_address = "{}:{}".format(node_ip_address, scheduler_port)
    ray.services.start_scheduler(scheduler_address, cleanup=True)
    time.sleep(0.1)
    ray.services.start_node(scheduler_address, node_ip_address, num_workers=5, cleanup=True)
    ray.services.start_node(scheduler_address, node_ip_address, num_workers=5, cleanup=True)
    ray.services.start_node(scheduler_address, node_ip_address, num_workers=5, cleanup=True)

    ray.init(node_ip_address=node_ip_address, scheduler_address=scheduler_address)

    @ray.remote
    def f(x):
      return x + 1
    self.assertEqual(ray.get(f.remote(0)), 1)

    ray.worker.cleanup()

if __name__ == "__main__":
  unittest.main(verbosity=2)
