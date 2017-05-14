from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import unittest
import ray
import numpy as np
import time
import shutil
import string
import sys
from collections import defaultdict, namedtuple

import ray.test.test_functions as test_functions

if sys.version_info >= (3, 0):
  from importlib import reload


def assert_equal(obj1, obj2):
  module_numpy = (type(obj1).__module__ == np.__name__ or
                  type(obj2).__module__ == np.__name__)
  if module_numpy:
    empty_shape = ((hasattr(obj1, "shape") and obj1.shape == ()) or
                   (hasattr(obj2, "shape") and obj2.shape == ()))
    if empty_shape:
      # This is a special case because currently np.testing.assert_equal fails
      # because we do not properly handle different numerical types.
      assert obj1 == obj2, ("Objects {} and {} are "
                            "different.".format(obj1, obj2))
    else:
      np.testing.assert_equal(obj1, obj2)
  elif hasattr(obj1, "__dict__") and hasattr(obj2, "__dict__"):
    special_keys = ["_pytype_"]
    assert (set(list(obj1.__dict__.keys()) + special_keys) ==
            set(list(obj2.__dict__.keys()) + special_keys)), ("Objects {} and "
                                                              "{} are "
                                                              "different."
                                                              .format(obj1,
                                                                      obj2))
    for key in obj1.__dict__.keys():
      if key not in special_keys:
        assert_equal(obj1.__dict__[key], obj2.__dict__[key])
  elif type(obj1) is dict or type(obj2) is dict:
    assert_equal(obj1.keys(), obj2.keys())
    for key in obj1.keys():
      assert_equal(obj1[key], obj2[key])
  elif type(obj1) is list or type(obj2) is list:
    assert len(obj1) == len(obj2), ("Objects {} and {} are lists with "
                                    "different lengths.".format(obj1, obj2))
    for i in range(len(obj1)):
      assert_equal(obj1[i], obj2[i])
  elif type(obj1) is tuple or type(obj2) is tuple:
    assert len(obj1) == len(obj2), ("Objects {} and {} are tuples with "
                                    "different lengths.".format(obj1, obj2))
    for i in range(len(obj1)):
      assert_equal(obj1[i], obj2[i])
  elif (ray.serialization.is_named_tuple(type(obj1)) or
        ray.serialization.is_named_tuple(type(obj2))):
    assert len(obj1) == len(obj2), ("Objects {} and {} are named tuples with "
                                    "different lengths.".format(obj1, obj2))
    for i in range(len(obj1)):
      assert_equal(obj1[i], obj2[i])
  else:
    assert obj1 == obj2, "Objects {} and {} are different.".format(obj1, obj2)


if sys.version_info >= (3, 0):
  long_extras = [0, np.array([["hi", u"hi"], [1.3, 1]])]
else:
  long_extras = [long(0), np.array([["hi", u"hi"], [1.3, long(1)]])]  # noqa: E501,F821

PRIMITIVE_OBJECTS = [0, 0.0, 0.9, 1 << 62, "a", string.printable, "\u262F",
                     u"hello world", u"\xff\xfe\x9c\x001\x000\x00", None, True,
                     False, [], (), {}, np.int8(3), np.int32(4), np.int64(5),
                     np.uint8(3), np.uint32(4), np.uint64(5), np.float32(1.9),
                     np.float64(1.9), np.zeros([100, 100]),
                     np.random.normal(size=[100, 100]), np.array(["hi", 3]),
                     np.array(["hi", 3], dtype=object)] + long_extras

COMPLEX_OBJECTS = [
    [[[[[[[[[[[[]]]]]]]]]]]],
    {"obj{}".format(i): np.random.normal(size=[100, 100]) for i in range(10)},
    # {(): {(): {(): {(): {(): {(): {(): {(): {(): {(): {
    #      (): {(): {}}}}}}}}}}}}},
    ((((((((((),),),),),),),),),),
    {"a": {"b": {"c": {"d": {}}}}}]


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
NamedTupleExample = namedtuple("Example",
                               "field1, field2, field3, field4, field5")

CUSTOM_OBJECTS = [Exception("Test object."), CustomError(), Point(11, y=22),
                  Foo(), Bar(), Baz(),  # Qux(), SubQux(),
                  NamedTupleExample(1, 1.0, "hi", np.zeros([3, 5]), [1, 2, 3])]

BASE_OBJECTS = PRIMITIVE_OBJECTS + COMPLEX_OBJECTS + CUSTOM_OBJECTS

LIST_OBJECTS = [[obj] for obj in BASE_OBJECTS]
TUPLE_OBJECTS = [(obj,) for obj in BASE_OBJECTS]
# The check that type(obj).__module__ != "numpy" should be unnecessary, but
# otherwise this seems to fail on Mac OS X on Travis.
DICT_OBJECTS = ([{obj: obj} for obj in PRIMITIVE_OBJECTS
                 if (obj.__hash__ is not None and
                     type(obj).__module__ != "numpy")] +
                [{0: obj} for obj in BASE_OBJECTS] +
                [{Foo(): Foo()}])

RAY_TEST_OBJECTS = BASE_OBJECTS + LIST_OBJECTS + TUPLE_OBJECTS + DICT_OBJECTS

# Check that the correct version of cloudpickle is installed.
try:
  import cloudpickle
  cloudpickle.dumps(Point)
except AttributeError:
  cloudpickle_command = "pip install --upgrade cloudpickle"
  raise Exception("You have an older version of cloudpickle that is not able "
                  "to serialize namedtuples. Try running "
                  "\n\n{}\n\n".format(cloudpickle_command))


class SerializationTest(unittest.TestCase):

  def testRecursiveObjects(self):
    ray.init(num_workers=0)

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
      self.assertRaises(Exception, lambda: ray.put(obj))

    ray.worker.cleanup()

  def testPassingArgumentsByValue(self):
    ray.init(num_workers=1)

    @ray.remote
    def f(x):
      return x

    ray.register_class(Exception)
    ray.register_class(CustomError)
    ray.register_class(Point)
    ray.register_class(Foo)
    ray.register_class(Bar)
    ray.register_class(Baz)
    ray.register_class(NamedTupleExample)

    # Check that we can pass arguments by value to remote functions and that
    # they are uncorrupted.
    for obj in RAY_TEST_OBJECTS:
      assert_equal(obj, ray.get(f.remote(obj)))

    ray.worker.cleanup()

  def testPassingArgumentsByValueOutOfTheBox(self):
    ray.init(num_workers=1)

    @ray.remote
    def f(x):
      return x

    # Test passing lambdas.

    def temp():
      return 1

    self.assertEqual(ray.get(f.remote(temp))(), 1)
    self.assertEqual(ray.get(f.remote(lambda x: x + 1))(3), 4)

    # Test sets.
    self.assertEqual(ray.get(f.remote(set())), set())
    s = set([1, (1, 2, "hi")])
    self.assertEqual(ray.get(f.remote(s)), s)

    # Test types.
    self.assertEqual(ray.get(f.remote(int)), int)
    self.assertEqual(ray.get(f.remote(float)), float)
    self.assertEqual(ray.get(f.remote(str)), str)

    class Foo(object):
      def __init__(self):
        pass

    # Make sure that we can put and get a custom type. Note that the result
    # won't be "equal" to Foo.
    ray.get(ray.put(Foo))

    ray.worker.cleanup()


class WorkerTest(unittest.TestCase):

  def testPythonWorkers(self):
    # Test the codepath for starting workers from the Python script, instead of
    # the local scheduler. This codepath is for debugging purposes only.
    num_workers = 4
    ray.worker._init(num_workers=num_workers,
                     start_workers_from_local_scheduler=False,
                     start_ray_local=True)

    @ray.remote
    def f(x):
      return x

    values = ray.get([f.remote(1) for i in range(num_workers * 2)])
    self.assertEqual(values, [1] * (num_workers * 2))
    ray.worker.cleanup()

  def testPutGet(self):
    ray.init(num_workers=0)

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

  def testRegisterClass(self):
    ray.init(num_workers=0)

    # Check that putting an object of a class that has not been registered
    # throws an exception.
    class TempClass(object):
      pass
    self.assertRaises(Exception, lambda: ray.put(TempClass()))
    # Check that registering a class that Ray cannot serialize efficiently
    # raises an exception.
    self.assertRaises(Exception, lambda: ray.register_class(defaultdict))
    # Check that registering the same class with pickle works.
    ray.register_class(defaultdict, pickle=True)
    ray.get(ray.put(defaultdict(lambda: 0)))

    ray.worker.cleanup()

  def testKeywordArgs(self):
    reload(test_functions)
    ray.init(num_workers=1)

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

    # Check that we cannot pass invalid keyword arguments to functions.
    @ray.remote
    def f1():
      return

    @ray.remote
    def f2(x, y=0, z=0):
      return

    # Make sure we get an exception if too many arguments are passed in.
    with self.assertRaises(Exception):
      f1.remote(3)

    with self.assertRaises(Exception):
      f1.remote(x=3)

    with self.assertRaises(Exception):
      f2.remote(0, w=0)

    # Make sure we get an exception if too many arguments are passed in.
    with self.assertRaises(Exception):
      f2.remote(1, 2, 3, 4)

    @ray.remote
    def f3(x):
      return x

    self.assertEqual(ray.get(f3.remote(4)), 4)

    ray.worker.cleanup()

  def testVariableNumberOfArgs(self):
    reload(test_functions)
    ray.init(num_workers=1)

    x = test_functions.varargs_fct1.remote(0, 1, 2)
    self.assertEqual(ray.get(x), "0 1 2")
    x = test_functions.varargs_fct2.remote(0, 1, 2)
    self.assertEqual(ray.get(x), "1 2")

    self.assertTrue(test_functions.kwargs_exception_thrown)
    self.assertTrue(test_functions.varargs_and_kwargs_exception_thrown)

    @ray.remote
    def f1(*args):
      return args

    @ray.remote
    def f2(x, y, *args):
      return x, y, args

    self.assertEqual(ray.get(f1.remote()), ())
    self.assertEqual(ray.get(f1.remote(1)), (1,))
    self.assertEqual(ray.get(f1.remote(1, 2, 3)), (1, 2, 3))
    with self.assertRaises(Exception):
      f2.remote()
    with self.assertRaises(Exception):
      f2.remote(1)
    self.assertEqual(ray.get(f2.remote(1, 2)), (1, 2, ()))
    self.assertEqual(ray.get(f2.remote(1, 2, 3)), (1, 2, (3,)))
    self.assertEqual(ray.get(f2.remote(1, 2, 3, 4)), (1, 2, (3, 4)))

    ray.worker.cleanup()

  def testNoArgs(self):
    reload(test_functions)
    ray.init(num_workers=1)

    ray.get(test_functions.no_op.remote())

    ray.worker.cleanup()

  def testDefiningRemoteFunctions(self):
    ray.init(num_workers=3, num_cpus=3)

    # Test that we can define a remote function in the shell.
    @ray.remote
    def f(x):
      return x + 1
    self.assertEqual(ray.get(f.remote(0)), 1)

    # Test that we can redefine the remote function.
    @ray.remote
    def f(x):
      return x + 10
    while True:
      val = ray.get(f.remote(0))
      self.assertTrue(val in [1, 10])
      if val == 10:
        break
      else:
        print("Still using old definition of f, trying again.")

    # Test that we can close over plain old data.
    data = [np.zeros([3, 5]), (1, 2, "a"), [0.0, 1.0, 1 << 62], 1 << 60,
            {"a": np.zeros(3)}]

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

    # Test that we can define remote functions that call other remote
    # functions.
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
    ray.init(num_workers=0)
    object_ids = [ray.put(i) for i in range(10)]
    self.assertEqual(ray.get(object_ids), list(range(10)))

    # Get a random choice of object IDs with duplicates.
    indices = list(np.random.choice(range(10), 5))
    indices += indices
    results = ray.get([object_ids[i] for i in indices])
    self.assertEqual(results, indices)

    ray.worker.cleanup()

  def testWait(self):
    ray.init(num_workers=1, num_cpus=1)

    @ray.remote
    def f(delay):
      time.sleep(delay)
      return 1

    objectids = [f.remote(1.0), f.remote(0.5), f.remote(0.5), f.remote(0.5)]
    ready_ids, remaining_ids = ray.wait(objectids)
    self.assertEqual(len(ready_ids), 1)
    self.assertEqual(len(remaining_ids), 3)
    ready_ids, remaining_ids = ray.wait(objectids, num_returns=4)
    self.assertEqual(set(ready_ids), set(objectids))
    self.assertEqual(remaining_ids, [])

    objectids = [f.remote(0.5), f.remote(0.5), f.remote(0.5), f.remote(0.5)]
    start_time = time.time()
    ready_ids, remaining_ids = ray.wait(objectids, timeout=1750, num_returns=4)
    self.assertLess(time.time() - start_time, 2)
    self.assertEqual(len(ready_ids), 3)
    self.assertEqual(len(remaining_ids), 1)
    ray.wait(objectids)
    objectids = [f.remote(1.0), f.remote(0.5), f.remote(0.5), f.remote(0.5)]
    start_time = time.time()
    ready_ids, remaining_ids = ray.wait(objectids, timeout=5000)
    self.assertTrue(time.time() - start_time < 5)
    self.assertEqual(len(ready_ids), 1)
    self.assertEqual(len(remaining_ids), 3)

    # Verify that calling wait with duplicate object IDs throws an exception.
    x = ray.put(1)
    self.assertRaises(Exception, lambda: ray.wait([x, x]))

    ray.worker.cleanup()

  def testMultipleWaitsAndGets(self):
    # It is important to use three workers here, so that the three tasks
    # launched in this experiment can run at the same time.
    ray.init(num_workers=3)

    @ray.remote
    def f(delay):
      time.sleep(delay)
      return 1

    @ray.remote
    def g(l):
      # The argument l should be a list containing one object ID.
      ray.wait([l[0]])

    @ray.remote
    def h(l):
      # The argument l should be a list containing one object ID.
      ray.get(l[0])

    # Make sure that multiple wait requests involving the same object ID all
    # return.
    x = f.remote(1)
    ray.get([g.remote([x]), g.remote([x])])

    # Make sure that multiple get requests involving the same object ID all
    # return.
    x = f.remote(1)
    ray.get([h.remote([x]), h.remote([x])])

    ray.worker.cleanup()

  def testCachingEnvironmentVariables(self):
    # Test that we can define environment variables before the driver is
    # connected.
    def foo_initializer():
      return 1

    def bar_initializer():
      return []

    def bar_reinitializer(bar):
      return []
    ray.env.foo = ray.EnvironmentVariable(foo_initializer)
    ray.env.bar = ray.EnvironmentVariable(bar_initializer, bar_reinitializer)

    @ray.remote
    def use_foo():
      return ray.env.foo

    @ray.remote
    def use_bar():
      ray.env.bar.append(1)
      return ray.env.bar

    ray.init(num_workers=2)

    self.assertEqual(ray.get(use_foo.remote()), 1)
    self.assertEqual(ray.get(use_foo.remote()), 1)
    self.assertEqual(ray.get(use_bar.remote()), [1])
    self.assertEqual(ray.get(use_bar.remote()), [1])

    ray.worker.cleanup()

  def testCachingFunctionsToRun(self):
    # Test that we export functions to run on all workers before the driver is
    # connected.
    def f(worker_info):
      sys.path.append(1)
    ray.worker.global_worker.run_function_on_all_workers(f)

    def f(worker_info):
      sys.path.append(2)
    ray.worker.global_worker.run_function_on_all_workers(f)

    def g(worker_info):
      sys.path.append(3)
    ray.worker.global_worker.run_function_on_all_workers(g)

    def f(worker_info):
      sys.path.append(4)
    ray.worker.global_worker.run_function_on_all_workers(f)

    ray.init(num_workers=2)

    @ray.remote
    def get_state():
      time.sleep(1)
      return sys.path[-4], sys.path[-3], sys.path[-2], sys.path[-1]

    res1 = get_state.remote()
    res2 = get_state.remote()
    self.assertEqual(ray.get(res1), (1, 2, 3, 4))
    self.assertEqual(ray.get(res2), (1, 2, 3, 4))

    # Clean up the path on the workers.
    def f(worker_info):
      sys.path.pop()
      sys.path.pop()
      sys.path.pop()
      sys.path.pop()
    ray.worker.global_worker.run_function_on_all_workers(f)

    ray.worker.cleanup()

  def testRunningFunctionOnAllWorkers(self):
    ray.init(num_workers=1)

    def f(worker_info):
      sys.path.append("fake_directory")
    ray.worker.global_worker.run_function_on_all_workers(f)

    @ray.remote
    def get_path1():
      return sys.path
    self.assertEqual("fake_directory", ray.get(get_path1.remote())[-1])

    def f(worker_info):
      sys.path.pop(-1)
    ray.worker.global_worker.run_function_on_all_workers(f)

    # Create a second remote function to guarantee that when we call
    # get_path2.remote(), the second function to run will have been run on the
    # worker.
    @ray.remote
    def get_path2():
      return sys.path
    self.assertTrue("fake_directory" not in ray.get(get_path2.remote()))

    ray.worker.cleanup()

  def testPassingInfoToAllWorkers(self):
    ray.init(num_workers=10, num_cpus=10)

    def f(worker_info):
      sys.path.append(worker_info)
    ray.worker.global_worker.run_function_on_all_workers(f)

    @ray.remote
    def get_path():
      time.sleep(1)
      return sys.path
    # Retrieve the values that we stored in the worker paths.
    paths = ray.get([get_path.remote() for _ in range(10)])
    # Add the driver's path to the list.
    paths.append(sys.path)
    worker_infos = [path[-1] for path in paths]
    for worker_info in worker_infos:
      self.assertEqual(list(worker_info.keys()), ["counter"])
    counters = [worker_info["counter"] for worker_info in worker_infos]
    # We use range(11) because the driver also runs the function.
    self.assertEqual(set(counters), set(range(11)))

    # Clean up the worker paths.
    def f(worker_info):
      sys.path.pop(-1)
    ray.worker.global_worker.run_function_on_all_workers(f)

    ray.worker.cleanup()

  def testLoggingAPI(self):
    ray.init(num_workers=1, driver_mode=ray.SILENT_MODE)

    def events():
      # This is a hack for getting the event log. It is not part of the API.
      keys = ray.worker.global_worker.redis_client.keys("event_log:*")
      return [ray.worker.global_worker.redis_client.lrange(key, 0, -1)
              for key in keys]

    def wait_for_num_events(num_events, timeout=10):
      start_time = time.time()
      while time.time() - start_time < timeout:
        if len(events()) >= num_events:
          return
        time.sleep(0.1)
      print("Timing out of wait.")

    @ray.remote
    def test_log_event():
      ray.log_event("event_type1", contents={"key": "val"})

    @ray.remote
    def test_log_span():
      with ray.log_span("event_type2", contents={"key": "val"}):
        pass

    # Make sure that we can call ray.log_event in a remote function.
    ray.get(test_log_event.remote())
    # Wait for the event to appear in the event log.
    wait_for_num_events(1)
    self.assertEqual(len(events()), 1)

    # Make sure that we can call ray.log_span in a remote function.
    ray.get(test_log_span.remote())
    # Wait for the events to appear in the event log.
    wait_for_num_events(2)
    self.assertEqual(len(events()), 2)

    @ray.remote
    def test_log_span_exception():
      with ray.log_span("event_type2", contents={"key": "val"}):
        raise Exception("This failed.")

    # Make sure that logging a span works if an exception is thrown.
    test_log_span_exception.remote()
    # Wait for the events to appear in the event log.
    wait_for_num_events(3)
    self.assertEqual(len(events()), 3)

    ray.worker.cleanup()

  def testIdenticalFunctionNames(self):
    # Define a bunch of remote functions and make sure that we don't
    # accidentally call an older version.
    ray.init(num_workers=2)

    num_calls = 200

    @ray.remote
    def f():
      return 1
    results1 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
      return 2
    results2 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
      return 3
    results3 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
      return 4
    results4 = [f.remote() for _ in range(num_calls)]

    @ray.remote
    def f():
      return 5
    results5 = [f.remote() for _ in range(num_calls)]

    self.assertEqual(ray.get(results1), num_calls * [1])
    self.assertEqual(ray.get(results2), num_calls * [2])
    self.assertEqual(ray.get(results3), num_calls * [3])
    self.assertEqual(ray.get(results4), num_calls * [4])
    self.assertEqual(ray.get(results5), num_calls * [5])

    @ray.remote
    def g():
      return 1

    @ray.remote  # noqa: F811
    def g():
      return 2

    @ray.remote  # noqa: F811
    def g():
      return 3

    @ray.remote  # noqa: F811
    def g():
      return 4

    @ray.remote  # noqa: F811
    def g():
      return 5

    result_values = ray.get([g.remote() for _ in range(num_calls)])
    self.assertEqual(result_values, num_calls * [5])

    ray.worker.cleanup()

  def testIllegalAPICalls(self):
    ray.init(num_workers=0)

    # Verify that we cannot call put on an ObjectID.
    x = ray.put(1)
    with self.assertRaises(Exception):
      ray.put(x)
    # Verify that we cannot call get on a regular value.
    with self.assertRaises(Exception):
      ray.get(3)

    ray.worker.cleanup()


class PythonModeTest(unittest.TestCase):

  def testPythonMode(self):
    reload(test_functions)
    ray.init(driver_mode=ray.PYTHON_MODE)

    @ray.remote
    def f():
      return np.ones([3, 4, 5])
    xref = f.remote()
    # Remote functions should return by value.
    assert_equal(xref, np.ones([3, 4, 5]))
    # Check that ray.get is the identity.
    assert_equal(xref, ray.get(xref))
    y = np.random.normal(size=[11, 12])
    # Check that ray.put is the identity.
    assert_equal(y, ray.put(y))

    # Make sure objects are immutable, this example is why we need to copy
    # arguments before passing them into remote functions in python mode
    aref = test_functions.python_mode_f.remote()
    assert_equal(aref, np.array([0, 0]))
    bref = test_functions.python_mode_g.remote(aref)
    # Make sure python_mode_g does not mutate aref.
    assert_equal(aref, np.array([0, 0]))
    assert_equal(bref, np.array([1, 0]))

    ray.worker.cleanup()

  def testEnvironmentVariablesInPythonMode(self):
    reload(test_functions)
    ray.init(driver_mode=ray.PYTHON_MODE)

    def l_init():
      return []

    def l_reinit(l):
      return []
    ray.env.l = ray.EnvironmentVariable(l_init, l_reinit)

    @ray.remote
    def use_l():
      l = ray.env.l
      l.append(1)
      return l

    # Get the local copy of the environment variable. This should be stateful.
    l = ray.env.l
    assert_equal(l, [])

    # Make sure the remote function does what we expect.
    assert_equal(ray.get(use_l.remote()), [1])
    assert_equal(ray.get(use_l.remote()), [1])

    # Make sure the local copy of the environment variable has not been
    # mutated.
    assert_equal(l, [])
    l = ray.env.l
    assert_equal(l, [])

    # Make sure that running a remote function does not reset the state of the
    # local copy of the environment variable.
    l.append(2)
    assert_equal(ray.get(use_l.remote()), [1])
    assert_equal(l, [2])

    ray.worker.cleanup()


class EnvironmentVariablesTest(unittest.TestCase):

  def testEnvironmentVariables(self):
    ray.init(num_workers=1)

    # Test that we can add a variable to the key-value store.

    def foo_initializer():
      return 1

    def foo_reinitializer(foo):
      return foo

    ray.env.foo = ray.EnvironmentVariable(foo_initializer, foo_reinitializer)
    self.assertEqual(ray.env.foo, 1)

    @ray.remote
    def use_foo():
      return ray.env.foo
    self.assertEqual(ray.get(use_foo.remote()), 1)
    self.assertEqual(ray.get(use_foo.remote()), 1)
    self.assertEqual(ray.get(use_foo.remote()), 1)

    # Test that we can add a variable to the key-value store, mutate it, and
    # reset it.

    def bar_initializer():
      return [1, 2, 3]

    ray.env.bar = ray.EnvironmentVariable(bar_initializer)

    @ray.remote
    def use_bar():
      ray.env.bar.append(4)
      return ray.env.bar
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

    ray.env.baz = ray.EnvironmentVariable(baz_initializer, baz_reinitializer)

    @ray.remote
    def use_baz(i):
      baz = ray.env.baz
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

    ray.env.qux = ray.EnvironmentVariable(qux_initializer, qux_reinitializer)

    @ray.remote
    def use_qux():
      return ray.env.qux
    self.assertEqual(ray.get(use_qux.remote()), 0)
    self.assertEqual(ray.get(use_qux.remote()), 1)
    self.assertEqual(ray.get(use_qux.remote()), 2)

    ray.worker.cleanup()

  def testUsingEnvironmentVariablesOnDriver(self):
    ray.init(num_workers=1)

    # Test that we can add a variable to the key-value store.

    def foo_initializer():
      return []

    def foo_reinitializer(foo):
      return []

    ray.env.foo = ray.EnvironmentVariable(foo_initializer, foo_reinitializer)

    @ray.remote
    def use_foo():
      foo = ray.env.foo
      foo.append(1)
      return foo

    # Check that running a remote function does not reset the enviroment
    # variable on the driver.
    foo = ray.env.foo
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
    foo = ray.env.foo
    self.assertEqual(foo, [2, 3])

    ray.worker.cleanup()


class UtilsTest(unittest.TestCase):

  def testCopyingDirectory(self):
    # The functionality being tested here is really multi-node functionality,
    # but this test just uses a single node.

    ray.init(num_workers=1)

    source_text = "hello world"

    temp_dir1 = os.path.join(os.path.dirname(__file__), "temp_dir1")
    source_dir = os.path.join(temp_dir1, "dir")
    source_file = os.path.join(source_dir, "file.txt")
    temp_dir2 = os.path.join(os.path.dirname(__file__), "temp_dir2")
    target_dir = os.path.join(temp_dir2, "dir")
    target_file = os.path.join(target_dir, "file.txt")

    def remove_temporary_files():
      if os.path.exists(temp_dir1):
        shutil.rmtree(temp_dir1)
      if os.path.exists(temp_dir2):
        shutil.rmtree(temp_dir2)

    # Remove the relevant files if they are left over from a previous run of
    # this test.
    remove_temporary_files()

    # Create the source files.
    os.mkdir(temp_dir1)
    os.mkdir(source_dir)
    with open(source_file, "w") as f:
      f.write(source_text)

    # Copy the source directory to the target directory.
    ray.experimental.copy_directory(source_dir, target_dir)
    time.sleep(0.5)

    # Check that the target files exist and are the same as the source files.
    self.assertTrue(os.path.exists(target_dir))
    self.assertTrue(os.path.exists(target_file))
    with open(target_file, "r") as f:
      self.assertEqual(f.read(), source_text)

    # Remove the relevant files to clean up.
    remove_temporary_files()

    ray.worker.cleanup()


class ResourcesTest(unittest.TestCase):

  def testResourceConstraints(self):
    num_workers = 20
    ray.init(num_workers=num_workers, num_cpus=10, num_gpus=2)

    # Attempt to wait for all of the workers to start up.
    ray.worker.global_worker.run_function_on_all_workers(
        lambda worker_info: sys.path.append(worker_info["counter"]))

    @ray.remote(num_cpus=0)
    def get_worker_id():
      time.sleep(1)
      return sys.path[-1]
    while True:
      if len(set(ray.get([get_worker_id.remote()
                          for _ in range(num_workers)]))) == num_workers:
        break

    time_buffer = 0.3

    # At most 10 copies of this can run at once.
    @ray.remote(num_cpus=1)
    def f(n):
      time.sleep(n)

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(10)])
    duration = time.time() - start_time
    self.assertLess(duration, 0.5 + time_buffer)
    self.assertGreater(duration, 0.5)

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(11)])
    duration = time.time() - start_time
    self.assertLess(duration, 1 + time_buffer)
    self.assertGreater(duration, 1)

    @ray.remote(num_cpus=3)
    def f(n):
      time.sleep(n)

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(3)])
    duration = time.time() - start_time
    self.assertLess(duration, 0.5 + time_buffer)
    self.assertGreater(duration, 0.5)

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(4)])
    duration = time.time() - start_time
    self.assertLess(duration, 1 + time_buffer)
    self.assertGreater(duration, 1)

    @ray.remote(num_gpus=1)
    def f(n):
      time.sleep(n)

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(2)])
    duration = time.time() - start_time
    self.assertLess(duration, 0.5 + time_buffer)
    self.assertGreater(duration, 0.5)

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(3)])
    duration = time.time() - start_time
    self.assertLess(duration, 1 + time_buffer)
    self.assertGreater(duration, 1)

    start_time = time.time()
    ray.get([f.remote(0.5) for _ in range(4)])
    duration = time.time() - start_time
    self.assertLess(duration, 1 + time_buffer)
    self.assertGreater(duration, 1)

    ray.worker.cleanup()

  def testMultiResourceConstraints(self):
    num_workers = 20
    ray.init(num_workers=num_workers, num_cpus=10, num_gpus=10)

    # Attempt to wait for all of the workers to start up.
    ray.worker.global_worker.run_function_on_all_workers(
        lambda worker_info: sys.path.append(worker_info["counter"]))

    @ray.remote(num_cpus=0)
    def get_worker_id():
      time.sleep(1)
      return sys.path[-1]
    while True:
      if len(set(ray.get([get_worker_id.remote()
                          for _ in range(num_workers)]))) == num_workers:
        break

    @ray.remote(num_cpus=1, num_gpus=9)
    def f(n):
      time.sleep(n)

    @ray.remote(num_cpus=9, num_gpus=1)
    def g(n):
      time.sleep(n)

    time_buffer = 0.3

    start_time = time.time()
    ray.get([f.remote(0.5), g.remote(0.5)])
    duration = time.time() - start_time
    self.assertLess(duration, 0.5 + time_buffer)
    self.assertGreater(duration, 0.5)

    start_time = time.time()
    ray.get([f.remote(0.5), f.remote(0.5)])
    duration = time.time() - start_time
    self.assertLess(duration, 1 + time_buffer)
    self.assertGreater(duration, 1)

    start_time = time.time()
    ray.get([g.remote(0.5), g.remote(0.5)])
    duration = time.time() - start_time
    self.assertLess(duration, 1 + time_buffer)
    self.assertGreater(duration, 1)

    start_time = time.time()
    ray.get([f.remote(0.5), f.remote(0.5), g.remote(0.5), g.remote(0.5)])
    duration = time.time() - start_time
    self.assertLess(duration, 1 + time_buffer)
    self.assertGreater(duration, 1)

    ray.worker.cleanup()

  def testGPUIDs(self):
    num_gpus = 10
    ray.init(num_cpus=10, num_gpus=num_gpus)

    @ray.remote(num_gpus=0)
    def f0():
      time.sleep(0.1)
      gpu_ids = ray.get_gpu_ids()
      assert len(gpu_ids) == 0
      for gpu_id in gpu_ids:
        assert gpu_id in range(num_gpus)
      return gpu_ids

    @ray.remote(num_gpus=1)
    def f1():
      time.sleep(0.1)
      gpu_ids = ray.get_gpu_ids()
      assert len(gpu_ids) == 1
      for gpu_id in gpu_ids:
        assert gpu_id in range(num_gpus)
      return gpu_ids

    @ray.remote(num_gpus=2)
    def f2():
      time.sleep(0.1)
      gpu_ids = ray.get_gpu_ids()
      assert len(gpu_ids) == 2
      for gpu_id in gpu_ids:
        assert gpu_id in range(num_gpus)
      return gpu_ids

    @ray.remote(num_gpus=3)
    def f3():
      time.sleep(0.1)
      gpu_ids = ray.get_gpu_ids()
      assert len(gpu_ids) == 3
      for gpu_id in gpu_ids:
        assert gpu_id in range(num_gpus)
      return gpu_ids

    @ray.remote(num_gpus=4)
    def f4():
      time.sleep(0.1)
      gpu_ids = ray.get_gpu_ids()
      assert len(gpu_ids) == 4
      for gpu_id in gpu_ids:
        assert gpu_id in range(num_gpus)
      return gpu_ids

    @ray.remote(num_gpus=5)
    def f5():
      time.sleep(0.1)
      gpu_ids = ray.get_gpu_ids()
      assert len(gpu_ids) == 5
      for gpu_id in gpu_ids:
        assert gpu_id in range(num_gpus)
      return gpu_ids

    list_of_ids = ray.get([f0.remote() for _ in range(10)])
    self.assertEqual(list_of_ids, 10 * [[]])

    list_of_ids = ray.get([f1.remote() for _ in range(10)])
    set_of_ids = set([tuple(gpu_ids) for gpu_ids in list_of_ids])
    self.assertEqual(set_of_ids, set([(i,) for i in range(10)]))

    list_of_ids = ray.get([f2.remote(), f4.remote(), f4.remote()])
    all_ids = [gpu_id for gpu_ids in list_of_ids for gpu_id in gpu_ids]
    self.assertEqual(set(all_ids), set(range(10)))

    remaining = [f5.remote() for _ in range(20)]
    for _ in range(10):
      t1 = time.time()
      ready, remaining = ray.wait(remaining, num_returns=2)
      t2 = time.time()
      # There are only 10 GPUs, and each task uses 2 GPUs, so there should only
      # be 2 tasks scheduled at a given time, so if we wait for 2 tasks to
      # finish, then it should take at least 0.1 seconds for each pair of tasks
      # to finish.
      self.assertGreater(t2 - t1, 0.09)
      list_of_ids = ray.get(ready)
      all_ids = [gpu_id for gpu_ids in list_of_ids for gpu_id in gpu_ids]
      self.assertEqual(set(all_ids), set(range(10)))

    ray.worker.cleanup()

  def testMultipleLocalSchedulers(self):
    # This test will define a bunch of tasks that can only be assigned to
    # specific local schedulers, and we will check that they are assigned to
    # the correct local schedulers.
    address_info = ray.worker._init(start_ray_local=True,
                                    num_local_schedulers=3,
                                    num_cpus=[100, 5, 10],
                                    num_gpus=[0, 5, 1])

    # Define a bunch of remote functions that all return the socket name of the
    # plasma store. Since there is a one-to-one correspondence between plasma
    # stores and local schedulers (at least right now), this can be used to
    # identify which local scheduler the task was assigned to.

    # This must be run on the zeroth local scheduler.
    @ray.remote(num_cpus=11)
    def run_on_0():
      return ray.worker.global_worker.plasma_client.store_socket_name

    # This must be run on the first local scheduler.
    @ray.remote(num_gpus=2)
    def run_on_1():
      return ray.worker.global_worker.plasma_client.store_socket_name

    # This must be run on the second local scheduler.
    @ray.remote(num_cpus=6, num_gpus=1)
    def run_on_2():
      return ray.worker.global_worker.plasma_client.store_socket_name

    # This can be run anywhere.
    @ray.remote(num_cpus=0, num_gpus=0)
    def run_on_0_1_2():
      return ray.worker.global_worker.plasma_client.store_socket_name

    # This must be run on the first or second local scheduler.
    @ray.remote(num_gpus=1)
    def run_on_1_2():
      return ray.worker.global_worker.plasma_client.store_socket_name

    # This must be run on the zeroth or second local scheduler.
    @ray.remote(num_cpus=8)
    def run_on_0_2():
      return ray.worker.global_worker.plasma_client.store_socket_name

    def run_lots_of_tasks():
      names = []
      results = []
      for i in range(100):
        index = np.random.randint(6)
        if index == 0:
          names.append("run_on_0")
          results.append(run_on_0.remote())
        elif index == 1:
          names.append("run_on_1")
          results.append(run_on_1.remote())
        elif index == 2:
          names.append("run_on_2")
          results.append(run_on_2.remote())
        elif index == 3:
          names.append("run_on_0_1_2")
          results.append(run_on_0_1_2.remote())
        elif index == 4:
          names.append("run_on_1_2")
          results.append(run_on_1_2.remote())
        elif index == 5:
          names.append("run_on_0_2")
          results.append(run_on_0_2.remote())
      return names, results

    store_names = [object_store_address.name for object_store_address
                   in address_info["object_store_addresses"]]

    def validate_names_and_results(names, results):
      for name, result in zip(names, ray.get(results)):
        if name == "run_on_0":
          self.assertIn(result, [store_names[0]])
        elif name == "run_on_1":
          self.assertIn(result, [store_names[1]])
        elif name == "run_on_2":
          self.assertIn(result, [store_names[2]])
        elif name == "run_on_0_1_2":
          self.assertIn(result, [store_names[0], store_names[1],
                                 store_names[2]])
        elif name == "run_on_1_2":
          self.assertIn(result, [store_names[1], store_names[2]])
        elif name == "run_on_0_2":
          self.assertIn(result, [store_names[0], store_names[2]])
        else:
          raise Exception("This should be unreachable.")
        self.assertEqual(set(ray.get(results)), set(store_names))

    names, results = run_lots_of_tasks()
    validate_names_and_results(names, results)

    # Make sure the same thing works when this is nested inside of a task.

    @ray.remote
    def run_nested1():
      names, results = run_lots_of_tasks()
      return names, results

    @ray.remote
    def run_nested2():
      names, results = ray.get(run_nested1.remote())
      return names, results

    names, results = ray.get(run_nested2.remote())
    validate_names_and_results(names, results)

    ray.worker.cleanup()


class WorkerPoolTests(unittest.TestCase):

  def tearDown(self):
    ray.worker.cleanup()

  def testNoWorkers(self):
    ray.init(num_workers=0)

    @ray.remote
    def f():
      return 1

    # Make sure we can call a remote function. This will require starting a new
    # worker.
    ray.get(f.remote())

    ray.get([f.remote() for _ in range(100)])

  def testBlockingTasks(self):
    ray.init(num_workers=1)

    @ray.remote
    def f(i, j):
      return (i, j)

    @ray.remote
    def g(i):
      # Each instance of g submits and blocks on the result of another remote
      # task.
      object_ids = [f.remote(i, j) for j in range(10)]
      return ray.get(object_ids)

    ray.get([g.remote(i) for i in range(100)])

    @ray.remote
    def _sleep(i):
      time.sleep(1)
      return (i)

    @ray.remote
    def sleep():
      # Each instance of sleep submits and blocks on the result of another
      # remote task, which takes one second to execute.
      ray.get([_sleep.remote(i) for i in range(10)])

    ray.get(sleep.remote())

    ray.worker.cleanup()


class SchedulingAlgorithm(unittest.TestCase):

  def attempt_to_load_balance(self, remote_function, args, total_tasks,
                              num_local_schedulers, minimum_count,
                              num_attempts=20):
    attempts = 0
    while attempts < num_attempts:
      locations = ray.get([remote_function.remote(*args)
                           for _ in range(total_tasks)])
      names = set(locations)
      counts = [locations.count(name) for name in names]
      print("Counts are {}.".format(counts))
      if len(names) == num_local_schedulers and all([count >= minimum_count
                                                     for count in counts]):
        break
      attempts += 1
    self.assertLess(attempts, num_attempts)

  def testLoadBalancing(self):
    # This test ensures that tasks are being assigned to all local schedulers
    # in a roughly equal manner.
    num_local_schedulers = 3
    num_cpus = 7
    ray.worker._init(start_ray_local=True,
                     num_local_schedulers=num_local_schedulers,
                     num_cpus=num_cpus)

    @ray.remote
    def f():
      time.sleep(0.001)
      return ray.worker.global_worker.plasma_client.store_socket_name

    self.attempt_to_load_balance(f, [], 100, num_local_schedulers, 25)
    self.attempt_to_load_balance(f, [], 1000, num_local_schedulers, 250)

    ray.worker.cleanup()

  def testLoadBalancingWithDependencies(self):
    # This test ensures that tasks are being assigned to all local schedulers
    # in a roughly equal manner even when the tasks have dependencies.
    num_workers = 3
    num_local_schedulers = 3
    ray.worker._init(start_ray_local=True, num_workers=num_workers,
                     num_local_schedulers=num_local_schedulers)

    @ray.remote
    def f(x):
      return ray.worker.global_worker.plasma_client.store_socket_name

    # This object will be local to one of the local schedulers. Make sure this
    # doesn't prevent tasks from being scheduled on other local schedulers.
    x = ray.put(np.zeros(1000000))

    self.attempt_to_load_balance(f, [x], 100, num_local_schedulers, 25)

    ray.worker.cleanup()


def wait_for_num_tasks(num_tasks, timeout=10):
  start_time = time.time()
  while time.time() - start_time < timeout:
    if len(ray.global_state.task_table()) >= num_tasks:
      return
    time.sleep(0.1)
  raise Exception("Timed out while waiting for global state.")


def wait_for_num_objects(num_objects, timeout=10):
  start_time = time.time()
  while time.time() - start_time < timeout:
    if len(ray.global_state.object_table()) >= num_objects:
      return
    time.sleep(0.1)
  raise Exception("Timed out while waiting for global state.")


class GlobalStateAPI(unittest.TestCase):

  def testGlobalStateAPI(self):
    with self.assertRaises(Exception):
      ray.global_state.object_table()

    with self.assertRaises(Exception):
      ray.global_state.task_table()

    with self.assertRaises(Exception):
      ray.global_state.client_table()

    ray.init()

    self.assertEqual(ray.global_state.object_table(), dict())

    ID_SIZE = 20

    driver_id = ray.experimental.state.binary_to_hex(
        ray.worker.global_worker.worker_id)
    driver_task_id = ray.experimental.state.binary_to_hex(
        ray.worker.global_worker.current_task_id.id())

    # One task is put in the task table which corresponds to this driver.
    wait_for_num_tasks(1)
    task_table = ray.global_state.task_table()
    self.assertEqual(len(task_table), 1)
    self.assertEqual(driver_task_id, list(task_table.keys())[0])
    self.assertEqual(task_table[driver_task_id]["State"], "RUNNING")
    self.assertEqual(task_table[driver_task_id]["TaskSpec"]["TaskID"],
                     driver_task_id)
    self.assertEqual(task_table[driver_task_id]["TaskSpec"]["ActorID"],
                     ID_SIZE * "ff")
    self.assertEqual(task_table[driver_task_id]["TaskSpec"]["Args"], [])
    self.assertEqual(task_table[driver_task_id]["TaskSpec"]["DriverID"],
                     driver_id)
    self.assertEqual(task_table[driver_task_id]["TaskSpec"]["FunctionID"],
                     ID_SIZE * "ff")
    self.assertEqual(task_table[driver_task_id]["TaskSpec"]["ReturnObjectIDs"],
                     [])

    client_table = ray.global_state.client_table()
    node_ip_address = ray.worker.global_worker.node_ip_address
    self.assertEqual(len(client_table[node_ip_address]), 3)
    manager_client = [c for c in client_table[node_ip_address]
                      if c["ClientType"] == "plasma_manager"][0]

    @ray.remote
    def f(*xs):
      return 1

    x_id = ray.put(1)
    result_id = f.remote(1, "hi", x_id)

    # Wait for one additional task to complete.
    start_time = time.time()
    while time.time() - start_time < 10:
      wait_for_num_tasks(1 + 1)
      task_table = ray.global_state.task_table()
      self.assertEqual(len(task_table), 1 + 1)
      task_id_set = set(task_table.keys())
      task_id_set.remove(driver_task_id)
      task_id = list(task_id_set)[0]
      if task_table[task_id]["State"] == "DONE":
        break
      time.sleep(0.1)
    self.assertEqual(task_table[task_id]["TaskSpec"]["ActorID"],
                     ID_SIZE * "ff")
    self.assertEqual(task_table[task_id]["TaskSpec"]["Args"], [1, "hi", x_id])
    self.assertEqual(task_table[task_id]["TaskSpec"]["DriverID"], driver_id)
    self.assertEqual(task_table[task_id]["TaskSpec"]["ReturnObjectIDs"],
                     [result_id])

    self.assertEqual(task_table[task_id], ray.global_state.task_table(task_id))

    # Wait for two objects, one for the x_id and one for result_id.
    wait_for_num_objects(2)

    def wait_for_object_table():
      timeout = 10
      start_time = time.time()
      while time.time() - start_time < timeout:
        object_table = ray.global_state.object_table()
        tables_ready = (object_table[x_id]["ManagerIDs"] is not None and
                        object_table[result_id]["ManagerIDs"] is not None)
        if tables_ready:
          return
        time.sleep(0.1)
      raise Exception("Timed out while waiting for object table to update.")

    # Wait for the object table to be updated.
    wait_for_object_table()
    object_table = ray.global_state.object_table()
    self.assertEqual(len(object_table), 2)

    self.assertEqual(object_table[x_id]["IsPut"], True)
    self.assertEqual(object_table[x_id]["TaskID"], driver_task_id)
    self.assertEqual(object_table[x_id]["ManagerIDs"],
                     [manager_client["DBClientID"]])

    self.assertEqual(object_table[result_id]["IsPut"], False)
    self.assertEqual(object_table[result_id]["TaskID"], task_id)
    self.assertEqual(object_table[result_id]["ManagerIDs"],
                     [manager_client["DBClientID"]])

    self.assertEqual(object_table[x_id], ray.global_state.object_table(x_id))
    self.assertEqual(object_table[result_id],
                     ray.global_state.object_table(result_id))

    ray.worker.cleanup()


if __name__ == "__main__":
  unittest.main(verbosity=2)
