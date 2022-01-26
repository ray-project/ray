.. _cross_language:

Cross-Language Programming
==========================

This page will show you how to use Ray's cross-language programming feature.

Setup the driver
-----------------

We need to set :ref:`code_search_path` in your driver.

.. tabbed:: Python

    .. code-block:: python

      ray.init(job_config=ray.job_config.JobConfig(code_search_path="/path/to/code"))

.. tabbed:: Java

    .. code-block:: bash

        java -classpath <classpath> \
            -Dray.address=<address> \
            -Dray.job.code-search-path=/path/to/code/ \
            <classname> <args>

You may want to include multiple directories to load both Python and Java code for workers, if they are placed in different directories.

.. tabbed:: Python

    .. code-block:: python

      ray.init(job_config=ray.job_config.JobConfig(code_search_path="/path/to/jars:/path/to/pys"))

.. tabbed:: Java

    .. code-block:: bash

        java -classpath <classpath> \
            -Dray.address=<address> \
            -Dray.job.code-search-path=/path/to/jars:/path/to/pys \
            <classname> <args>

Python calling Java
-------------------

Suppose we have a Java static method and a Java class as follows:

.. code-block:: java

  package io.ray.demo;

  public class Math {

    public static int add(int a, int b) {
      return a + b;
    }
  }

.. code-block:: java

  package io.ray.demo;

  // A regular Java class.
  public class Counter {

    private int value = 0;

    public int increment() {
      this.value += 1;
      return this.value;
    }
  }

Then, in Python, we can call the above Java remote function, or create an actor
from the above Java class.

.. code-block:: python

  import ray

  ray.init(address="auto")

  # Define a Java class.
  counter_class = ray.java_actor_class(
        "io.ray.demo.Counter")

  # Create a Java actor and call actor method.
  counter = counter_class.remote()
  obj_ref1 = counter.increment.remote()
  assert ray.get(obj_ref1) == 1
  obj_ref2 = counter.increment.remote()
  assert ray.get(obj_ref2) == 2

  # Define a Java function.
  add_function = ray.java_function(
        "io.ray.demo.Math", "add")

  # Call the Java remote function.
  obj_ref3 = add_function.remote(1, 2)
  assert ray.get(obj_ref3) == 3

  ray.shutdown()

Java calling Python
-------------------

Suppose we have a Python module as follows:

.. code-block:: python

  # ray_demo.py

  import ray

  @ray.remote
  class Counter(object):
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value

  @ray.remote
  def add(a, b):
      return a + b

.. note::

  * The function or class should be decorated by `@ray.remote`.

Then, in Java, we can call the above Python remote function, or create an actor
from the above Python class.

.. code-block:: java

  package io.ray.demo;

  import io.ray.api.ObjectRef;
  import io.ray.api.PyActorHandle;
  import io.ray.api.Ray;
  import io.ray.api.function.PyActorClass;
  import io.ray.api.function.PyActorMethod;
  import io.ray.api.function.PyFunction;
  import org.testng.Assert;

  public class JavaCallPythonDemo {

    public static void main(String[] args) {
      Ray.init();

      // Define a Python class.
      PyActorClass actorClass = PyActorClass.of(
          "ray_demo", "Counter");

      // Create a Python actor and call actor method.
      PyActorHandle actor = Ray.actor(actorClass).remote();
      ObjectRef objRef1 = actor.task(
          PyActorMethod.of("increment", int.class)).remote();
      Assert.assertEquals(objRef1.get(), 1);
      ObjectRef objRef2 = actor.task(
          PyActorMethod.of("increment", int.class)).remote();
      Assert.assertEquals(objRef2.get(), 2);

      // Call the Python remote function.
      ObjectRef objRef3 = Ray.task(PyFunction.of(
          "ray_demo", "add", int.class), 1, 2).remote();
      Assert.assertEquals(objRef3.get(), 3);

      Ray.shutdown();
    }
  }

Cross-language data serialization
---------------------------------

The arguments and return values of ray call can be serialized & deserialized
automatically if their types are the following:

  - Primitive data types
      ===========   =======  =======
      MessagePack   Python   Java
      ===========   =======  =======
      nil           None     null
      bool          bool     Boolean
      int           int      Short / Integer / Long / BigInteger
      float         float    Float / Double
      str           str      String
      bin           bytes    byte[]
      ===========   =======  =======

  - Basic container types
      ===========   =======  =======
      MessagePack   Python   Java
      ===========   =======  =======
      array         list     Array
      ===========   =======  =======

  - Ray builtin types
      - ActorHandle

.. note::

  * Be aware of float / double precision between Python and Java. If Java use a
    float type to receive the input argument, the double precision Python data
    will be reduced to float precision in Java.
  * BigInteger can support max value of 2^64-1, please refer to:
    https://github.com/msgpack/msgpack/blob/master/spec.md#int-format-family.
    If the value larger than 2^64-1, then transfer the BigInteger:

      - From Java to Python: *raise an exception*
      - From Java to Java: **OK**

The following example shows how to pass these types as parameters and how to
return return these types.

You can write a Python function which returns the input data:

.. code-block:: python

  # ray_serialization.py

  import ray

  @ray.remote
  def py_return_input(v):
      return v

Then you can transfer the object from Java to Python, then returns from Python
to Java:

.. code-block:: java

  package io.ray.demo;

  import io.ray.api.ObjectRef;
  import io.ray.api.Ray;
  import io.ray.api.function.PyFunction;
  import java.math.BigInteger;
  import org.testng.Assert;

  public class SerializationDemo {

    public static void main(String[] args) {
      Ray.init();

      Object[] inputs = new Object[]{
          true,  // Boolean
          Byte.MAX_VALUE,  // Byte
          Short.MAX_VALUE,  // Short
          Integer.MAX_VALUE,  // Integer
          Long.MAX_VALUE,  // Long
          BigInteger.valueOf(Long.MAX_VALUE),  // BigInteger
          "Hello World!",  // String
          1.234f,  // Float
          1.234,  // Double
          "example binary".getBytes()};  // byte[]
      for (Object o : inputs) {
        ObjectRef res = Ray.task(
            PyFunction.of("ray_serialization", "py_return_input", o.getClass()),
            o).remote();
        Assert.assertEquals(res.get(), o);
      }

      Ray.shutdown();
    }
  }

Cross-language exception stacks
-------------------------------

Suppose we have a Java package as follows:

.. code-block:: java

  package io.ray.demo;

  import io.ray.api.ObjectRef;
  import io.ray.api.Ray;
  import io.ray.api.function.PyFunction;

  public class MyRayClass {

    public static int raiseExceptionFromPython() {
      PyFunction<Integer> raiseException = PyFunction.of(
          "ray_exception", "raise_exception", Integer.class);
      ObjectRef<Integer> refObj = Ray.task(raiseException).remote();
      return refObj.get();
    }
  }

and a Python module as follows:

.. code-block:: python

  # ray_exception.py

  import ray

  @ray.remote
  def raise_exception():
      1 / 0

Then, run the following code:

.. code-block:: python

  # ray_exception_demo.py

  import ray

  ray.init(address="auto")

  obj_ref = ray.java_function(
        "io.ray.demo.MyRayClass",
        "raiseExceptionFromPython").remote()
  ray.get(obj_ref)  # <-- raise exception from here.

  ray.shutdown()

The exception stack will be:

.. code-block:: text

  Traceback (most recent call last):
    File "ray_exception_demo.py", line 10, in <module>
      ray.get(obj_ref)  # <-- raise exception from here.
    File "ray/worker.py", line 1425, in get
      raise value
  ray.exceptions.CrossLanguageError: An exception raised from JAVA:
  io.ray.runtime.exception.RayTaskException: (pid=92253, ip=10.15.239.68) Error executing task df5a1a828c9685d3ffffffff01000000
    at io.ray.runtime.task.TaskExecutor.execute(TaskExecutor.java:167)
  Caused by: io.ray.runtime.exception.CrossLanguageException: An exception raised from PYTHON:
  ray.exceptions.RayTaskError: ray::raise_exception() (pid=92252, ip=10.15.239.68)
    File "python/ray/_raylet.pyx", line 482, in ray._raylet.execute_task
    File "ray_exception.py", line 7, in raise_exception
      1 / 0
  ZeroDivisionError: division by zero
