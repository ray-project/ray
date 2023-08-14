.. _cross_language:

Cross-Language Programming
==========================

This page will show you how to use Ray's cross-language programming feature.

Setup the driver
-----------------

We need to set :ref:`code_search_path` in your driver.

.. tab-set::

    .. tab-item:: Python

        .. literalinclude:: ./doc_code/cross_language.py
            :language: python
            :start-after: __crosslang_init_start__
            :end-before: __crosslang_init_end__

    .. tab-item:: Java

        .. code-block:: bash

            java -classpath <classpath> \
                -Dray.address=<address> \
                -Dray.job.code-search-path=/path/to/code/ \
                <classname> <args>

You may want to include multiple directories to load both Python and Java code for workers, if they are placed in different directories.

.. tab-set::

    .. tab-item:: Python

        .. literalinclude:: ./doc_code/cross_language.py
            :language: python
            :start-after: __crosslang_multidir_start__
            :end-before: __crosslang_multidir_end__

    .. tab-item:: Java

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

.. literalinclude:: ./doc_code/cross_language.py
  :language: python
  :start-after: __python_call_java_start__
  :end-before: __python_call_java_end__

Java calling Python
-------------------

Suppose we have a Python module as follows:

.. literalinclude:: ./doc_code/cross_language.py
  :language: python
  :start-after: __python_module_start__
  :end-before: __python_module_end__

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
      // Set the code-search-path to the directory of your `ray_demo.py` file.
      System.setProperty("ray.job.code-search-path", "/path/to/the_dir/");
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

  * Be aware of float / double precision between Python and Java. If Java is using a
    float type to receive the input argument, the double precision Python data
    will be reduced to float precision in Java.
  * BigInteger can support a max value of 2^64-1, please refer to:
    https://github.com/msgpack/msgpack/blob/master/spec.md#int-format-family.
    If the value is larger than 2^64-1, then sending the value to Python will raise an exception.

The following example shows how to pass these types as parameters and how to
return these types.

You can write a Python function which returns the input data:

.. literalinclude:: ./doc_code/cross_language.py
  :language: python
  :start-after: __serialization_start__
  :end-before: __serialization_end__

Then you can transfer the object from Java to Python, and back from Python
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

.. literalinclude:: ./doc_code/cross_language.py
  :language: python
  :start-after: __raise_exception_start__
  :end-before: __raise_exception_end__

Then, run the following code:

.. literalinclude:: ./doc_code/cross_language.py
  :language: python
  :start-after: __raise_exception_demo_start__
  :end-before: __raise_exception_demo_end__

The exception stack will be:

.. code-block:: text

  Traceback (most recent call last):
    File "ray_exception_demo.py", line 9, in <module>
      ray.get(obj_ref)  # <-- raise exception from here.
    File "ray/python/ray/_private/client_mode_hook.py", line 105, in wrapper
      return func(*args, **kwargs)
    File "ray/python/ray/_private/worker.py", line 2247, in get
      raise value
  ray.exceptions.CrossLanguageError: An exception raised from JAVA:
  io.ray.api.exception.RayTaskException: (pid=61894, ip=172.17.0.2) Error executing task c8ef45ccd0112571ffffffffffffffffffffffff01000000
          at io.ray.runtime.task.TaskExecutor.execute(TaskExecutor.java:186)
          at io.ray.runtime.RayNativeRuntime.nativeRunTaskExecutor(Native Method)
          at io.ray.runtime.RayNativeRuntime.run(RayNativeRuntime.java:231)
          at io.ray.runtime.runner.worker.DefaultWorker.main(DefaultWorker.java:15)
  Caused by: io.ray.api.exception.CrossLanguageException: An exception raised from PYTHON:
  ray.exceptions.RayTaskError: ray::raise_exception() (pid=62041, ip=172.17.0.2)
    File "ray_exception.py", line 7, in raise_exception
      1 / 0
  ZeroDivisionError: division by zero
