Cross-language programming
==============

This page will show you how to use Ray's cross-language programming feature.

Python calling Java
-------------------

Suppose we have a Java static method and a Java class as follows:

.. code-block:: java

  // A regular Java class.
  public class Counter {

    private int value = 0;

    public int increment() {
      this.value += 1;
      return this.value;
    }

    public static int add(int a, int b) {
      return a + b;
    }
  }

Then, in Python, we can call the above Java remote function, or create an actor from the above Java class.

.. code-block:: python

  import ray

  ray.init(_include_java=True, _load_code_from_local=True)

  # Define a Java class.
  counter_class = ray.java_actor_class(
        "<your java package>.Counter")

  # Create a Java actor and call actor method.
  counter = counter_class.remote()
  obj_ref1 = counter.increment.remote()
  assert ray.get(obj_ref1) == 1
  obj_ref2 = counter.increment.remote()
  assert ray.get(obj_ref2) == 2

  # Define a Java function.
  add_function = ray.java_function(
        "<your java package>.Counter", "add")
  
  # Call the Java remote function.
  obj_ref3 = add_function.remote(1, 2)
  assert ray.get(obj_ref3) == 3

  ray.shutdown()
  
Java calling Python
-------------------

You can write a Python module as follows:

.. code-block:: python

  class Counter(object):
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value


  def add(a, b):
      return a + b

Then you can create Python actor from Java, or call Python remote function
from Java:

.. code-block:: java

  import io.ray.api.Ray;
  import io.ray.api.function.PyActorClass;
  import io.ray.api.function.PyActorMethod;
  import io.ray.api.function.PyFunction;
  import org.testng.Assert;

  public class MyRayApp {

    public static void main(String[] args) {
      Ray.init();
      
      // Define a Python class.
      PyActorClass actorClass = PyActorClass.of(
          "<your python module>", "Counter");
      
      // Create a Python actor and call actor method.
      PyActorHandle actor = Ray.actor(actorClass).remote();
      ObjectRef<Integer> objRef1 = actor.task(
          PyActorMethod.of("increment", Integer.class)).remote();
      Assert.assertEquals(objRef1.get(), 1);
      ObjectRef<Integer> objRef2 = actor.task(
          PyActorMethod.of("increment", Integer.class)).remote();
      Assert.assertEquals(objRef2.get(), 2);

      // Define a Python remote function.
      PyFunction<Integer> addFunction = PyFunction.of(
          "<your python module>", "add", Integer.class);

      // Call the Python remote function.
      ObjectRef<Integer> objRef3 = Ray.task(add_function, 1, 2).remote();
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

You can write a Python function which returns the input data:

.. code-block:: python

  @ray.remote
  def py_return_input(v):
      return v

Then you can transfer the object from Java to Python, then returns from Python
to Java:

.. code-block:: java

  import io.ray.api.Ray;
  import io.ray.api.function.PyActorClass;
  import io.ray.api.function.PyActorMethod;
  import io.ray.api.function.PyFunction;
  import org.testng.Assert;

  public class MyRayApp {

    public static void main(String[] args) {
      Ray.init();

      Object[] inputs = new Object[]{
          true,  // Boolean
          Byte.MAX_VALUE,  // Byte
          Short.MAX_VALUE,  // Short
          Integer.MAX_VALUE,  // Integer
          Long.MAX_VALUE,  // Long
          // BigInteger can support max value of 2^64-1, please refer to:
          // https://github.com/msgpack/msgpack/blob/master/spec.md#int-format-family
          // If BigInteger larger than 2^64-1, the value can only be transferred among Java workers.
          BigInteger.valueOf(Long.MAX_VALUE),  // BigInteger
          "Hello World!",  // String
          1.234f,  // Float
          1.234,  // Double
          "example binary".getBytes()};  // byte[]
      for (Object o : inputs) {
        ObjectRef res = Ray.task(
            PyFunction.of(PYTHON_MODULE, "py_return_input", o.getClass()),
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

  import io.ray.api.ObjectRef;
  import io.ray.api.Ray;
  import io.ray.api.function.PyFunction;

  public class MyRayClass {

    public static int raiseExceptionFromPython() {
      PyFunction<Integer> raiseException = PyFunction.of(
          "<your python module>", "raise_exception", Integer.class);
      ObjectRef<Integer> refObj = Ray.task(raiseException).remote();
      return refObj.get();
    }
  }

and a Python module as follows:

.. code-block:: python

  import ray

  @ray.remote
  def raise_exception():
      1 / 0

  ray.init(_include_java=True, _load_code_from_local=True)

  obj_ref = ray.java_function(
        "<your java package>.MyRayClass",
        "raiseExceptionFromPython").remote()
  ray.get(obj_ref)  # <-- raise exception from here.

  ray.shutdown()

The exception will be:

.. code-block:: text

  xxx
