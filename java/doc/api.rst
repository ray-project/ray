Ray Java API
============

Basic API
---------

``Ray.init()``
~~~~~~~~~~~~~~

``Ray.init`` is used to initialize Ray runtime. It should be called befored using
any other Ray APIs.

``@RayRemote``
~~~~~~~~~~~~~~

The ``@RayRemote`` annotation can be used to decorate static java
methods and classes.

When the annotation is used on a static method, the target method becomes
a remote function.

When the annotation is used on a class, the class becomes an actor class.
An actor is the encapsulation of state shared among many remote functions.

``Ray.call``
~~~~~~~~~~~~

``Ray.call`` is used to invoke a remote function.

.. code:: java

    RayObject<R> call(RayFunc func, ...);

The parameters of ``Ray.call`` are the target method ``func``, followed by
its original parameters.

-  The return type of ``func`` must be ``R``.
-  Currently at most 6 parameters of ``func`` are allowed.
-  Each parameter can either be the raw type ``T``, or ``RayObject<T>``.

The returned object is labeled as ``RayObject<R>`` and its value will be
put into the object store on the machine where the function call is executed.

Example:

.. code:: java

  public class Echo {

    @RayRemote
    public static String echo(String str) { return str; }

  }

  RayObject<String> res = Ray.call(Echo::echo, "hello");

``Ray.put``
~~~~~~~~~~~

You can also invoke ``Ray.put`` to explicitly place an object into the object
store.

.. code:: java

  public static <T> RayObject<T> put(T object);

Example:

.. code:: java

  RayObject<String> fooObject = Ray.put("foo");

``RayObject<T>.get``
~~~~~~~~~~~~~~~~~~~~

.. code:: java

  public class RayObject<T> {
    public T get();
  }

This method is used to fetch the value of this ``RayObject`` from the object store.
It will block the current thread until the requested data is locally available.

Example:

.. code:: java

  String foo = fooObject.get();

``Ray.wait``
~~~~~~~~~~~~

``Ray.wait`` is used to wait for a list of ``RayObject``\s to be locally available.
It will block the current thread until ``numReturns`` objects are ready or
``timeoutMs`` has passed.

.. code:: java

  public static WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns, int timeoutMs);
  public static WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns);
  public static WaitResult<T> wait(List<RayObject<T>> waitList);

Example:

.. code:: java

  WaitResult<String> waitResult = Ray.wait(waitList, 5, 1000);
  // `ready` is a list of objects that is already in local object store.
  List<RayObject<String>> ready = waitResult.getReady();
  // `unready` is the remaining objects that aren't in local object store.
  List<RayObject<String>> unready = waitResult.getUnready();

Actor Support
-------------

Create Actors
~~~~~~~~~~~~~

A regular class annotated with ``@RayRemote`` is an actor class.

.. code:: java

  @RayRemote
  public class Adder {

    private int sum;

    public Adder(int initValue) {
      sum = initValue;
    }

    public int add(int n) {
      return sum += n;
    }
  }

To create an actor instance, use ``Ray.createActor()``.

.. code:: java

    RayActor<Adder> adder = Ray.createActor(Adder::new, 0);

Similar to ``Ray.call``, the first parameter of ``Ray.createActor`` is a method that returns an instance
of the Actor class (the method can be either a constructor, or any factory methods). The rest of the parameters are
the arguments of the method.

Call Actor Methods
~~~~~~~~~~~~~~~~~~

``Ray.call`` is also used to call actor methods, where the actor instance must be the first parameter after the remote function.

.. code:: java

    RayObject<Integer> result1 = Ray.call(Adder::add, adder, 1);
    System.out.println(result1.get()); // 1
    RayObject<Integer> result2 = Ray.call(Adder::add, adder, 10);
    System.out.println(result2.get()); // 11
