Ray Java API
============

Basic API
---------

``Ray.init()``
~~~~~~~~~~~~~~

``Ray.init`` should be invoked before any other Ray functions to initialize
the runtime.

``@RayRemote``
~~~~~~~~~~~~~~

The ``@RayRemote`` annotation can be used to decorate static java
methods and classes. The former indicates that a target function is a remote
function, which is valid with the following requirements:

- It must be a public static method.
- The method must be deterministic for task reconstruction to behave correctly.

When the annotation is used for a class, the class becomes an actor class
(an encapsulation of state shared among many remote functions). The
member functions can be invoked using ``Ray.call``. The requirements for
an actor class are as follows:

- It must have a constructor without any parameters.
- Any inner class must be public static.
- It must not have any static fields or methods, as the semantic is undefined
  with multiple instances of this same class on different machines.
- All methods that will be invoked remotely must be ``public``.

``Ray.call``
~~~~~~~~~~~~

``Ray.call`` is used to invoke a remote function.

.. code:: java

    RayObject<R> call(Func func, ...);

The parameters of ``Ray.call`` are the target method ``func``, followed by
its original parameters.

-  The return type of ``func`` must be ``R``.
-  Currently at most 6 parameters of ``func`` are allowed.
-  Each parameter can either be the raw type ``T``, or ``RayObject<T>``.

The returned object is labeled as ``RayObject<R>`` and its value will be
put into the object store on the machine where the function call is executed.

``Ray.put``
~~~~~~~~~~~

You can also invoke ``Ray.put`` to explicitly place an object into the object
store.

.. code:: java

    public static <T> RayObject<T> put(T object);
    public static <T, TM> RayObject<T> put(T obj, TM metadata);

``RayObject<T>.get/getMeta``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: java

    public class RayObject<T> {
        public T get() throws TaskExecutionException;
        public <M> M getMeta() throws TaskExecutionException;
    }

These 2 methods will block the current thread until the requested data is
locally available.

``Ray.wait``
~~~~~~~~~~~~

``Ray.wait`` is used to wait for a list of ``RayObject``\s to be locally available.
It will block the current thread until ``numReturns`` objects are ready or
``timeoutMilliseconds`` has passed.

.. code:: java

    public static WaitResult<T> wait(List<RayObject<T>> waitfor, int numReturns, int timeoutMilliseconds);
    public static WaitResult<T> wait(List<RayObject<T>> waitfor, int numReturns);
    public static WaitResult<T> wait(List<RayObject<T>> waitfor);

Actor Support
-------------

Create Actors
~~~~~~~~~~~~~

A regular class annotated with ``@RayRemote`` is an actor class.

.. code:: java

    @RayRemote
    public class Adder {
      public Adder() {
        sum = 0;
      }

      public int add(int n) {
        return sum += n;
      }

      private int sum;
    }

To create an actor instance, use ``Ray.create()``.

.. code:: java

    RayActor<Adder> adder = Ray.create(Adder.class);

Call Actor Methods
~~~~~~~~~~~~~~~~~~

``Ray.call`` or its extended versions (e.g., ``Ray.call_n``)  are also
used to call actor methods, and the actor instance must be the first parameter.

.. code:: java

    RayObject<Integer> result1 = Ray.call(Adder::add, adder, 1);
    System.out.println(result1.get()); // 1
    RayObject<Integer> result2 = Ray.call(Adder::add, adder, 10);
    System.out.println(result2.get()); // 11
