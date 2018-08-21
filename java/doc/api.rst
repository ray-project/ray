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
``timeoutMilliseconds`` has passed. See multi-value support for ``RayList``.

.. code:: java

    public static WaitResult<T> wait(RayList<T> waitfor, int numReturns, int timeoutMilliseconds);
    public static WaitResult<T> wait(RayList<T> waitfor, int numReturns);
    public static WaitResult<T> wait(RayList<T> waitfor);

Multi-value API
---------------

Multi-value Types
~~~~~~~~~~~~~~~~~

Multiple ``RayObject``\s can be placed in a single data
structure as a return value or as a ``Ray.call`` parameter through the
following container types.

``MultipleReturnsX<R0, R1, ...>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This consists of multiple heterogeneous values, with types ``R0``,
``R1``,... respectively. Currently this container type is only
supported as the return type of ``Ray.call``. Therefore you cannot use it
as the type of an input parameter.

``RayList<T>``
^^^^^^^^^^^^^^

This is a list of ``RayObject<T>``\s, which inherits from ``List<T>`` in Java. It
can be used as the type for both a return value and a parameter value.

``RayMap<L, T>``
^^^^^^^^^^^^^^^^

A map of ``RayObject<T>``\s with each indexed using a label with type
``L``, inherited from ``Map<L, T>``. It can be used as the type for both
a return value and a parameter value.

Multiple heterogeneous return values
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To return multiple heterogeneous values in a remote functions, you can
define your original method's return type as ``MultipleReturnsX`` and
then invoke it with ``Ray.call_X``. Note: ``X`` is the number of return
values, at most 4 values are supported.

Here's an `example <https://github.com/ray-project/ray/tree/master/java/tutorial/src/main/java/org/ray/exercise/Exercise05.java>`_.


Return with ``RayList``
~~~~~~~~~~~~~~~~~~~~~~~

To return a list of ``RayObject``\s, you can invoke your method with ``Ray.call_n``.
``Ray.call_n`` is similar to ``Ray.call`` except that it has an additional parameter
``returnCount``, which specifies the number of return values in the list.

Here's an `example <https://github.com/ray-project/ray/tree/master/java/tutorial/src/main/java/org/ray/exercise/Exercise06.java>`_.

Return with ``RayMap``
~~~~~~~~~~~~~~~~~~~~~~

This is similar to ``RayList`` case, except that now each returned
``RayObject<R>`` in ``RayMap<L,R>`` has a given label when
``Ray.call_n`` is called.

Here's an `example <https://github.com/ray-project/ray/tree/master/java/tutorial/src/main/java/org/ray/exercise/Exercise07.java>`_.

Use ``RayList`` and ``RayMap`` as parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: java

    public class ListTExample {
        public static void main(String[] args) {
            Ray.init();
            RayList<Integer> ints = new RayList<>();
            ints.add(Ray.put(new Integer(1)));
            ints.add(Ray.put(new Integer(1)));
            ints.add(Ray.put(new Integer(1)));
            RayObject<Integer> obj = Ray.call(ListTExample::sum，(List<Integer>)ints);
            Assert.assertTrue(obj.get().equals(3));
        }

        @RayRemote
        public static int sum(List<Integer> ints) {
            int sum = 0;
            for (Integer i : ints) {
                sum += i;
            }
            return sum;
        }
    }

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
