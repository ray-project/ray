Ray Java API
============

Basic API
---------

``Ray.init()``
~~~~~~~~~~~~~~

Ray.init should be invoked before any other Ray functions to initialize
the runtime.

``@RayRemote``
~~~~~~~~~~~~~~

The annotation of ``@RayRemote`` can be used to decorate static java
method or class. The former indicates that a target function is a remote
function, which is valid with the follow requirements. \* it must be a
public static method \* parameters and return value must not be the
primitive type of java such as int, double, but could use the wrapper
class like Integer,Double \* the return value of the method must always
be the same with the same input

When the annotation is used for classes, the classes are considered
actors(a mechanism to share state among many remote functions). The
member functions can be invoked using ``Ray.call``. The requirements for
an actor class are as follows. \* it must have an constructor without
any parameter \* if it is an inner class, it must be public static \* it
must not have a member field or method decorated using
``public static``, as the semantic is undefined with multiple instances
of this same class on different machines \* an actor method must be
decorated using ``public`` but no ``static``, and the other requirements
are the same as above.

``Ray.call``
~~~~~~~~~~~~

.. code:: java

    RayObject<R> call(Func func, ...);

``func`` is the target method, continued with appropriate parameters.
There are some requirements here:

-  the return type of ``func`` must be ``R``
-  currently at most 6 parameters of ``func`` are allowed
-  each parameter must be of type ``T`` of the correspondent ``func``'s
   parameter, or be the lifted ``RayObject<T>`` to indicate a result
   from another ray call

The returned object is labled as ``RayObject<R>`` and its value will be
put into memory of the machine where the function call is executed.

``Ray.put``
~~~~~~~~~~~

You can also invoke ``Ray.put`` to explicitly place an object into local
memory.

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

This method blocks current thread until requested data gets ready and is
fetched (if needed) from remote memory to local.

``Ray.wait``
~~~~~~~~~~~~

Calling ``Ray.wait`` will block current thread and wait for specified
ray calls. It returns when at least ``numReturns`` calls are completed,
or the ``timeout`` expires. See multi-value support for ``RayList``.

.. code:: java

    public static WaitResult<T> wait(RayList<T> waitfor, int numReturns, int timeout);
    public static WaitResult<T> wait(RayList<T> waitfor, int numReturns);
    public static WaitResult<T> wait(RayList<T> waitfor);

Multi-value API
---------------

Multi-value Types
~~~~~~~~~~~~~~~~~

Java worker supports multiple ``RayObject``\ s in a single data
structure as a return value or a ray call parameter, through the
following container types.

``MultipleReturnsX<R0, R1, ...>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are multiple heterogeneous values, with their types as ``R0``,
``R1``,... respectively. Note currently this container type is only
supported as the return type of a ray call, therefore you can not use it
as the type of an input parameter.

``RayList<T>``
''''''''''''''

A list of ``RayObject<T>``, inherited from ``List<T>`` in Java. It can
be used as the type for both return value and parameters.

``RayMap<L, T>``
''''''''''''''''

A map of ``RayObject<T>`` with each indexed using a label with type
``L``, inherited from ``Map<L, T>``. It can be used as the type for both
return value and parameters.

Enable multiple heterogeneous return values
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Java worker support at most four multiple heterogeneous return values,
and in order to let the runtime know the number of return values we
supply the method of ``Ray.call_X`` as follows.

.. code:: java

    RayObjects2<R0, R1> call_2(Func func, ...);
    RayObjects3<R0, R1, R2> call_3(Func func, ...);
    RayObjects4<R0, R1, R2, R3> call_4(Func func, ...);

Note ``func`` must match the following requirements.

-  It must hava the return value of ``MultipleReturnsX``, and must be
   invoked using correspondent ``Ray.call_X``

Here is an example.

.. code:: java

    public class MultiRExample {
        public static void main(String[] args) {
            Ray.init();
            RayObjects2<Integer, String> refs = Ray.call_2(MultiRExample::sayMultiRet);
            Integer obj1 = refs.r0().get();
            String obj2 = refs.r1().get();
            Assert.assertTrue(obj1.equals(123));
            Assert.assertTrue(obj2.equals("123"));
        }

        @RayRemote
        public static MultipleReturns2<Integer, String> sayMultiRet() {
            return new MultipleReturns2<Integer, String>(123, "123");
        }
    }

Return with ``RayList``
~~~~~~~~~~~~~~~~~~~~~~~

We use ``Ray.call_n`` to do so, which is similar to ``Ray.call`` except
an additional parameter ``returnCount`` which tells the number of return
``RayObject<R>`` in ``RayList<R>``. This is because Ray core engines
needs to know it before the method is really called.

.. code:: java

    RayList<R> call_n(Func func, Integer returnCount, ...);

Here is an example.

.. code:: java

    public class ListRExample {
        public static void main(String[] args) {
            Ray.init();
            RayList<Integer> ns = Ray.call_n(ListRExample::sayList, 10, 10);
            for (int i = 0; i < 10; i++) {
                RayObject<Integer> obj = ns.Get(i);
                Assert.assertTrue(i == obj.get());
            }
        }

        @RayRemote
        public static List<Integer> sayList(Integer count) {
            ArrayList<Integer> rets = new ArrayList<>();
            for (int i = 0; i < count; i++)
                rets.add(i);
            return rets;
        }
    }

Return with ``RayMap``
~~~~~~~~~~~~~~~~~~~~~~

This is similar to ``RayList`` case, except that now each return
``RayObject<R>`` in ``RayMap<L,R>`` has a given label when
``Ray.call_n`` is made.

.. code:: java

    RayMap<L, R> call_n(Func func, Collection<L> returnLabels, ...);

Here is an example.

.. code:: java

    public class MapRExample {
        public static void main(String[] args) {
            Ray.init();
            RayMap<Integer, String> ns = Ray.call_n(MapRExample::sayMap,
                    Arrays.asList(1, 2, 4, 3), "n_futures_");
            for (Entry<Integer, RayObject<String>> ne : ns.EntrySet()) {
                Integer key = ne.getKey();
                RayObject<String> obj = ne.getValue();
                Assert.assertTrue(obj.get().equals("n_futures_" + key));
            }
        }

        @RayRemote(externalIO = true)
        public static Map<Integer, String> sayMap(Collection<Integer> ids,
                                                String prefix) {
            Map<Integer, String> ret = new HashMap<>();
            for (int id : ids) {
                ret.put(id, prefix + id);
            }
            return ret;
        }
    }

Enable ``RayList`` and ``RayMap`` as parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: java

    public class ListTExample {
        public static void main(String[] args) {
            Ray.init();
            RayList<Integer> ints = new RayList<>();
            ints.add(Ray.put(new Integer(1)));
            ints.add(Ray.put(new Integer(1)));
            ints.add(Ray.put(new Integer(1)));
            RayObject<Integer> obj = Ray.call(ListTExample::sayReadRayList，
                                            (List<Integer>)ints);
            Assert.assertTrue(obj.get().equals(3));
        }

        @RayRemote
        public static int sayReadRayList(List<Integer> ints) {
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

      public Integer add(Integer n) {
        return sum += n;
      }

      private Integer sum;
    }

Whenever you call ``Ray.create()`` method, an actor will be created, and
you get a local ``RayActor`` of that actor as the return value.

.. code:: java

    RayActor<Adder> adder = Ray.create(Adder.class);

Call Actor Methods
~~~~~~~~~~~~~~~~~~

The same ``Ray.call`` or its extended versions (e.g., ``Ray.call_n``) is
applied, except that the first argument becomes ``RayActor``.

.. code:: java

    RayObject<R> Ray.call(Func func, RayActor<Adder> actor, ...);
    RayObject<Integer> result1 = Ray.call(Adder::add, adder, 1);
    RayObject<Integer> result2 = Ray.call(Adder::add, adder, 10);
    result2.get(); // 11
