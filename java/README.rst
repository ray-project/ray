Quick start
===========

Configuration
-------------
Ray will read your configurations in the following order:

* Java system properties: for example ``-Dray.home=/path/to/ray``.
* A ``ray.conf`` file in the classpath:`example <https://github.com/ray-project/ray/java/example.conf>`_.

For all available config items, see `this file <https://github.com/ray-project/ray/java/runtime/src/main/resources/ray.default.conf>`_.

Starting Ray
------------

.. code:: java

    Ray.init();

Read and write remote objects
-----------------------------

Each remote object is considered a ``RayObject<T>`` where ``T`` is the
type for this object. You can use ``Ray.put`` and ``RayObject<T>.get``
to write and read the objects.

.. code:: java

    Integer x = 1;
    RayObject<Integer> obj = Ray.put(x);
    Integer x1 = obj.get();
    assert (x.equals(x1));

Remote functions
----------------

Here is an ordinary java code piece for composing
``hello world example``.

.. code:: java

    public class ExampleClass {
        public static void main(String[] args) {
            String str1 = add("hello", "world");
            String str = add(str1, "example");
            System.out.println(str);
        }
        public static String add(String a, String b) {
            return a + " " + b;
        }
    }

We use ``@RayRemote`` to indicate that a function is remote, and use
``Ray.call`` to invoke it. The result from the latter is a
``RayObject<R>`` where ``R`` is the return type of the target function.
The following shows the changed example with ``add`` annotated, and
correspondent calls executed on remote machines.

.. code:: java

    public class ExampleClass {
        public static void main(String[] args) {
            Ray.init();
            RayObject<String> objStr1 = Ray.call(ExampleClass::add, "hello", "world");
            RayObject<String> objStr2 = Ray.call(ExampleClass::add, objStr1, "example");
            String str = objStr2.get();
            System.out.println(str);
        }

        @RayRemote
        public static String add(String a, String b) {
            return a + " " + b;
        }
    }

More information
================

- `Installation <https://github.com/ray-project/ray/tree/master/java/doc/installation.rst>`_
- `API document <https://github.com/ray-project/ray/tree/master/java/doc/api.rst>`_
- `Tutorial <https://github.com/ray-project/ray/tree/master/java/tutorial>`_

