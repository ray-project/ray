.. _objects-in-ray:

Objects
=======

In Ray, we can create and compute on objects. We refer to these objects as **remote objects**, and we use **object refs** to refer to them. Remote objects are stored in `shared-memory <https://en.wikipedia.org/wiki/Shared_memory>`__ **object stores**, and there is one object store per node in the cluster. In the cluster setting, we may not actually know which machine each object lives on.

An **object ref** is essentially a unique ID that can be used to refer to a
remote object. If you're familiar with futures, our object refs are conceptually
similar.

Object refs can be created in multiple ways.

  1. They are returned by remote function calls.
  2. They are returned by ``put`` (:ref:`docstring <ray-put-ref>`).

.. tabbed:: Python

  .. code-block:: python

    # Put an object in Ray's object store.
    y = 1
    object_ref = ray.put(y)

.. tabbed:: Java

  .. code-block:: java

    // Put an object in Ray's object store.
    int y = 1;
    ObjectRef<Integer> objectRef = Ray.put(y);

.. tabbed:: C++

  .. code-block:: c++

    // Put an object in Ray's object store.
    int y = 1;
    ray::ObjectRef<int> object_ref = ray::Put(y);

.. note::

    Remote objects are immutable. That is, their values cannot be changed after
    creation. This allows remote objects to be replicated in multiple object
    stores without needing to synchronize the copies.


Fetching Object Data
--------------------

You can use the ``get`` method (:ref:`docstring <ray-get-ref>`) to fetch the result of a remote object from an object ref.
If the current node's object store does not contain the object, the object is downloaded.

.. tabbed:: Python

    If the object is a `numpy array <https://docs.scipy.org/doc/numpy/reference/generated/numpy.array.html>`__
    or a collection of numpy arrays, the ``get`` call is zero-copy and returns arrays backed by shared object store memory.
    Otherwise, we deserialize the object data into a Python object.

    .. code-block:: python

      # Get the value of one object ref.
      obj_ref = ray.put(1)
      assert ray.get(obj_ref) == 1

      # Get the values of multiple object refs in parallel.
      assert ray.get([ray.put(i) for i in range(3)]) == [0, 1, 2]

      # You can also set a timeout to return early from a ``get``
      # that's blocking for too long.
      from ray.exceptions import GetTimeoutError

      @ray.remote

      def long_running_function():
          time.sleep(8)

      obj_ref = long_running_function.remote()
      try:
          ray.get(obj_ref, timeout=4)
      except GetTimeoutError:
          print("`get` timed out.")

.. tabbed:: Java

    .. code-block:: java

      // Get the value of one object ref.
      ObjectRef<Integer> objRef = Ray.put(1);
      Assert.assertTrue(objRef.get() == 1);
      // You can also set a timeout(ms) to return early from a ``get`` that's blocking for too long.
      Assert.assertTrue(objRef.get(1000) == 1);

      // Get the values of multiple object refs in parallel.
      List<ObjectRef<Integer>> objectRefs = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
	objectRefs.add(Ray.put(i));
      }
      List<Integer> results = Ray.get(objectRefs);
      Assert.assertEquals(results, ImmutableList.of(0, 1, 2));

      // Ray.get timeout example: Ray.get will throw an RayTimeoutException if time out.
      public class MyRayApp {
        public static int slowFunction() throws InterruptedException {
          TimeUnit.SECONDS.sleep(10);
          return 1;
        }
      }
      Assert.assertThrows(RayTimeoutException.class, 
        () -> Ray.get(Ray.task(MyRayApp::slowFunction).remote(), 3000));

.. tabbed:: C++

    .. code-block:: c++

      // Get the value of one object ref.
      ray::ObjectRef<int> obj_ref = ray::Put(1);
      assert(*obj_ref.Get() == 1);

      // Get the values of multiple object refs in parallel.
      std::vector<ray::ObjectRef<int>> obj_refs;
      for (int i = 0; i < 3; i++) {
        obj_refs.emplace_back(ray::Put(i));
      }
      auto results = ray::Get(obj_refs);
      assert(results.size() == 3);
      assert(*results[0] == 0);
      assert(*results[1] == 1);
      assert(*results[2] == 2);

More about Ray Objects
----------------------

.. toctree::
    :maxdepth: -1

    objects/pass-by-reference.rst
    objects/serialization.rst
    objects/object-spilling.rst
    objects/memory-management.rst
