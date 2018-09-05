Ray Java Tutorial
=================

- `Installation guide <https://github.com/ray-project/ray/tree/master/java/doc/installation.rst>`_
- `API document <https://github.com/ray-project/ray/tree/master/java/doc/api.rst>`_

Exercises
---------

Each file ``java/example/src/main/java/org/ray/exercise/Exercise*.java`` is a separate exercise.
To run a exercise case, set the ``RAY_CONFIG`` env variable and run the following command in ``ray/java/`` directory.

.. code-block:: shell

    java -Djava.library.path=../build/src/plasma/:../build/src/local_scheduler/ -classpath "tutorial/target/ray-tutorial-1.0.jar:tutorial/lib/*" org.ray.exercise.Exercise01

`Exercise 1 <https://github.com/ray-project/ray/tree/master/java/tutorial/src/main/java/org/ray/exercise/Exercise01.java>`_: Define a remote function, and execute multiple remote functions in parallel.

`Exercise 2 <https://github.com/ray-project/ray/tree/master/java/tutorial/src/main/java/org/ray/exercise/Exercise02.java>`_: Execute remote functions in parallel with some dependencies.

`Exercise 3 <https://github.com/ray-project/ray/tree/master/java/tutorial/src/main/java/org/ray/exercise/Exercise03.java>`_: Call remote functions from within remote functions.

`Exercise 4 <https://github.com/ray-project/ray/tree/master/java/tutorial/src/main/java/org/ray/exercise/Exercise04.java>`_: Use ``Ray.wait`` to ignore stragglers.

`Exercise 5 <https://github.com/ray-project/ray/tree/master/java/tutorial/src/main/java/org/ray/exercise/Exercise08.java>`_: Actor Support of create Actor and call Actor method.
