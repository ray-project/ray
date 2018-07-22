Ray Java Tutorial
============

Requirement
-----
::

    JDK (>=8.0)

    Maven(>=3.5.0)

Setup
-----
::

    # build native components
    ../build.sh -l java

    # build java worker
    mvn clean install -Dmaven.test.skip

    # set config file and test
    # The RAY_CONFIG please set the absolute path of the ray.config.ini in your disk
    export RAY_CONFIG=/rootpath/ray/java/ray.config.ini
    mvn test

Introduction of Api
---------
Please reference the java `README.rst`_

Exercises
---------

Each file ``java/example/src/main/java/org/ray/exercise/Exercise*.java`` is a separate exercise.
Before we run the exercise case, we have to run the command of setup step especially set RAY_CONFIG,
and entry to the java root package. And we could run the Exercise01 with the command below.

.. code-block:: shell

    java -Djava.library.path=../build/src/plasma/:../build/src/local_scheduler/ -classpath "example/target/ray-example-1.0.jar:test/lib/*" org.ray.exercise.Exercise01

**Exercise 1:** Define a remote function, and execute multiple remote functions in parallel.

**Exercise 2:** Execute remote functions in parallel with some dependencies.

**Exercise 3:** Call remote functions from within remote functions.

**Exercise 4:** Use ``Ray.wait`` to ignore stragglers.

**Exercise 5:** Enable multiple heterogeneous return values.

**Exercise 6:** Usage of ``RayList<T>``.

**Exercise 7:** Usage of ``RayMap<L, T>``.

**Exercise 8:** Actor Support of create Actor and call Actor method.

.. _`README.rst`: https://github.com/ray-project/ray/tree/master/java

