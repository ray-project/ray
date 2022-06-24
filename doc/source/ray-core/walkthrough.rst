.. include:: /_includes/core/announcement.rst

.. _core-walkthrough:

Ray Core Walkthrough
====================

This walkthrough will overview the core concepts of Ray:

1. Starting Ray
2. Using remote functions (tasks)
3. Using remote classes (actors)
4. Working with Ray Objects

With Ray, your code will work on a single machine and can be easily scaled to large cluster.

Java demo code in this documentation can be found `here <https://github.com/ray-project/ray/blob/master/java/test/src/main/java/io/ray/docdemo/WalkthroughDemo.java>`__.

Installation
------------

.. tabbed:: Python

    To run this walkthrough, install Ray with ``pip install -U ray``. For the latest wheels (for a snapshot of ``master``), you can use these instructions at :ref:`install-nightlies`.

.. tabbed:: Java

    To run this walkthrough, add `Ray API <https://mvnrepository.com/artifact/io.ray/ray-api>`_ and `Ray Runtime <https://mvnrepository.com/artifact/io.ray/ray-runtime>`_ as dependencies. Snapshot versions can be found in `sonatype repository <https://oss.sonatype.org/#nexus-search;quick~io.ray>`_.

    Note: To run your Ray Java application, you need to install Ray Python with `pip install -U ray` first. (For Ray Java snapshot versions, install nightly Ray Python wheels.) The versions of Ray Java and Ray Python must match.

.. tabbed:: C++

    The C++ Ray API is currently experimental with limited support and it's not supported on Windows. You can track its development `here <https://github.com/ray-project/ray/milestone/17>`__ and report issues on GitHub.
    Run the following commands to get started:

    Install ray with C++ API support and generate a bazel project with the ray command.

    .. code-block:: shell

      pip install "ray[cpp]"
      mkdir ray-template && ray cpp --generate-bazel-project-template-to ray-template

    The project template comes with a simple example application. You can try this example out in 2 ways:

    1. Run the example application directly, which will start a Ray cluster locally.

    .. code-block:: shell

      cd ray-template && bash run.sh

    2. Connect the example application to an existing Ray cluster by specifying the RAY_ADDRESS env var.

    .. code-block:: shell

      ray start --head
      RAY_ADDRESS=127.0.0.1:6379 bash run.sh

    Now you can build your own Ray C++ application based on this project template.

    .. note::

      If you build Ray from source, please remove the build option ``build --cxxopt="-D_GLIBCXX_USE_CXX11_ABI=0"`` from the file ``cpp/example/.bazelrc`` before you run the example application. The related issue is `here <https://github.com/ray-project/ray/issues/26031>`_.

Starting Ray
------------

You can start Ray on a single machine by adding this to your code.

.. note::

  In recent versions of Ray (>=1.5), ``ray.init()`` will automatically be called on the first use of a Ray remote API.

.. tabbed:: Python

    .. code-block:: python

        import ray

        # Start Ray. If you're connecting to an existing cluster, you would use
        # ray.init(address=<cluster-address>) instead.
        ray.init()

        ...

.. tabbed:: Java

    .. code-block:: java

        import io.ray.api.Ray;

        public class MyRayApp {

            public static void main(String[] args) {
                // Start Ray runtime. If you're connecting to an existing cluster, you can set
                // the `-Dray.address=<cluster-address>` java system property.
                Ray.init();
                ...
            }
        }

.. tabbed:: C++

    .. code-block:: c++

        // Run `ray cpp --show-library-path` to find headers and libraries.
        #include <ray/api.h>

        int main(int argc, char **argv) {
            // Start Ray runtime. If you're connecting to an existing cluster, you can set
            // the `RAY_ADDRESS` env var.
            ray::Init();
            ...
        }


Ray will then be able to utilize all cores of your machine. Find out how to configure the number of cores Ray will use at :ref:`configuring-ray`.

To start a multi-node Ray cluster, see the :ref:`cluster setup page <cluster-index>`.

Using Tasks, Actors, and Objects
--------------------------------

Click through below to walk through using Ray's key concepts: Tasks, Actors, and Objects.

.. tip::

    We suggest reading through the walkthrough for each section prior to browsing more deeply into the materials.

.. panels::
    :container: container pb-4
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-50 d-block mx-auto

    ---
    :img-top: /images/tasks.png

    .. link-button:: ray-remote-functions
        :type: ref
        :text: Using remote functions (Tasks)
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/actors.png

    .. link-button:: ray-remote-classes
        :type: ref
        :text: Using remote classes (Actors)
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/objects.png

    .. link-button:: objects-in-ray
        :type: ref
        :text: Working with Ray Objects
        :classes: btn-link btn-block stretched-link


.. include:: /_includes/core/announcement_bottom.rst
