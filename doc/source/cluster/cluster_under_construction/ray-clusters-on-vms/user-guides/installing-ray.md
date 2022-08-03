:::{warning}
This page is under construction!
:::



# Installing Ray

We highly recommand you to use :ref:`Ray cluster launchers to  <Ray-ports>` to set up a Ray cluster.
However, you can also manually install Ray via `pip`, or using Ray docker images.

## Install Ray via `pip`

You can install the latest official version of Ray as follows.

.. code-block:: bash

  pip install -U ray  # minimal install

  # To install Ray with support for the dashboard + cluster launcher, run
  # `pip install -U "ray[default]"`

To install Ray libraries:

.. code-block:: bash

  pip install -U "ray[air]" # installs Ray + dependencies for Ray AI Runtime
  pip install -U "ray[tune]"  # installs Ray + dependencies for Ray Tune
  pip install -U "ray[rllib]"  # installs Ray + dependencies for Ray RLlib
  pip install -U "ray[serve]"  # installs Ray + dependencies for Ray Serve


## Use the Ray docker images

You can also pull a Ray Docker image from the `Ray Docker Hub <https://hub.docker.com/r/rayproject/>`__.

- The ``rayproject/ray`` `images <https://hub.docker.com/r/rayproject/ray>`__ include Ray and all required dependencies. It comes with anaconda and various versions of Python.
- The ``rayproject/ray-ml`` `images <https://hub.docker.com/r/rayproject/ray-ml>`__ include the above as well as many additional ML libraries.
- The ``rayproject/base-deps`` and ``rayproject/ray-deps`` images are for the Linux and Python dependencies respectively.

Images are `tagged` with the format ``{Ray version}[-{Python version}][-{Platform}]``. ``Ray version`` tag can be one of the following:

.. list-table::
   :widths: 25 50
   :header-rows: 1

   * - Ray version tag
     - Description
   * - latest
     - The most recent Ray release.
   * - x.y.z
     - A specific Ray release, e.g. 1.12.1
   * - nightly
     - The most recent Ray development build (a recent commit from Github ``master``)
   * - 6 character Git SHA prefix
     - A specific development build (uses a SHA from the Github ``master``, e.g. ``8960af``).

The optional ``Python version`` tag specifies the Python version in the image. All Python versions supported by Ray are available, e.g. ``py37``, ``py38``, ``py39`` and ``py310``. If unspecified, the tag points to an image using ``Python 3.7``.

The optional ``Platform`` tag specifies the platform where the image is intended for:

.. list-table::
   :widths: 16 40
   :header-rows: 1

   * - Platform tag
     - Description
   * - -cpu
     - These are based off of an Ubuntu image.
   * - -cuXX
     - These are based off of an NVIDIA CUDA image with the specified CUDA version. They require the Nvidia Docker Runtime.
   * - -gpu
     - Aliases to a specific ``-cuXX`` tagged image.
   * - <no tag>
     - Aliases to ``-cpu`` tagged images. For ``ray-ml`` image, aliases to ``-gpu`` tagged image.

Example: for the nightly image based on ``Python 3.8`` and without GPU support, the tag is ``nightly-py38-cpu``.

If you want to tweak some aspect of these images and build them locally, refer to the following script:

.. code-block:: bash

  cd ray
  ./build-docker.sh

Beyond creating the above Docker images, this script can also produce the following two images.

- The ``rayproject/development`` image has the ray source code included and is setup for development.
- The ``rayproject/examples`` image adds additional libraries for running examples.

Review images by listing them:

.. code-block:: bash

  docker images

Output should look something like the following:

.. code-block:: bash

  REPOSITORY                          TAG                 IMAGE ID            CREATED             SIZE
  rayproject/ray                      latest              7243a11ac068        2 days ago          1.11 GB
  rayproject/ray-deps                 latest              b6b39d979d73        8 days ago          996  MB
  rayproject/base-deps                latest              5606591eeab9        8 days ago          512  MB
  ubuntu                              focal               1e4467b07108        3 weeks ago         73.9 MB


Launch Ray in Docker
~~~~~~~~~~~~~~~~~~~~

Start out by launching the deployment container.

.. code-block:: bash

  docker run --shm-size=<shm-size> -t -i rayproject/ray

Replace ``<shm-size>`` with a limit appropriate for your system, for example
``512M`` or ``2G``. A good estimate for this is to use roughly 30% of your available memory (this is
what Ray uses internally for its Object Store). The ``-t`` and ``-i`` options here are required to support
interactive use of the container.

If you use a GPU version Docker image, remember to add ``--gpus all`` option. Replace ``<ray-version>`` with your target ray version in the following command:

.. code-block:: bash

  docker run --shm-size=<shm-size> -t -i --gpus all rayproject/ray:<ray-version>-gpu

**Note:** Ray requires a **large** amount of shared memory because each object
store keeps all of its objects in shared memory, so the amount of shared memory
will limit the size of the object store.

You should now see a prompt that looks something like:

.. code-block:: bash

  root@ebc78f68d100:/ray#