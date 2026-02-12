.. _working_with_robotics:

==========================
Working with Robotics Data
==========================

Ray Data provides built-in support for loading and processing robotics datasets,
with native integration for `LeRobot <https://huggingface.co/lerobot>`_ datasets from HuggingFace.

This guide shows you how to:

* :ref:`Install dependencies <robotics_installation>`
* :ref:`Load LeRobot datasets <loading_lerobot>`
* :ref:`Configure video backends <video_backends>`
* :ref:`Filter episodes <filtering_episodes>`
* :ref:`Troubleshoot common issues <robotics_troubleshooting>`

.. _robotics_installation:

Installation
============

To use :func:`~ray.data.read_lerobot`, install the required dependencies.

.. note::

    Ray Data includes built-in LeRobot utilities. You don't need to install the
    ``lerobot`` package.

Quick start (PyAV backend)
---------------------------

For the easiest setup, use the PyAV video backend:

.. code-block:: bash

    pip install huggingface_hub av

Advanced setup (TorchCodec backend)
------------------------------------

TorchCodec provides better performance for large-scale video processing, but requires
FFmpeg shared libraries.

**Install Python packages:**

.. code-block:: bash

    pip install huggingface_hub torchcodec

**Install FFmpeg libraries:**

Choose one of the following methods to install FFmpeg:

**Using conda (recommended):**

.. code-block:: bash

    conda install -c conda-forge ffmpeg

**Using system package manager:**

Ubuntu/Debian:

.. code-block:: bash

    sudo apt-get update
    sudo apt-get install ffmpeg libavcodec-dev libavformat-dev \
        libavutil-dev libswscale-dev libswresample-dev

macOS (Homebrew):

.. code-block:: bash

    brew install ffmpeg

**Custom FFmpeg location:**

If you installed FFmpeg in a custom directory, set the library path before using Python:

Linux:

.. code-block:: bash

    export LD_LIBRARY_PATH=/path/to/ffmpeg/lib:$LD_LIBRARY_PATH

macOS:

.. code-block:: bash

    export DYLD_LIBRARY_PATH=/path/to/ffmpeg/lib:$DYLD_LIBRARY_PATH

Verify installation
-------------------

Test that the dependencies loaded correctly:

.. code-block:: bash

    # Test PyAV (if you installed it)
    python -c "import av; print('PyAV works!')"

    # Test TorchCodec (if you installed it with FFmpeg)
    python -c "import torchcodec; print('TorchCodec works!')"

    # Test HuggingFace Hub
    python -c "from huggingface_hub import HfFileSystem; print('HuggingFace Hub works!')"

.. _loading_lerobot:

Loading LeRobot Datasets
========================

Ray Data provides the :func:`~ray.data.read_lerobot` function to load LeRobot datasets
from HuggingFace Hub or local paths.

Loading from HuggingFace Hub
-----------------------------

.. testcode::
    :skipif: True

    import ray

    # Load a LeRobot dataset from HuggingFace Hub
    ds = ray.data.read_lerobot(
        repo_id="lerobot/pusht",
        root="",  # Use default cache location
        delta_timestamps={
            "observation.state": [-0.1, -0.05, 0.0],
            "action": [0.0, 0.05, 0.1, 0.15],
        }
    )

    print(ds.schema())
    print(f"Total frames: {ds.count()}")

Loading from Local Path
------------------------

If you've already downloaded a LeRobot dataset locally:

.. testcode::
    :skipif: True

    import ray

    ds = ray.data.read_lerobot(
        repo_id="lerobot/pusht",
        root="/path/to/local/cache",  # Path to local dataset cache
        delta_timestamps={
            "observation.state": [-0.1, -0.05, 0.0],
            "action": [0.0, 0.05, 0.1, 0.15],
        }
    )

.. _video_backends:

Video Backends
==============

Ray Data supports two video backends for processing LeRobot datasets:

PyAV Backend
------------

The PyAV backend is recommended for easier setup.

.. testcode::
    :skipif: True

    import ray

    ds = ray.data.read_lerobot(
        repo_id="lerobot/pusht",
        root="",
        video_backend="pyav",
        delta_timestamps={
            "observation.state": [-0.1, -0.05, 0.0],
            "action": [0.0, 0.05, 0.1, 0.15],
        }
    )

**Advantages:**

* Easy to install - no additional setup required
* Works out of the box
* Reliable and well-tested

**Disadvantages:**

* Slightly slower than TorchCodec for large-scale processing

TorchCodec Backend
------------------

TorchCodec provides better performance for large-scale video processing.

.. testcode::
    :skipif: True

    import ray

    ds = ray.data.read_lerobot(
        repo_id="lerobot/pusht",
        root="",
        video_backend="torchcodec",
        delta_timestamps={
            "observation.state": [-0.1, -0.05, 0.0],
            "action": [0.0, 0.05, 0.1, 0.15],
        }
    )

**Advantages:**

* Better performance for large datasets
* Optimized for PyTorch workflows

**Disadvantages:**

* Requires FFmpeg shared libraries
* More complex setup (see :ref:`Setting up TorchCodec <setup_torchcodec>`)

.. _setup_torchcodec:
.. _robotics_troubleshooting:

Troubleshooting TorchCodec Setup
=================================

Common Issue: "Couldn't load libtorchcodec"
---------------------------------------------

**Error message:**

.. code-block:: text

    RuntimeError: Couldn't load libtorchcodec. Likely causes:
              1. FFmpeg is not properly installed in your environment.

**Diagnosis:**

TorchCodec can't find FFmpeg shared libraries. TorchCodec supports FFmpeg versions 4, 5, 6, and 7.

**Solution:**

1. **Verify FFmpeg is installed:**

   .. code-block:: bash

       # Check if FFmpeg libraries exist
       ldconfig -p | grep libavutil  # Linux
       # or
       ls /usr/local/lib/libavutil*  # macOS/Linux

2. **If FFmpeg isn't installed, install it:**

   Using conda (recommended):

   .. code-block:: bash

       conda install -c conda-forge ffmpeg

   Using apt (Ubuntu/Debian):

   .. code-block:: bash

       sudo apt-get update
       sudo apt-get install ffmpeg libavcodec-dev libavformat-dev libavutil-dev

3. **If FFmpeg is installed in a custom location, set environment variable:**

   .. code-block:: bash

       # Linux
       export LD_LIBRARY_PATH=/path/to/your/ffmpeg/lib:$LD_LIBRARY_PATH

       # macOS
       export DYLD_LIBRARY_PATH=/path/to/your/ffmpeg/lib:$DYLD_LIBRARY_PATH

   **Important:** This must be set **before** importing torchcodec in Python.

4. **Verify the fix:**

   .. code-block:: bash

       python -c "import torchcodec; print('Success!')"

.. _filtering_episodes:

Filtering Episodes
==================

LeRobot datasets are organized into episodes (trajectories). You can filter specific episodes:

.. testcode::
    :skipif: True

    import ray

    # Load only episodes 0, 1, and 2
    ds = ray.data.read_lerobot(
        repo_id="lerobot/pusht",
        root="",
        episode_indices=[0, 1, 2],
        delta_timestamps={
            "observation.state": [-0.1, -0.05, 0.0],
            "action": [0.0, 0.05, 0.1, 0.15],
        }
    )

    print(f"Loaded {ds.count()} frames from 3 episodes")

This is useful for:

* Quick testing with a subset of data
* Debugging specific episodes
* Training on specific trajectories
