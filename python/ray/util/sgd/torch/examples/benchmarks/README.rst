Running benchmarks
==================

You can run ``benchmark.py`` for benchmarking the RaySGD TorchTrainer implementation. To benchmark training on a multi-node multi-gpu cluster, you can use the `Ray Autoscaler <https://ray.readthedocs.io/en/latest/autoscaling.html#aws>`_.

Results
-------

Here are benchmarking results on synthetic data (via ``benchmark.py`` and ``horovod_benchmark_apex.py``) as of 03/04/2020:

 - PyTorch Version: torch-1.4.0-cp36-cp36m
 - Torchvision Version: torchvision-0.5.0-cp36-cp36m
 - Apex Version: commit hash 5633f6d
 - Horovod Version: horovod-0.19.0

This compares the following:

 - Horovod
 - Horovod with fp16-allreduce enabled
 - Pytorch DistributedDataParallel
 - Pytorch DistributedDataParallel with ``APEX amp`` enabled (``O1``)

.. code-block:: bash

    # Images per second for ResNet50
    # Batches per worker = 128
    # GPU Type = V100
    # Run on AWS us-east-1c, p3dn.24xlarge instances.

    Number   Horovod  Ray (PyTorch)  Horovod  Ray (PyTorch)
    of GPUs                          + Apex   + Apex
    =======  =======  =============  =======  ==============
    1 * 8    2769.7   5143           2962.7   6172
    2 * 8    5492.2   9463           5886.1   10052.8
    4 * 8    10733.4  18807          11705.9  20319.5
    8 * 8    21872.5  36911.8        23317.9  38642


.. image:: raysgd_multinode_benchmark.png
    :scale: 30%
    :align: center


Here are benchmarking results on synthetic data (via ``benchmark.py`` and ``dp_benchmark.py``) as of 03/04/2020:

 - PyTorch Version: torch-1.4.0-cp36-cp36m
 - Torchvision Version: torchvision-0.5.0-cp36-cp36m
 - Apex Version: commit hash 5633f6d

This compares the following:

 - Horovod
 - Horovod with fp16-allreduce enabled
 - Pytorch DistributedDataParallel
 - Pytorch DistributedDataParallel with ``APEX amp`` enabled (``O1``)

.. code-block:: bash

    # Images per second for ResNet50
    # Batches per worker = 128
    # GPU Type = V100
    # Run on AWS us-east-1c, p3.16xlarge instances.

    Number of GPUs  Horovod   Horovod + FP16  PyTorch    PyTorch + Amp
    ==============  =======   ==============  =======    =============
    1 * 8 GPU(s)    2273.4    2552.3          2863.6     6171.5
    2 * 8 GPU(s)    4210.5    4974.2          5640.2     8414.1
    4 * 8 GPU(s)    6633.3    9544.4          11014.8    16346.8
    8 * 8 GPU(s)    12414.6   18479.8         22273.6    33148.2


Simple Instructions
-------------------

First, ``git clone https://github.com/ray-project/ray && cd ray/python/ray/util/sgd/torch/examples/``.

Then, run ``ray up sgd-development.yaml``. You may want to install FP16 support for PyTorch with the following configuration in the YAML file:

.. code-block:: yaml

    setup_commands:
        - ray || pip install -U ray[rllib]
        - pip install -U ipdb torch torchvision
        # Install apex, but continue if this command fails.
        # For faster installation purposes, we do not install the apex cpp bindings
        # The cpp bindings can improve your benchmarked performance.
        - git clone https://github.com/NVIDIA/apex && cd apex && pip install -v --no-cache-dir  ./ || true

You should then run ``ray monitor sgd-development.yaml`` to monitor the progress of the cluster setup. When the cluster is done setting up, you should see something like the following:

.. code-block:: bash

    2020-03-05 01:24:53,613 INFO log_timer.py:17 -- AWSNodeProvider: Set tag ray-node-status=up-to-date on ['i-07ba946522fcb1d3d'] [LogTimer=134ms]
    2020-03-05 01:24:53,734 INFO log_timer.py:17 -- AWSNodeProvider: Set tag ray-runtime-config=c12bae3df69d4d6a207e90948dc4bf763319d7ed on ['i-07ba946522fcb1d3d'] [LogTimer=121ms]
    2020-03-05 01:24:58,475 INFO autoscaler.py:733 -- StandardAutoscaler: 7/7 target nodes (0 pending)
    2020-03-05 01:24:58,476 INFO autoscaler.py:734 -- LoadMetrics: MostDelayedHeartbeats={'172.31.38.189': 0.21588897705078125, '172.31.38.95': 0.21587467193603516, '172.31.42.196': 0.21586227416992188, '172.31.34.227': 0.2158496379852295, '172.31.42.101': 0.2158372402191162}, NodeIdleSeconds=Min=6 Mean=27 Max=40, NumNodesConnected=8, NumNodesUsed=0.0, ResourceUsage=0.0/512.0 CPU, 0.0/64.0 GPU, 0.0 GiB/4098.67 GiB memory, 0.0/1.0 node:172.31.34.227, 0.0/1.0 node:172.31.36.8, 0.0/1.0 node:172.31.36.82, 0.0/1.0 node:172.31.38.189, 0.0/1.0 node:172.31.38.95, 0.0/1.0 node:172.31.42.101, 0.0/1.0 node:172.31.42.196, 0.0/1.0 node:172.31.45.185, 0.0 GiB/5.45 GiB object_store_memory, TimeSinceLastHeartbeat=Min=0 Mean=0 Max=0

You can then launch a synthetic benchmark run with the following command:

.. code-block:: bash

    $ ray submit sgd-development.yaml benchmarks/benchmark.py --args="--batch-size 128"

    # Or with apex fp16
    $ ray submit sgd-development.yaml benchmarks/benchmark.py --args="--batch-size 128 --use-fp16"

You should see something like:

.. code-block:: bash

    Model: resnet50
    Batch size: 128
    Number of GPUs: 16
    Iter #0: 354.2 img/sec per GPU
    Iter #1: 354.0 img/sec per GPU
    Iter #2: 353.0 img/sec per GPU
    Iter #3: 353.3 img/sec per GPU
    Iter #4: 352.8 img/sec per GPU
    Iter #5: 348.5 img/sec per GPU
    Iter #6: 352.5 img/sec per GPU
    Iter #7: 352.5 img/sec per GPU
    Iter #8: 352.1 img/sec per GPU
    Iter #9: 352.2 img/sec per GPU
    Img/sec per GPU: 352.5 +-3.0
    Total img/sec on 16 GPU(s): 5640.2 +-47.2


You can also run ``benchmarks/horovod-benchmark.yaml`` to launch an AWS cluster that sets up Horovod on each machine.
See ``https://github.com/horovod/horovod`` for launching Horovod training. ``horovod_benchmark_apex.py`` can be used with ``horovodrun`` to obtain benchmarking results.
