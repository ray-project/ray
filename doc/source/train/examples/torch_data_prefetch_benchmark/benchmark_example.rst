:orphan:

Torch Data Prefetching Benchmark Example
========================================

We provide a benchmark example to show how the auto pipeline for host to device data transfer speeds up training on GPUs.
Running the following command gives the actual runtime of a small model training with and without the auto pipeline functionality.
The experiment size can be modified by setting different values for :code:`epochs` and :code:`num_hidden_layers`, e.g.,

.. code-block:: bash

    python auto_pipeline_for_host_to_device_data_transfer.py --epochs 2 --num_hidden_layers 2


The table below displays the runtime in seconds (excluding preparation work) under different configurations.
The first value in the parentheses reports the runtime of using the auto pipeline, and the second reports the time of not using it.
These experiments were done on a NVIDIA 2080 Ti.
The auto pipeline functionality offers more speed improvement when the model size and the number of epochs get larger.
(The actual runtime outputs may vary if these experiments are run locally or different hardware devices are used.)


======== ============= ========= ============
 Epoch    # of layers     use     not use
======== ============= ========= ============
 1            1          2.52     2.69
 1            4          6.85     7.21
 1            8          13.05    13.54
 5            1          12.14    12.88
 5            4          34.33    36.48
 5            8          66.38    69.12
 50           1          123.12   132.88
 50           4          369.42   381.67
 50           8          693.52   736.17
======== ============= ========= ============
|

.. literalinclude:: /../../python/ray/train/examples/torch_data_prefetch_benchmark/auto_pipeline_for_host_to_device_data_transfer.py
