:orphan:

Torch Data Prefetching Benchmark Example
========================================

We provide a benchmark example to show how the auto pipeline for host to device data transfer speeds up training on GPUs.
Running the following command gives the actual runtime of a small model training with and without the auto pipeline functionality.
The experiment size can be modified by setting different values for :code:`epochs` and :code:`num_hidden_layers`, e.g.,

.. code-block:: bash

    python auto_pipeline_for_host_to_device_data_transfer.py --epochs 2 --num_hidden_layers 2


The table below displays the runtime (excluding preparation work) under different configurations.
The first value in the parentheses reports the runtime of using the auto pipeline, and the second reports the time of not using it.
These experiments were done on a NVIDIA 2080 Ti.
The auto pipeline functionality offers more speed improvement when the model size and the number of epochs get larger.
(The actual runtime outputs may vary if these experiments are run locally or different hardware devices are used.)


+--------+----------+---------+---------+
| epoch  | # layers | use     | not use |
+========+==========+=========+=========+
| 1      |  1       | 2.52s   | 2.69s   |
+--------+----------+---------+---------+
| 1      |  4       | 6.85s   | 7.21s   |
+--------+----------+---------+---------+
| 1      |  8       | 13.05   | 13.54s  |
+--------+----------+---------+---------+
| 5      |  1       | 12.14s  | 12.88s  |
+--------+----------+---------+---------+
| 5      |  4       | 34.33s  | 36.48s  |
+--------+----------+---------+---------+
| 5      |  8       | 66.38s  |  69.12s |
+--------+----------+---------+---------+
| 50     |  1       | 123.12s | 132.88s |
+--------+----------+---------+---------+
| 50     |  4       | 369.42s | 381.67s |
+--------+----------+---------+---------+
| 50     |  8       | 693.52s | 736.17s |
+--------+----------+---------+---------+

|
.. literalinclude:: /../../python/ray/train/examples/torch_data_prefetch_benchmark/auto_pipeline_for_host_to_device_data_transfer.py
    :language: python
