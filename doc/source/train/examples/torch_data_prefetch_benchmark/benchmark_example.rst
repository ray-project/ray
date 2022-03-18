:orphan:

Torch Data Prefetching Benchmark
================================

We provide a benchmark example to show how the auto pipeline for host to device data transfer speeds up training on GPUs.
This functionality can be easily enabled by setting ``auto_transfer=True`` in :ref:`train.torch.prepare_data_loader() <train-api-torch-prepare-data-loader>`.

.. code-block:: python

    from torch.utils.data import DataLoader
    from ray import train

    ...

    data_loader = DataLoader(my_dataset, batch_size)
    train_loader = train.torch.prepare_data_loader(
        data_loader=train_loader, move_to_device=True, auto_transfer=True
    )


Running the following command gives the runtime of a small model training with and without the auto pipeline functionality.
The experiment size can be modified by setting different values for ``epochs`` and ``num_hidden_layers``, e.g.,

.. code-block:: bash

    python auto_pipeline_for_host_to_device_data_transfer.py --epochs 2 --num_hidden_layers 2


The table below displays the runtime in seconds (excluding preparation work) under different configurations.
The first value in the parentheses reports the runtime of using the auto pipeline, and the second reports the time of not using it.
These experiments were done on a NVIDIA 2080 Ti.
The auto pipeline functionality offers more speed improvement when the model size and the number of epochs gets larger.
(The actual runtime outputs may vary if these experiments are run locally or different hardware devices are used.)


========== =================== ======================== ========================
 `epochs`    `num_of_layers`    `auto_transfer=False`     `auto_transfer=True`
========== =================== ======================== ========================
    1            1                      2.69                      2.52
    1            4                      7.21                      6.85
    1            8                      13.54                     13.05
    5            1                      12.88                     12.14
    5            4                      36.48                     34.33
    5            8                      69.12                     66.38
    50           1                      132.88                    123.12
    50           4                      381.67                    369.42
    50           8                      736.17                    693.52
========== =================== ======================== ========================


.. literalinclude:: /../../python/ray/train/examples/torch_data_prefetch_benchmark/auto_pipeline_for_host_to_device_data_transfer.py
