Torch Data Prefetching Benchmark Example
=================================================

We provide a benchmark example to show how the auto pipeline for host to device data transfer speeds up the training on GPUs.
Running the following command gives the actual runtime of a small model training with and without the auto pipeline functionality.
The experiment size can be modified by setting different values for :code:`epochs` and :code:`num_hidden_layers`.

.. code-block:: bash

    python auto_pipeline_for_host_to_device_data_transfer.py \
      --epochs 2 \
      --num_hidden_layers 2


The table below displays the runtime of training epochs (excluding preparation work) under different configurations on a Nvidia 2080 Ti.
The auto pipeline functionality offers more speed improvement when the model size and the number of epochs get larger.
(The actual runtime outputs may vary if these experiments are run locally or different hardware devices are used.)

.. list-table:: Runtime of training epochs (using auto pipeline, not using auto pipeline)
   :widths: 25 25 25 25
   :header-rows: 1

   * - epoch \\ num_hidden_layers
     - 1
     - 4
     - 8
   * - 1
     - (2.52s, 2.69s)
     - (6.85s, 7.21s)
     - (13.05s, 13.54s)
   * - 5
     - (12.14s, 12.88s)
     - (34.33s, 36.48s)
     - (66.38s, 69.12s)
   * - 50
     - (123.12s, 132.88s)
     - (369.42s, 381.67s)
     - (693.52s, 736.17s)