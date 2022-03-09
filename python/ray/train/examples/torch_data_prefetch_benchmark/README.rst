Torch Data Prefetching Benchmark Example
=================================================

We provide a benchmark example to show how the auto pipeline for host to device data transfer speeds up the training on GPUs.
Running the following command gives the actual runtime of a small model training with and without the auto pipeline functionality.
You modify the experiment size by setting different values for `epochs` and `num_hidden_layers`.
The table below displays the actual runtime of training epochs (excluding preparation work) under different configurations on a Nvidia 2080 Ti.

| epoch \ num_hidden_layers  | 1 | 4 | 8 |
| :---        |    :----:   |  :----: | :----: |
| 1     | (2.44s, 2.61s)  | (2.47s, 7.42s)  | (2.52s, 13.32s) |
| 5     | (12.20s, 12.76s)       | (11.99s, 36.94s)  | (12.27s, 67.42s) |
| 10   | (24.01, 25.77s)       | (24.62s, 73.10s)  | (23.88s, 139.00s) |

.. code-block:: bash

    python auto_pipeline_for_host_to_device_data_transfer.py \
      --epochs 2 \
      --num_hidden_layers 2
