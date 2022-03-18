HuggingFace Transformers Glue Fine-tuning Example
=================================================

We've ported the ``huggingface/transformers/examples/pytorch/text-classification/run_glue_no_trainer.py`` example to
Ray Train. This example enables fine-tuning the library models for sequence classification on the GLUE benchmark: General Language Understanding Evaluation.

This script can fine-tune the following models:
 CoLA, SST-2, MRPC, STS-B, QQP, MNLI, QNLI, RTE, WNLI.

Additional information can be found at the `HuggingFace Repository
<https://github.com/huggingface/transformers/tree/master/examples/pytorch/text-classification>`_.

Local process training
----------------------

To run an example tuning MRPC locally, without Ray:

.. code-block:: bash

    export TASK_NAME=mrpc

    python transformers_example.py \
      --model_name_or_path bert-base-cased \
      --task_name $TASK_NAME \
      --max_length 128 \
      --per_device_train_batch_size 32 \
      --learning_rate 2e-5 \
      --num_train_epochs 3 \
      --output_dir /tmp/$TASK_NAME/

This is the same as running `run_glue_no_trainer.py <https://github
.com/huggingface/transformers/blob/master/examples/pytorch/text-classification/run_glue_no_trainer.py>`_.

Distributed multi-node GPU training
-----------------------------------

To run an example tuning MRPC on AWS with 8 GPUs across multiple nodes:

.. code-block:: bash

    export TASK_NAME=mrpc

    ray up cluster.yaml
    # (Optional) ray monitor cluster.yaml
    ray submit cluster.yaml transformers_example.py \
      --model_name_or_path bert-base-cased \
      --task_name $TASK_NAME \
      --max_length 128 \
      --per_device_train_batch_size 32 \
      --learning_rate 2e-5 \
      --num_train_epochs 3 \
      --output_dir /tmp/$TASK_NAME/ \
      --address auto \
      --num_workers 8 \
      --use_gpu

The example can also be run using :ref:`Ray Job Submission <jobs-overview>`, which is in beta starting with Ray 1.12.