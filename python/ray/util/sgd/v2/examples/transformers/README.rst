HuggingFace Transformers Glue Fine-tuning Example
=================================================

We've ported the ``huggingface/transformers/examples/pytorch/text-classification/run_glue_no_trainer.py`` example to
RaySGD. This example enables fine-tuning the library models for sequence classification on the GLUE benchmark: General Language Understanding Evaluation.

This script can fine-tune the following models:
 CoLA, SST-2, MRPC, STS-B, QQP, MNLI, QNLI, RTE, WNLI.

Additional information can be found at the `HuggingFace Repository
<https://github.com/huggingface/transformers/tree/master/examples/pytorch/text-classification>`_.

Single-node CPU training
----------------------------

To run an example tuning MRPC locally using CPU:

.. code-block:: bash

    export TASK_NAME=mrpc

    ray.init()
    python transformers_example.py \
      --model_name_or_path bert-base-cased \
      --task_name $TASK_NAME \
      --max_length 128 \
      --per_device_train_batch_size 32 \
      --learning_rate 2e-5 \
      --num_train_epochs 3 \
      --output_dir /tmp/$TASK_NAME/


Multi-node GPU training
-------------------

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
      --num_workers 8
      --use_gpu