HuggingFace Transformers Glue Fine-tuning Example
=================================================

We've ported the ``huggingface/transformers/examples/run_glue.py`` example to
RaySGD. This example enables fine-tuning the library models for sequence classification on the GLUE benchmark: General Language Understanding Evaluation.

This script can fine-tune the following models: BERT, XLM, XLNet and RoBERTa.

The below information can be found at the `HuggingFace Repository <https://github.com/huggingface/transformers/tree/master/examples#glue-1>`_ and is copied over at your convenience.

Before running any one of these GLUE tasks you should download the
`GLUE data <https://gluebenchmark.com/tasks>`_ by running
`this script <https://gist.github.com/W4ngatang/60c2bdb54d156a41194446737ce03e2e>`_
and unpack it to some directory ``$GLUE_DIR``.

.. code-block:: bash

    export GLUE_DIR=/path/to/glue
    export TASK_NAME=MRPC

    python transformers_example.py \
      --model_type bert \
      --model_name_or_path bert-base-cased \
      --task_name $TASK_NAME \
      --do_train \
      --do_eval \
      --data_dir glue_data/$TASK_NAME \
      --max_seq_length 128 \
      --per_gpu_train_batch_size 32 \
      --learning_rate 2e-5 \
      --num_train_epochs 3.0 \
      --output_dir /tmp/$TASK_NAME/

where task name can be one of CoLA, SST-2, MRPC, STS-B, QQP, MNLI, QNLI, RTE, WNLI.

The dev set results will be present within the text file ``eval_results.txt`` in the specified output_dir.
In case of MNLI, since there are two separate dev sets (matched and mismatched), there will be a separate
output folder called ``/tmp/MNLI-MM/`` in addition to ``/tmp/MNLI/``.

Multi-GPU training with Apex
----------------------------

To run an example tuning MNLI on your local machine with 8 GPUs and apex, first install `apex <https://github.com/NVIDIA/apex>`_, and then run:

.. code-block:: bash

    python transformers_example.py \
        --model_type bert \
        --model_name_or_path bert-base-cased \
        --task_name mnli \
        --do_train \
        --do_eval \
        --data_dir glue_data/MNLI/ \
        --max_seq_length 128 \
        --per_gpu_train_batch_size 8 \
        --learning_rate 2e-5 \
        --num_train_epochs 3.0 \
        --output_dir output_dir \
        --num_workers 8
        --fp16


Multi-node training
-------------------

To run an example tuning MNLI on AWS with 16 GPUs and apex, just run:

.. code-block:: bash

    ray up cluster.yaml
    # Optionally,
    # ray monitor cluster.yaml
    ray submit cluster.yaml transformers_example.py -- --model_type bert \
        --model_name_or_path bert-base-cased \
        --task_name mnli \
        --do_train \
        --do_eval \
        --data_dir /home/ubuntu/glue_data/MNLI/ \
        --max_seq_length 128 \
        --per_gpu_train_batch_size 8 \
        --learning_rate 2e-5 \
        --num_train_epochs 3.0 \
        --output_dir /home/ubuntu/output/ \
        --num_workers 16 \
        --fp16 \
        --address auto

Note that with Apex, you can increase ``per_gpu_train_batch_size`` to 32, which
should make each epoch take 10 minutes or less.

The example can also be run using :ref:`Ray Job Submission <jobs-overview>`, which is in beta starting with Ray 1.12.