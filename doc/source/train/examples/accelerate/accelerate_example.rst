:orphan:

.. _accelerate_example:

Distributed Training Example with Hugging Face Accelerate
=========================================================

This example does distributed data parallel training
with Hugging Face (HF) Accelerate, Ray Train, and Ray Data.
It fine-tunes a BERT model and is adapted from
https://github.com/huggingface/accelerate/blob/main/examples/nlp_example.py


Code example
------------

.. literalinclude:: /../../python/ray/train/examples/accelerate/accelerate_torch_trainer.py

See also
--------

For a tutorial on using Ray Train and HF Accelerate, 
see :ref:`Training with Hugging Face Accelerate <train-hf-accelerate>`.

For more Train examples, see :ref:`Ray Train Examples <train-examples>`.
