:orphan:

.. _transformers_torch_trainer_basic_example :

Fine-tune a Text Classifier with Hugging Face Transformers
==========================================================

.. raw:: html

    <a id="try-anyscale-quickstart-transformers_torch_trainer_basic" target="_blank" href="https://www.anyscale.com/ray-on-anyscale?utm_source=ray_docs&utm_medium=docs&utm_campaign=transformers_torch_trainer_basic">
      <img src="../../../_static/img/run-on-anyscale.svg" alt="Run on Anyscale" />
      <br/><br/>
    </a>

This basic example of distributed training with Ray Train and Hugging Face (HF) Transformers
fine-tunes a text classifier on the Yelp review dataset using HF Transformers and Ray Train.

Code example
------------

.. literalinclude:: /../../python/ray/train/examples/transformers/transformers_torch_trainer_basic.py

See also
--------

* :ref:`Get Started with Hugging Face Transformers <train-pytorch-transformers>` for a tutorial

* :doc:`Ray Train Examples <../../examples>` for more use cases
