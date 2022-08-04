.. include:: /_includes/train/announcement.rst

.. _train-docs:

Ray Train: Scalable Model Training
==================================

.. _`issue on GitHub`: https://github.com/ray-project/ray/issues

.. tip:: Get in touch with us if you're using or considering using `Ray Train <https://forms.gle/PXFcJmHwszCwQhqX7>`_!

Ray Train is a library for scalable model training, allowing you
to scale up and speed up training for various popular machine learning frameworks.

The main features are:

- **Ease of use**: Scale your single process training code to a cluster in just a couple lines of code.
- **Interactivity**: Ray Train fits in your workflow with support to run from any environment, including seamless Jupyter notebook support.
- **Composability**: Ray Train interoperates with :ref:`Ray Tune <tune-main>` to tune your distributed model and :ref:`Ray Datasets <datasets>` to train on large amounts of data.


Intro to Ray Train
------------------

**Frameworks**: Ray Train is built to abstract away the complexity of scaling up training
for common machine learning frameworks such as XGBoost, Pytorch, and Tensorflow.

There are three broad categories of Trainers that Ray Train offers:

* :ref:`Deep Learning Trainers <train-dl-guide>` (Pytorch, Tensorflow, Horovod)
* :ref:`Tree-based Trainers <train-gbdt-guide>` (XGboost, LightGBM)
* Other ML frameworks (HuggingFace, Scikit-Learn, RLlib)

**Built for data scientists/ML practitioners**: Ray Train has support for standard ML tools and features that practitioners love:

* Callbacks for early stopping
* Checkpointing
* Integration with TensorBoard, Weights/Biases, and MLflow
* Jupyter notebooks

**Integration with Ray Ecosystem**: Ray Train is part of :ref:`Ray AIR <air>` and seamlessly operates
with the rest of the Ray ecosystem.

* Use :ref:`Ray Datasets <datasets>` with Ray Train to handle and train on large amounts of data.
* Use :ref:`Ray Tune <tune-main>` with Ray Train to leverage cutting edge hyperparameter techniques and distribute both your training and tuning.
* Leverage the :ref:`Ray cluster launcher <cluster-cloud>` to launch autoscaling or spot instance clusters to train your model at scale on any cloud.


Quick Start
-----------

.. tabbed:: XGBoost

    .. literalinclude:: doc_code/gbdt_user_guide.py
       :language: python
       :start-after: __xgboost_start__
       :end-before: __xgboost_end__

.. tabbed:: LightGBM

    .. literalinclude:: doc_code/gbdt_user_guide.py
       :language: python
       :start-after: __lightgbm_start__
       :end-before: __lightgbm_end__

.. tabbed:: Pytorch

   .. literalinclude:: /ray-air/doc_code/torch_trainer.py
      :language: python

.. tabbed:: Tensorflow

   .. literalinclude:: /ray-air/doc_code/tf_starter.py
      :language: python
      :start-after: __air_tf_train_start__
      :end-before: __air_tf_train_end__

.. tabbed:: Horovod

   .. literalinclude:: /ray-air/doc_code/hvd_trainer.py
      :language: python


**Next steps:** Check out:

* :ref:`Getting started guide <train-getting-started>`
* :ref:`Key Concepts for Ray Train <train-key-concepts>`
* :ref:`User Guide for Deep Learning trainers <train-dl-guide>`
* :ref:`User Guide for Tree-based trainers <train-gbdt-guide>`

.. include:: /_includes/train/announcement_bottom.rst
