.. include:: /_includes/train/announcement.rst

.. _train-docs:

Ray Train: Scalable Model Training
==================================

.. _`issue on GitHub`: https://github.com/ray-project/ray/issues

.. tip::

    Train is currently in **beta**. Fill out `this short form <https://forms.gle/PXFcJmHwszCwQhqX7>`_ to get involved with Train development!

Ray Train scales model training for popular ML frameworks such as Torch, XGBoost, TensorFlow, and more. It seamlessly integrates with other Ray libraries such as Tune and Predictors:

.. https://docs.google.com/drawings/d/1FezcdrXJuxLZzo6Rjz1CHyJzseH8nPFZp6IUepdn3N4/edit

.. image:: images/train-specific.svg

Intro to Ray Train
------------------

**Framework support**: Train abstracts away the complexity of scaling up training
for common machine learning frameworks such as XGBoost, Pytorch, and Tensorflow.
There are three broad categories of Trainers that Train offers:

* Deep Learning Trainers (:doc:`PyTorch </train/distributed-pytorch>`, :doc:`TensorFlow </train/distributed-tensorflow-keras>`, :doc:`Horovod </train/horovod>`)
* :doc:`Tree-based Trainers </train/distributed-xgboost-lightgbm>` (XGboost, LightGBM)
* Other ML frameworks (HuggingFace, Scikit-Learn, RLlib)

**Built for ML practitioners**: Train supports standard ML tools and features that practitioners love:

* Callbacks for early stopping
* Checkpointing
* Integration with TensorBoard, Weights/Biases, and MLflow
* Jupyter notebooks

**Batteries included**: Train seamlessly operates in the Ray ecosystem.

* Use :ref:`Ray Data <data>` with Train to load and process datasets both small and large.
* Use :ref:`Ray Tune <tune-main>` with Train to sweep parameter grids and leverage cutting edge hyperparameter search algorithms.
* Leverage the :ref:`Ray cluster launcher <cluster-index>` to launch autoscaling or spot instance clusters on any cloud.


Next steps
----------

* :ref:`Key Concepts for Ray Train <train-key-concepts>`
* :doc:`User Guide for distributed PyTorch </train/distributed-pytorch>`
* :doc:`User Guide for distributed TensorFlow </train/distributed-tensorflow-keras>`
* :doc:`User Guide for Tree-Based Trainers </train/distributed-xgboost-lightgbm>`

.. include:: /_includes/train/announcement_bottom.rst
