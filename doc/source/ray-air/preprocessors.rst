.. _air-preprocessors:

Using Preprocessors
===================

Data preprocessing is a common technique for transforming raw data into features for a machine learning model.
In general, you may want to apply the same preprocessing logic to your offline training data and online inference data.
Ray AIR provides several common preprocessors out of the box and interfaces to define your own custom logic.

Overview
--------

The most common way of using a preprocessor is by passing it as an argument to the constructor of a :ref:`Trainer <air-trainers>` in conjunction with a :ref:`Ray Dataset <datasets>`.
For example, the following code trains a model with a preprocessor that normalizes the data.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __trainer_start__
    :end-before: __trainer_end__

The  ``Preprocessor`` class with four public methods that can we used separately from a trainer:

#. ``fit()``: Compute state information about a :class:`Dataset <ray.data.Dataset>` (e.g., the mean or standard deviation of a column)
   and save it to the ``Preprocessor``. This information is used to perform ``transform()``, and the method is typically called on a
   training dataset.
#. ``transform()``: Apply a transformation to a ``Dataset``.
   If the ``Preprocessor`` is stateful, then ``fit()`` must be called first. This method is typically called on training,
   validation, and test datasets.
#. ``transform_batch()``: Apply a transformation to a single :class:`batch <ray.train.predictor.DataBatchType>` of data. This method is typically called on online or offline inference data.
#. ``fit_transform()``: Syntactic sugar for calling both ``fit()`` and ``transform()`` on a ``Dataset``.

To show these methods in action, let's walk through a basic example. First, we'll set up two simple Ray ``Dataset``\s.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __preprocessor_setup_start__
    :end-before: __preprocessor_setup_end__

Next, ``fit`` the ``Preprocessor`` on one ``Dataset``, and then ``transform`` both ``Dataset``\s with this fitted information.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __preprocessor_fit_transform_start__
    :end-before: __preprocessor_fit_transform_end__

Finally, call ``transform_batch`` on a single batch of data.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __preprocessor_transform_batch_start__
    :end-before: __preprocessor_transform_batch_end__

Life of an AIR Preprocessor
---------------------------

Now that we've gone over the basics, let's dive into how ``Preprocessor``\s fit into an end-to-end application built with AIR.
The diagram below depicts an overview of the main steps of a ``Preprocessor``:

#. Passed into a ``Trainer`` to ``fit`` and ``transform`` input ``Dataset``\s
#. Saved as a ``Checkpoint``
#. Reconstructed in a ``Predictor`` to ``fit_batch`` on batches of data

.. figure:: images/air-preprocessor.svg

Throughout this section we'll go through this workflow in more detail, with code examples using XGBoost.
The same logic is applicable to other machine learning framework integrations as well.

Trainer
~~~~~~~

The journey of the ``Preprocessor`` starts with the :class:`Trainer <ray.train.trainer.BaseTrainer>`.
If the ``Trainer`` is instantiated with a ``Preprocessor``, then the following logic is executed when ``Trainer.fit()`` is called:

#. If a ``"train"`` ``Dataset`` is passed in, then the ``Preprocessor`` calls ``fit()`` on it.
#. The ``Preprocessor`` then calls ``transform()`` on all ``Dataset``\s, including the ``"train"`` ``Dataset``.
#. The ``Trainer`` then performs training on the preprocessed ``Dataset``\s.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __trainer_start__
    :end-before: __trainer_end__

.. note::

    If you're passing a ``Preprocessor`` that is already fitted, it is refitted on the ``"train"`` ``Dataset``.
    Adding the functionality to support passing in a fitted Preprocessor is being tracked
    `here <https://github.com/ray-project/ray/issues/25299>`__.

.. TODO: Remove the note above once the issue is resolved.

Tune
~~~~

If you're using ``Ray Tune`` for hyperparameter optimization, be aware that each ``Trial`` instantiates its own copy of
the ``Preprocessor`` and the fitting and transforming logic occur once per ``Trial``.

Checkpoint
~~~~~~~~~~

``Trainer.fit()`` returns a ``Result`` object which contains a ``Checkpoint``.
If a ``Preprocessor`` is passed into the ``Trainer``, then it is saved in the ``Checkpoint`` along with any fitted state.

As a sanity check, let's confirm the ``Preprocessor`` is available in the ``Checkpoint``. In practice, you don't need to check.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __checkpoint_start__
    :end-before: __checkpoint_end__


Predictor
~~~~~~~~~

A ``Predictor`` can be constructed from a saved ``Checkpoint``. If the ``Checkpoint`` contains a ``Preprocessor``,
then the ``Preprocessor`` calls ``transform_batch`` on input batches prior to performing inference.

In the following example, we show the Batch Predictor flow. The same logic applies to the :ref:`Online Inference flow <air-key-concepts-online-inference>`.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __predictor_start__
    :end-before: __predictor_end__

Types of Preprocessors
----------------------

Basic Preprocessors
~~~~~~~~~~~~~~~~~~~

Ray AIR provides a handful of ``Preprocessor``\s out of the box, and more will be added over time. We welcome
`Contributions <https://docs.ray.io/en/master/getting-involved.html>`__!

.. tabbed:: Common APIs

    #. :class:`Preprocessor <ray.data.preprocessor.Preprocessor>`
    #. :class:`BatchMapper <ray.data.preprocessors.BatchMapper>`
    #. :class:`Chain <ray.data.preprocessors.Chain>`

.. tabbed:: Tabular

    #. :class:`Categorizer <ray.data.preprocessors.Categorizer>`
    #. :class:`Concatenator <ray.data.preprocessors.Concatenator>`
    #. :class:`FeatureHasher <ray.data.preprocessors.FeatureHasher>`
    #. :class:`LabelEncoder <ray.data.preprocessors.LabelEncoder>`
    #. :class:`MaxAbsScaler <ray.data.preprocessors.MaxAbsScaler>`
    #. :class:`MinMaxScaler <ray.data.preprocessors.MinMaxScaler>`
    #. :class:`Normalizer <ray.data.preprocessors.Normalizer>`
    #. :class:`OneHotEncoder <ray.data.preprocessors.OneHotEncoder>`
    #. :class:`OrdinalEncoder <ray.data.preprocessors.OrdinalEncoder>`
    #. :class:`PowerTransformer <ray.data.preprocessors.PowerTransformer>`
    #. :class:`RobustScaler <ray.data.preprocessors.RobustScaler>`
    #. :class:`SimpleImputer <ray.data.preprocessors.SimpleImputer>`
    #. :class:`StandardScaler <ray.data.preprocessors.StandardScaler>`

.. tabbed:: Text

    #. :class:`CountVectorizer <ray.data.preprocessors.CountVectorizer>`
    #. :class:`HashingVectorizer <ray.data.preprocessors.HashingVectorizer>`
    #. :class:`Tokenizer <ray.data.preprocessors.Tokenizer>`

.. tabbed:: Image

    Coming soon!

.. tabbed:: Utilities

    #. :meth:`Dataset.train_test_split <ray.data.Dataset.train_test_split>`

Chaining Preprocessors
~~~~~~~~~~~~~~~~~~~~~~

More often than not, your preprocessing logic may contain multiple logical steps or apply different transformations to each column.
A simple ``Chain`` ``Preprocessor`` can be used to apply individual ``Preprocessor`` operations sequentially.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __chain_start__
    :end-before: __chain_end__

.. tip::

    Keep in mind that the operations are sequential. For example, if you define a ``Preprocessor``
    ``Chain([preprocessorA, preprocessorB])``, then ``preprocessorB.transform()`` is applied
    to the result of ``preprocessorA.transform()``.

Custom Preprocessors
~~~~~~~~~~~~~~~~~~~~

**Stateless Preprocessors:** Stateless preprocessors can be implemented with the ``BatchMapper``.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __custom_stateless_start__
    :end-before: __custom_stateless_end__

**Stateful Preprocessors:** Stateful preprocessors can be implemented by extending the
:py:class:`~ray.data.preprocessor.Preprocessor` base class.

.. literalinclude:: doc_code/preprocessors.py
    :language: python
    :start-after: __custom_stateful_start__
    :end-before: __custom_stateful_end__
