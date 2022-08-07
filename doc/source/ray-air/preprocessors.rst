.. _air-preprocessors:

Using Preprocessors
===================

Data preprocessing is a common technique for transforming raw data into features for a machine learning model.
In general, you may want to apply the same preprocessing logic to your offline training data and online inference data.
Ray AIR provides several common preprocessors out of the box and interfaces to define your own custom logic.

Overview
--------

Ray AIR exposes a ``Preprocessor`` class with four public methods for data preprocessing:

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

Built-in Preprocessors
~~~~~~~~~~~~~~~~~~~

Ray AIR provides a handful of ``Preprocessor``\s out of the box, and more will be added over time. We welcome
`Contributions <https://docs.ray.io/en/master/getting-involved.html>`__!

.. tabbed:: Generic

    .. autosummary::

        ray.data.preprocessor.Preprocessor
        ray.data.preprocessors.BatchMapper
        ray.data.preprocessors.Chain
        ray.data.preprocessors.SimpleImputer

.. tabbed:: Categorical

    .. autosummary::

        ray.data.preprocessors.Categorizer
        ray.data.preprocessors.OneHotEncoder
        ray.data.preprocessors.MultiHotEncoder
        ray.data.preprocessors.OrdinalEncoder
        ray.data.preprocessors.LabelEncoder

.. tabbed:: Numeric

    .. autosummary::

        ray.data.preprocessors.Concatenator
        ray.data.preprocessors.MaxAbsScaler
        ray.data.preprocessors.MinMaxScaler
        ray.data.preprocessors.Normalizer
        ray.data.preprocessors.PowerTransformer
        ray.data.preprocessors.RobustScaler
        ray.data.preprocessors.StandardScaler

.. tabbed:: Text

    .. autosummary::

        ray.data.preprocessors.CountVectorizer
        ray.data.preprocessors.HashingVectorizer
        ray.data.preprocessors.Tokenizer
        ray.data.preprocessors.FeatureHasher

.. tabbed:: Utilities

    .. autosummary::

        ray.data.Dataset.train_test_split

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

.. _air-custom-preprocessors:

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


Which Preprocessors Should I Use?
---------------------------------

* If you have categorical data, `encode categories <air-encoding-categorical-data>`_.
* If you have numerical data, `normalize features <air-scaling-numerical-data>`_.
* If your model expects a tensor or ``ndarray``, create a tensor using
  :class:`~ray.data.preprocessors.Concatenator`.
* If your preprocess isn't supported, `implement a custom preprocessor <air-custom-preprocessors>`_.
* If want to count the frequency of terms in a document,
  `create a document-term matrix <air-encoding-documents>`_.
* If you need to apply more than one preprocessor, compose them together with
  :class:`~ray.data.preprocessors.Chain`.
* If your data contains missing values, impute them with
  :class:`~ray.data.preprocessors.SimpleImputer`.

.. _air-encoding-categorical-data:

Encoding categorical data
~~~~~~~~~~~~~~~~~~~~~~~~~

Most models expect numerical inputs. To represent your categorical data in a way your
model can understand, encode categories using one of the preprocessors described below.

.. list-table::
   :header-rows: 1

   * - Categorical Data Type
     - Example
     - Preprocessor
   * - Labels
     - ``"cat"``, ``"dog"``, ``"airplane"``
     - :class:`~ray.data.preprocessors.LabelEncoder`
   * - Ordered categories
     - ``"bs"``, ``"md"``, ``"phd"``
     - :class:`~ray.data.preprocessors.OrdinalEncoder`
   * - Unordered categories
     - ``"red"``, ``"green"``, ``"blue"``
     - :class:`~ray.data.preprocessors.OneHotEncoder`
   * - Lists of categories
     - ``("sci-fi", "action")``, ``("action", "comedy", "animated")``
     - :class:`~ray.data.preprocessors.MultiHotEncoder`

.. note::
    If you're using LightGBM, you don't need to encode your categorical data. Instead,
    use :class:`~ray.data.preprocessors.Categorizer` to convert your data to
    `pandas.CategoricalDtype`.

.. _air-scaling-numerical-data:

Scaling numerical data
~~~~~~~~~~~~~~~~~~~~~~

To ensure your models behaves properly,
normalize your numerical data. Reference the table below to determine which preprocessor
to use.

.. list-table::
   :header-rows: 1

   * - Data Property
     - Preprocessor
   * - Your data is approximately normal
     - :class:`~ray.data.preprocessors.StandardScaler`
   * - Your data is sparse
     - :class:`~ray.data.preprocessors.MaxAbsScaler`
   * - Your data contains many outliers
     - :class:`~ray.data.preprocessors.RobustScaler`
   * - Your data isn't normal, but you need it to be
     - :class:`~ray.data.preprocessors.PowerTransformer`
   * - You need unit-norm rows
     - :class:`~ray.data.preprocessors.Normalizer`
   * - You aren't sure what your data looks like
     - :class:`~ray.data.preprocessors.MinMaxScaler`

.. warning::
    These preprocessors operate on numeric columns. If your dataset contains columns of
    type :class:`~ray.air.util.tensor_extensions.pandas.TensorDtype`, you may need to
    :ref:`implement a custom preprocessor <air-custom-preprocessors>`.

.. _air-encoding-text:

Encoding documents
~~~~~~~~~~~~~~~~~~

A `document-term matrix <https://en.wikipedia.org/wiki/Document-term_matrix>`_ is a
table that describes text data. It's useful for natural language processing.

To generate a document-term matrix
from a collection of documents, use :class:`~ray.data.preprocessors.HashingVectorizer`
or :class:`~ray.data.preprocessors.CountVectorizer`. If already know the frequency of
tokens and want to store the data in a document-term matrix, use
:class:`~ray.data.preprocessors.FeatureHasher`.


.. list-table::
   :header-rows: 1

   * - Requirement
     - Preprocessor
   * - You care about memory efficiency
     - :class:`~ray.data.preprocessors.HashingVectorizer`
   * - You want to know which features are important to your model
     - :class:`~ray.data.preprocessors.CountVectorizer`
