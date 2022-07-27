.. _ray-joblib:

Distributed Scikit-learn / Joblib
=================================

.. _`issue on GitHub`: https://github.com/ray-project/ray/issues

Ray supports running distributed `scikit-learn`_ programs by
implementing a Ray backend for `joblib`_ using `Ray Actors <../ray-core/actors.html>`__
instead of local processes. This makes it easy to distribute existing application process that use scikit-learn from a single node to a cluster.

.. note::

  This API is new and may be revised in future Ray releases. If you encounter
  any bugs, please file an `issue on GitHub`_.

.. _`joblib`: https://joblib.readthedocs.io
.. _`scikit-learn`: https://scikit-learn.org

Quickstart
----------

`Install Ray <../ray-overview/installation.html>`__ and call some methods in your scikit-learn code as the following.

.. code-block:: python

  import joblib
  from ray.util.joblib import register_ray
  register_ray()
  with joblib.parallel_backend('ray'):
      # scikit-learn code

Usage
-----

``from ray.util.joblib import register_ray`` imports joblib with Ray backend library.
``register_ray()`` registers Ray as a joblib backend for scikit-learn to use.

The program inside ``with joblib.parallel_backend()`` is executed on the backend used by Parallel, and 
the program inside ``with joblib.parallel_backend('ray')`` is executed on Ray as the backend used by Parallel.
Then, ``with joblib.parallel_backend('ray')`` deploys your code on a Ray cluster. Inside the call, run your original scikit-learn code.

.. code-block:: python

  import numpy as np
  from sklearn.datasets import load_digits
  from sklearn.model_selection import RandomizedSearchCV
  from sklearn.svm import SVC
  digits = load_digits()
  param_space = {
      'C': np.logspace(-6, 6, 30),
      'gamma': np.logspace(-8, 8, 30),
      'tol': np.logspace(-4, -1, 30),
      'class_weight': [None, 'balanced'],
  }
  model = SVC(kernel='rbf')
  search = RandomizedSearchCV(model, param_space, cv=5, n_iter=300, verbose=10)

  import joblib
  from ray.util.joblib import register_ray
  register_ray()
  # ray.init(address=<address>) # if necessary
  with joblib.parallel_backend('ray'):
      search.fit(digits.data, digits.target)

This will start a local Ray cluster. 
To execute on a remote Ray cluster, start a Ray cluster and define a ``RAY_ADDRESS`` environment variable.
Please refer to the `cluster setup <../cluster/index.html>`__ instructions.
You can also start Ray manually by calling ``ray.init()`` (with any of its supported configuration options) before calling ``with joblib.parallel_backend('ray')``.

.. warning::

    If both ``RAY_ADDRESS`` environment variable and
    ``address`` in ``ray.init(address=<address>)`` are not defined, scikit-learn will run on a SINGLE node!

You can also set the ``ray_remote_args`` argument in ``parallel_backend`` to :ref:`configure
the Ray Actors <ray-remote-ref>` making up the Pool. This can be used to eg. :ref:`assign resources
to Actors, such as GPUs <actor-resource-guide>`.

.. code-block:: python

  # Allows to use GPU-enabled estimators, such as cuML
  with joblib.parallel_backend('ray', ray_remote_args=dict(num_gpus=1)):
      search.fit(digits.data, digits.target)
