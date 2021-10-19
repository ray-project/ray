.. _ray-joblib:

Distributed Scikit-learn / Joblib
=================================

.. _`issue on GitHub`: https://github.com/ray-project/ray/issues

Ray supports running distributed `scikit-learn`_ programs by
implementing a Ray backend for `joblib`_ using `Ray Actors <actors.html>`__
instead of local processes. This makes it easy to scale existing applications
that use scikit-learn from a single node to a cluster.

.. note::

  This API is new and may be revised in future Ray releases. If you encounter
  any bugs, please file an `issue on GitHub`_.

.. _`joblib`: https://joblib.readthedocs.io
.. _`scikit-learn`: https://scikit-learn.org

Quickstart
----------

To get started, first `install Ray <installation.html>`__, then use
``from ray.util.joblib import register_ray`` and run ``register_ray()``.
This will register Ray as a joblib backend for scikit-learn to use.
Then run your original scikit-learn code inside
``with joblib.parallel_backend('ray')``. This will start a local Ray cluster.
See the `Run on a Cluster`_ section below for instructions to run on
a multi-node Ray cluster instead.

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
  with joblib.parallel_backend('ray'):
      search.fit(digits.data, digits.target)

Run on a Cluster
----------------

This section assumes that you have a running Ray cluster. To start a Ray cluster,
please refer to the `cluster setup <cluster/index.html>`__ instructions.

To connect a scikit-learn to a running Ray cluster, you have to specify the address of the
head node by setting the ``RAY_ADDRESS`` environment variable.

You can also start Ray manually by calling ``ray.init()`` (with any of its supported
configuration options) before calling ``with joblib.parallel_backend('ray')``.

.. warning::

    If you do not set the ``RAY_ADDRESS`` environment variable and do not provide
    ``address`` in ``ray.init(address=<address>)`` then scikit-learn will run on a SINGLE node!
