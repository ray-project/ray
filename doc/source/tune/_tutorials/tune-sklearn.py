# flake8: noqa
"""
Tune's Scikit Learn Adapters
============================

Scikit-Learn is one of the most widely used tools in the ML community for working with data, offering dozens of easy-to-use machine learning algorithms. However, to achieve high performance for these algorithms, you often need to perform **model selection**.


.. image:: /images/tune-sklearn.png
    :align: center
    :width: 50%

Scikit-Learn `has an existing module for model selection <https://scikit-learn.org/stable/modules/grid_search.html>`_, but the algorithms offered (grid search and random search) are often considered ineffifient. In this tutorial, we'll cover ``tune-sklearn``, a drop-in replacement for Scikit-Learn’s model selection module with state-of-the-art optimization features such as early stopping and Bayesian Optimization.

Check out the repo here: https://github.com/ray-project/tune-sklearn.

Overview
--------

``tune-sklearn`` is a module that integrates Ray Tune's hyperparameter tuning and scikit-learn's Classifier API. It is a drop-in replacement for GridSearchCV and RandomizedSearchCV, so you only need to change less than 5 lines in a standard Scikit-Learn script to use the API.

``tune-sklearn`` allows you to easily leverage Bayesian Optimization, HyperBand, and other cutting edge tuning techniques by simply toggling a few parameters. It also supports and provides examples for many other frameworks with Scikit-Learn wrappers such as Skorch (Pytorch), KerasClassifiers (Keras), and XGBoostClassifiers (XGBoost).

Run ``pip install ray[tune] tune-sklearn`` to get started.

Walkthrough
-----------

``tune-sklearn`` has two APIs: TuneSearchCV, and TuneGridSearchCV. They are drop-in replacements for Scikit-learn's RandomizedSearchCV and GridSearchCV.

For this example, let’s start by using ``TuneGridSearchCV`` with sklearn’s `digits dataset`_ and a `SGDClassifier`_ to classify digits.

.. _`digits dataset`: https://scikit-learn.org/stable/modules/generated/sklearn.datasets.load_digits.html
.. _`SGDClassifier`: https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.SGDClassifier.html

To start out, change the import statement to get tune-scikit-learn’s grid search cross validation interface:

"""
# from sklearn.model_selection import GridSearchCV
from tune_sklearn import TuneGridSearchCV

#######################################################################
# And from there, we would proceed just like how we would in Scikit-Learn’s interface!
#
# The `SGDClassifier`_ has a ``partial_fit`` API, which enables it to stop fitting to the data for a certain hyperparameter configuration. If the estimator does not support early stopping, we would fall back to a parallel grid search.

# Other imports
from sklearn.model_selection import train_test_split
from sklearn.linear_model import SGDClassifier
from sklearn import datasets
import numpy as np

# Load in data
digits = datasets.load_digits()

# Set training and test sets
x = digits.data
y = digits.target
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=.2)

# Example parameters to tune from SGDClassifier
parameter_grid = {"alpha": [1e-4, 1e-1, 1], "epsilon": [0.01, 0.1]}

#######################################################################
# As you can see, the setup here is exactly how you would do it for Scikit-Learn. Now, let's try fitting a model.

tune_search = TuneGridSearchCV(
    SGDClassifier(),
    parameter_grid,
    scheduler="MedianStoppingRule",
    early_stopping=True,
    max_iters=10)

import time  # Just to compare fit times
start = time.time()
tune_search.fit(x_train, y_train)
end = time.time()
print("Tune Fit Time:", end - start)

#######################################################################
# Note the slight differences we introduced above:
#
#  * a `scheduler`, and
#  * a specification of `max_iters` parameter
#
# The ``scheduler`` determines when to stop early - MedianStoppingRule is a great default, but see :ref:`Tune's documentation on schedulers <tune-schedulers>` here for a full list to choose from. ``max_iters`` is the maximum number of iterations a given hyperparameter set could run for; it may run for fewer iterations if it is early stopped.
#
# Try running this compared to the GridSearchCV equivalent, and see the speedup for yourself!

from sklearn.model_selection import GridSearchCV
# n_jobs=-1 enables use of all cores like Tune does
sklearn_search = GridSearchCV(SGDClassifier(), parameter_grid, n_jobs=-1)

start = time.time()
sklearn_search.fit(x_train, y_train)
end = time.time()
print("Sklearn Fit Time:", end - start)

###################################################################
# Using Bayesian Optimization
# ---------------------------
#
# In addition to the grid search interface, tune-sklearn also provides an interface, TuneSearchCV, for sampling from **distributions of hyperparameters**.
#
# In addition, you can easily enable Bayesian optimization over the distributions in only 2 lines of code:

from tune_sklearn.tune_search import TuneSearchCV
from sklearn.linear_model import SGDClassifier
from sklearn import datasets
from sklearn.model_selection import train_test_split
from ray.tune.schedulers import MedianStoppingRule
import numpy as np

digits = datasets.load_digits()
x = digits.data
y = digits.target
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=.2)

clf = SGDClassifier()
parameter_grid = {"alpha": (1e-4, 1), "epsilon": (0.01, 0.1)}

scheduler = MedianStoppingRule(grace_period=10.0)

tune_search = TuneSearchCV(
    clf,
    parameter_grid,
    search_optimization="bayesian",
    n_iter=3,
    scheduler=scheduler,
    max_iters=10,
)
tune_search.fit(x_train, y_train)

################################################################
# As you can see, it’s very simple to integrate tune-sklearn into existing code.
#
# Code Examples
# -------------
#
# Check out more detailed examples and get started with tune-sklearn here and let us know what you think!
#
#  * Skorch with Tune-sklearn: https://github.com/ray-project/tune-sklearn/blob/master/examples/torch_nn.py
#
# Further Reading
# ---------------
#
# Also take a look at Ray’s :ref:`replacement for joblib <ray-joblib>`, which allows users to parallelize scikit learn jobs over multiple nodes.
