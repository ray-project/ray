.. _tune-xgboost:

Tuning XGBoost parameters
=========================

XGBoost is currently one of the most popular machine learning algorithms. It performs
very well on a large selection of tasks, and was the key to success in many Kaggle
competitions.

.. image:: /images/xgboost_logo.png
  :width: 200px
  :alt: XGBoost
  :align: center
  :target: https://xgboost.readthedocs.io/en/latest/


This tutorial will give you a quick introduction to XGBoost, show you how
to train an XGBoost model, and then guide you on how to optimize XGBoost
parameters using Tune to get the best performance. We tackle the following topics:

.. contents:: Table of contents
   :depth: 2

.. note::

    To run this tutorial, you will need to install the following:

    .. code-block:: bash

        $ pip install xgboost

What is XGBoost
---------------

XGBoost is an acronym for e\ **X**\ treme **G**\ radient **Boost**\ ing. Internally,
XGBoost uses `decision trees <https://en.wikipedia.org/wiki/Decision_tree>`_. Instead
of training just one large decision tree, XGBoost and other related algorithms train
many small decision trees. The intuition behind this is that even though single
decision trees can be inaccurate and suffer from high variance,
combining the output of a large number of these weak learners can actually lead to
strong learner, resulting in better predictions and less variance.

.. figure:: /images/tune-xgboost-ensemble.svg
  :alt: Single vs. ensemble learning

  A single decision tree (left) might be able to get to an accuracy of 70%
  for a binary classification task. By combining the output of several small
  decision trees, an ensemble learner (right) might end up with a higher accuracy
  of 90%.

Boosting algorithms start with a single small decision tree and evaluate how well
it predicts the given examples. When building the next tree, those samples that have
been misclassified before have a higher chance of being used to generate the tree.
This is useful because it avoids overfitting to samples that can be easily classified
and instead tries to come up with models that are able to classify hard examples, too.
Please see `here for a more thorough introduction to bagging and boosting algorithms
<https://towardsdatascience.com/ensemble-methods-bagging-boosting-and-stacking-c9214a10a205>`_.

There are many boosting algorithms. In their core, they are all very similar. XGBoost
uses second-level derivatives to find splits that maximize the *gain* (the inverse of
the *loss*) - hence the name. In practice, there really is no drawback in using
XGBoost over other boosting algorithms - in fact, it usually shows the best performance.

Training a simple XGBoost classifier
------------------------------------

Let's first see how a simple XGBoost classifier can be trained. We'll use the
``breast_cancer``-Dataset included in the ``sklearn`` dataset collection. This is
a binary classification dataset. Given 30 different input features, our task is to
learn to identify subjects with breast cancer and those without.

Here is the full code to train a simple XGBoost model:

.. code-block:: python

    import numpy as np
    import sklearn.datasets
    import sklearn.metrics
    from sklearn.model_selection import train_test_split
    import xgboost as xgb


    def train_breast_cancer(config):
        # Load dataset
        data, labels = sklearn.datasets.load_breast_cancer(return_X_y=True)
        # Split into train and test set
        train_x, test_x, train_y, test_y = train_test_split(
            data, labels, test_size=0.25)
        # Build input matrices for XGBoost
        train_set = xgb.DMatrix(train_x, label=train_y)
        test_set = xgb.DMatrix(test_x, label=test_y)
        # Train the classifier
        bst = xgb.train(config, train_set, evals=[(test_set, "eval")], verbose_eval=False)
        # Predict labels for the test set
        preds = bst.predict(test_set)
        pred_labels = np.rint(preds)
        # Return prediction accuracy
        accuracy = sklearn.metrics.accuracy_score(test_y, pred_labels)
        return accuracy


    if __name__ == "__main__":
        accuracy = train_breast_cancer({
            "objective": "binary:logistic"
        })
        print("Accuracy: {:.2f}".format(accuracy))

As you can see, the code is quite simple. First, the dataset is loaded and split
into a ``test`` and ``train`` set. The XGBoost model is trained with ``xgb.train()``
and the predictions for the test set are obtained with ``bst.predict()``. Lastly, we
return the accuracy of our predictions. Even in this simple example, most runs result
in a good accuracy of over ``0.90``.

Maybe you have noticed the ``config`` parameter we pass to the XGBoost algorithm. This
is a ``dict`` in which you can specify parameters for the XGBoost algorithm. In this
simple example, the only parameter we passed is the ``objective`` parameter. The value
``binary:logistic`` tells XGBoost that we aim to train a logistic regression model for
a binary classification task. You can find an overview over all valid objectives
`here in the XGBoost documentation <https://xgboost.readthedocs.io/en/latest/parameter.html#learning-task-parameters>`_.

XGBoost Hyperparameters
-----------------------
Even with the default settings, XGBoost was able to get to a good accuracy on the
breast cancer dataset. However, as in many machine learning algorithms, there are
many knobs to tune which might lead to even better performance. Let's explore some of
them below.

Maximum tree depth
..................
Remember that XGBoost internally uses many decision tree models to come up with
predictions. When training a decision tree, we need to tell the algorithm how
large the tree may get. The parameter for this is called the tree *depth*.

.. figure:: /images/tune-xgboost-depth.svg
  :alt: Decision tree depth
  :align: center

  In this image, the left tree has a depth of 2, and the right tree a depth of 3.
  Note that with each level, :math:`2^{(d-1)}` splits are added, where *d* is the depth
  of the tree.

Tree depth is a property that concerns the model complexity. If you only allow short
trees, the models are likely not very precise - they underfit the data. If you allow
very large trees, the single models are likely to overfit to the data. In practice,
a number between ``2`` and ``6`` is often a good starting point for this parameter.

XGBoost's default value is ``3``.

Minimum child weight
....................
When a decision tree creates new leaves, it splits up the remaining data at one node
into two groups. If there are only few samples in one of these groups, it often
doesn't make sense to split it further. One of the reasons for this is that the
model is harder to train when we have fewer samples.

.. figure:: /images/tune-xgboost-weight.svg
  :alt: Minimum child weight
  :align: center

  In this example, we start with 100 examples. At the first node, they are split
  into 4 and 96 samples, respectively. In the next step, our model might find
  that it doesn't make sense to split the 4 examples more. It thus only continues
  to add leaves on the right side.

The parameter used by the model to decide if it makes sense to split a node is called
the *minimum child weight*. In the case of linear regression, this is just the absolute
number of nodes requried in each child. In other objectives, this value is determined
using the weights of the examples, hence the name.

The larger the value, the more constrained the trees are and the less deep they will be.
This parameter thus also affects the model complexity. Values can range between 0
and infinity and are dependent on the sample size. For our ca. 500 examples in the
breast cancer dataset, values between ``0`` and ``10`` should be sensible.

XGBoost's default value is ``1``.

Subsample size
..............
Each decision tree we add is trained on a subsample of the total training dataset.
The probabilities for the samples are weighted according to the XGBoost algorithm,
but we can decide on which fraction of the samples we want to train each decision
tree on.

Setting this value to ``0.7`` would mean that we randomly sample ``70%`` of the
training dataset before each training iteration.

XGBoost's default value is ``1``.

Learning rate / Eta
...................
Remember that XGBoost sequentially trains many decision trees, and that later trees
are more likely trained on data that has been misclassified by prior trees. In effect
this means that earlier trees make decisions for easy samples (i.e. those samples that
can easily be classified) and later trees make decisions for harder samples. It is then
sensible to assume that the later trees are less accurate than earlier trees.

To address this fact, XGBoost uses a parameter called *Eta*, which is sometimes called
the *learning rate*. Don't confuse this with learning rates from gradient descent!
The original `paper on stochastic gradient boosting <https://www.sciencedirect.com/science/article/abs/pii/S0167947301000652>`_
introduces this parameter like so:

.. math::
    F_m(x) = F_{m-1}(x) + \eta \cdot \gamma_{lm} \textbf{1}(x \in R_{lm})

This is just a complicated way to say that when we train we new decision tree,
represented by :math:`\gamma_{lm} \textbf{1}(x \in R_{lm})`, we want to dampen
its effect on the previous prediction :math:`F_{m-1}(x)` with a factor
:math:`\eta`.

Typical values for this parameter are between ``0.01`` and ``0.3```.

XGBoost's default value is ``0.3``.

Number of boost rounds
......................
Lastly, we can decide on how many boosting rounds we perform, which means how
many decision trees we ultimately train. When we do heavy subsampling or use small
learning rate, it might make sense to increase the number of boosting rounds.

XGBoost's default value is ``10``.

Putting it together
...................
Let's see how this looks like in code! We just need to adjust our ``config`` dict:

.. code-block:: python

    if __name__ == "__main__":
        config = {
            "objective": "binary:logistic",
            "max_depth": 2,
            "min_child_weight": 0,
            "subsample": 0.8,
            "eta": 0.2
        }
        accuracy = train_breast_cancer(config)
        print("Accuracy: {:.2f}".format(accuracy))

The rest stays the same. Please note that we do not adjust the ``num_boost_rounds`` here.
The result should also show a high accuracy of over 90%.

Tuning the configuration parameters
-----------------------------------
XGBoosts default parameters already lead to a good accuracy, and even our guesses in the
last section should result in accuracies well above 90%. However, our guesses were
just that: guesses. Often we do not know what combination of parameters would actually
lead to the best results on a machine learning task.

Unfortunately, there are infinitely many combinations of hyperparameters we could try
out. Should we combine ``max_depth=3`` with ``subsample=0.8`` or with ``subsample=0.9``?
What about the other parameters?

This is where hyperparameter tuning comes into play. By using tuning libraries such as
Ray Tune we can try out combinations of hyperparameters. Using sophisticated search
strategies, these parameters can be selected so that they are likely to lead to good
results (avoiding an expensive *exhaustive search*). Also, trials that do not perform
well can be preemptively stopped to reduce waste of computing resources. Lastly, Ray Tune
also takes care of training these runs in parallel, greatly increasing search speed.

Let's start with a basic example on how to use Tune for this. We just need to make
a few changes to our code-block:

.. code-block:: python
   :emphasize-lines: 26,32,33,34,35,37,38,39,40,41

    import numpy as np
    import sklearn.datasets
    import sklearn.metrics
    from sklearn.model_selection import train_test_split
    import xgboost as xgb

    from ray import tune


    def train_breast_cancer(config):
        # Load dataset
        data, labels = sklearn.datasets.load_breast_cancer(return_X_y=True)
        # Split into train and test set
        train_x, test_x, train_y, test_y = train_test_split(
            data, labels, test_size=0.25)
        # Build input matrices for XGBoost
        train_set = xgb.DMatrix(train_x, label=train_y)
        test_set = xgb.DMatrix(test_x, label=test_y)
        # Train the classifier
        bst = xgb.train(config, train_set, evals=[(test_set, "eval")], verbose_eval=False)
        # Predict labels for the test set
        preds = bst.predict(test_set)
        pred_labels = np.rint(preds)
        # Return prediction accuracy
        accuracy = sklearn.metrics.accuracy_score(test_y, pred_labels)
        tune.report(mean_accuracy=accuracy, done=True)


    if __name__ == "__main__":
        config = {
            "objective": "binary:logistic",
            "max_depth": tune.randint(1, 9),
            "min_child_weight": tune.choice([1, 2, 3]),
            "subsample": tune.uniform(0.5, 1.0),
            "eta": tune.loguniform(1e-4, 1e-1)
        }
        tune.run(
            train_breast_cancer,
            resources_per_trial={"cpu": 1},
            config=config,
            num_samples=10)

As you can see, the changes in the actual training function are minimal. Instead of
returning the accuracy value, we report it back to Tune using ``tune.report()``.
Our ``config`` dictionary only changed slightly. Instead of passing hard-coded
parameters, we tell Tune to choose values from a range of valid options. There are
a number of options we have here, all of which are explained in
:ref:`the Tune docs <tune-sample-docs>`.

For a brief explanation, this is what they do:

* ``tune.randint(min, max)`` chooses a random integer value between *min* and *max*.
  Note that *max* is exclusive, so it will not be sampled.
* ``tune.choice([a, b, c])`` chooses one of the items of the list at random. Each item
  has the same chance to be sampled.
* ``tune.uniform(min, max)`` samples a floating point number between *min* and *max*.
  Note that *max* is exclusive here, too.
* ``tune.loguniform(min, max, base=10)`` samples a floating point number between *min* and *max*,
  but applies a logarithmic transformation to these boundaries first. Thus, this makes
  it easy to sample values from different orders of magnitude.



The ``num_samples=10`` option we pass to ``tune.run()`` means that we sample 10 different
hyperparameter configurations from this search space.

The output of our training run coud look like this:

.. code-block::
   :emphasize-lines: 10

    +---------------------------------+------------+-------+-------------+-------------+--------------------+-------------+----------+--------+------------------+
    | Trial name                      | status     | loc   |         eta |   max_depth |   min_child_weight |   subsample |      acc |   iter |   total time (s) |
    |---------------------------------+------------+-------+-------------+-------------+--------------------+-------------+----------+--------+------------------|
    | train_breast_cancer_c817a_00000 | TERMINATED |       | 0.00334038  |           8 |                  1 |    0.640256 | 0.93007  |      1 |        0.050081  |
    | train_breast_cancer_c817a_00001 | TERMINATED |       | 0.00285335  |           4 |                  3 |    0.951621 | 0.93007  |      1 |        0.0453899 |
    | train_breast_cancer_c817a_00002 | TERMINATED |       | 0.0597631   |           5 |                  2 |    0.96479  | 0.986014 |      1 |        0.0503612 |
    | train_breast_cancer_c817a_00003 | TERMINATED |       | 0.000650095 |           6 |                  2 |    0.923812 | 0.951049 |      1 |        0.0588872 |
    | train_breast_cancer_c817a_00004 | TERMINATED |       | 0.00753275  |           1 |                  1 |    0.973499 | 0.881119 |      1 |        0.0347321 |
    | train_breast_cancer_c817a_00005 | TERMINATED |       | 0.000411214 |           5 |                  1 |    0.672503 | 0.958042 |      1 |        0.0477931 |
    | train_breast_cancer_c817a_00006 | TERMINATED |       | 0.0940201   |           5 |                  2 |    0.711124 | 0.972028 |      1 |        0.069901  |
    | train_breast_cancer_c817a_00007 | TERMINATED |       | 0.0372492   |           1 |                  1 |    0.76303  | 0.895105 |      1 |        0.0496318 |
    | train_breast_cancer_c817a_00008 | TERMINATED |       | 0.000140322 |           1 |                  2 |    0.885415 | 0.909091 |      1 |        0.045424  |
    | train_breast_cancer_c817a_00009 | TERMINATED |       | 0.000341654 |           5 |                  3 |    0.720523 | 0.937063 |      1 |        0.0657773 |
    +---------------------------------+------------+-------+-------------+-------------+--------------------+-------------+----------+--------+------------------+

The best configuration we found used ``eta=0.0940201``, ``max_depth=5``,
``min_child_weight=2``, ``subsample=0.711124`` and reached an accuracy of
``0.972028``.

Early stopping
--------------
Currently, Tune samples 10 different hyperparameter configurations and trains a full
XGBoost on all of them. In our small example, training is very fast. However,
if training takes longer, a significant amount of computer resources is spent on trials
that will eventually show a bad performance, e.g. a low accuracy. It would be good
if we could identify these trials early and stop them, so we don't waste any resources.

This is where Tune's *Schedulers* shine. A Tune ``TrialScheduler`` is responsible
for starting and stopping trials. Tune implements a number of different schedulers, each
described :ref:`in the Tune documentation <tune-schedulers>`.
For our example, we will use the ``AsyncHyperBandScheduler`` or ``ASHAScheduler``.

The basic idea of this scheduler: We sample a number of hyperparameter configurations.
Each of these configurations is trained for a specific number of iterations.
After these iterations, only the best performing hyperparameters are retained. These
are selected according to some loss metric, usually an evaluation loss. This cycle is
repeated until we end up with the best configuration.

The ``ASHAScheduler`` needs to know three things:

1. Which metric should be used to identify badly performing trials?
2. Should this metric be maximized or minimized?
3. How many iterations does each trial train for?

There are more parameters, which are explained in the
:ref:`documentation <tune-scheduler-hyperband>`.

Lastly, we have to report the loss metric to Tune. We do this with a ``Callback`` that
XGBoost accepts and calls after each training iteration. We also tell XGBoost which
loss metrics to calculate in the ``eval_metric`` parameter. These are the metrics
available in ``env.evaluation_result_list`` below.

.. code-block:: python
   :emphasize-lines: 11,12,13,26,42,44,45,46,47,48,49

    import numpy as np
    import sklearn.datasets
    import sklearn.metrics
    from ray.tune.schedulers import ASHAScheduler
    from sklearn.model_selection import train_test_split
    import xgboost as xgb

    from ray import tune


    def XGBCallback(env):
        # After every training iteration, report loss to Tune
        tune.report(**dict(env.evaluation_result_list))


    def train_breast_cancer(config):
        # Load dataset
        data, labels = sklearn.datasets.load_breast_cancer(return_X_y=True)
        # Split into train and test set
        train_x, test_x, train_y, test_y = train_test_split(
            data, labels, test_size=0.25)
        # Build input matrices for XGBoost
        train_set = xgb.DMatrix(train_x, label=train_y)
        test_set = xgb.DMatrix(test_x, label=test_y)
        # Train the classifier
        bst = xgb.train(config, train_set, evals=[(test_set, "eval")], verbose_eval=False, callbacks=[XGBCallback])
        # Predict labels for the test set
        preds = bst.predict(test_set)
        pred_labels = np.rint(preds)
        # Return prediction accuracy
        accuracy = sklearn.metrics.accuracy_score(test_y, pred_labels)
        tune.report(mean_accuracy=accuracy, done=True)


    if __name__ == "__main__":
        config = {
            "objective": "binary:logistic",
            "max_depth": tune.randint(1, 9),
            "min_child_weight": tune.choice([1, 2, 3]),
            "subsample": tune.uniform(0.5, 1.0),
            "eta": tune.loguniform(1e-4, 1e-1),
            "eval_metric": ["auc", "ams@0", "logloss"]
        }
        scheduler = ASHAScheduler(
            metric="eval-logloss",  # The `eval` prefix is defined in xgb.train
            mode="min",  # Retain configurations with a low logloss
            max_t=11,  # 10 training iterations + 1 final evaluation
            grace_period=1,  # Number of minimum iterations for each trial
            reduction_factor=2)  # How aggressively to stop trials
        tune.run(
            train_breast_cancer,
            resources_per_trial={"cpu": 1},
            config=config,
            num_samples=10,
            scheduler=scheduler)

The output of our run could look like this:

.. code-block::
   :emphasize-lines: 13

    +---------------------------------+------------+-------+-------------+-------------+--------------------+-------------+----------+--------+------------------+
    | Trial name                      | status     | loc   |         eta |   max_depth |   min_child_weight |   subsample |      acc |   iter |   total time (s) |
    |---------------------------------+------------+-------+-------------+-------------+--------------------+-------------+----------+--------+------------------|
    | train_breast_cancer_806ea_00000 | TERMINATED |       | 0.0371055   |           2 |                  1 |    0.611729 | 0.951049 |     11 |        0.339279  |
    | train_breast_cancer_806ea_00001 | TERMINATED |       | 0.0324613   |           3 |                  2 |    0.643815 |          |      4 |        0.230338  |
    | train_breast_cancer_806ea_00002 | TERMINATED |       | 0.0100875   |           4 |                  3 |    0.985147 |          |      2 |        0.0661929 |
    | train_breast_cancer_806ea_00003 | TERMINATED |       | 0.00124263  |           1 |                  3 |    0.890299 |          |      1 |        0.0201721 |
    | train_breast_cancer_806ea_00004 | TERMINATED |       | 0.000230373 |           5 |                  3 |    0.627611 |          |      1 |        0.0265107 |
    | train_breast_cancer_806ea_00005 | TERMINATED |       | 0.000186942 |           5 |                  2 |    0.831801 |          |      1 |        0.026082  |
    | train_breast_cancer_806ea_00006 | TERMINATED |       | 0.00871051  |           2 |                  3 |    0.721523 | 0.958042 |     11 |        0.299392  |
    | train_breast_cancer_806ea_00007 | TERMINATED |       | 0.00440949  |           2 |                  3 |    0.606252 |          |      1 |        0.0210171 |
    | train_breast_cancer_806ea_00008 | TERMINATED |       | 0.00948289  |           5 |                  2 |    0.892979 |          |      2 |        0.140424  |
    | train_breast_cancer_806ea_00009 | TERMINATED |       | 0.0514017   |           2 |                  1 |    0.859864 | 0.972028 |     11 |        0.365437  |
    +---------------------------------+------------+-------+-------------+-------------+--------------------+-------------+----------+--------+------------------+

As you can see, four trials have been stopped after just one iteration, two after two iterations,
one after four iterations, and the three most promising configurations have been run for
ten iterations. The 11 is due to the fact that we finally report the accuracy after
training the full model, which is internally interpreted as another iteration.

Using fractional GPUs
---------------------
You can often accelerate your training by using GPUs in addition to CPUs. However,
you usually don't have as many GPUs as you have trials to run. For instance, if you
run 10 Tune trials in parallel, you usually don't have access to 10 separate GPUs.

Tune supports *fractional GPUs*. This means that each task is assigned a fraction
of the GPU memory for training. For 10 tasks, this could look like this:

.. code-block:: python
   :emphasize-lines: 8,12

    config = {
        "objective": "binary:logistic",
        "max_depth": tune.randint(1, 9),
        "min_child_weight": tune.choice([1, 2, 3]),
        "subsample": tune.uniform(0.5, 1.0),
        "eta": tune.loguniform(1e-4, 1e-1),
        "eval_metric": ["auc", "ams@0", "logloss"],
        "tree_method": "gpu_hist"
    }
    tune.run(
        train_breast_cancer,
        resources_per_trial={"cpu": 1, "gpu": 0.1},
        config=config,
        num_samples=10,
        scheduler=scheduler)

Each task thus works with 10% of the available GPU memory. You also have to tell
XGBoost to use the ``gpu_hist`` tree method, so it knows it should use the GPU.

Conclusion
----------
You should now have a basic understanding on how to train XGBoost models and on how
to tune the hyperparameters to yield the best results. In our simple example,
Tuning the parameters didn't make a huge difference for the accuracy.
But in larger applications, intelligent hyperparameter tuning can make the
difference between a model that doesn't seem to learn at all, and a model
that outperforms all the other ones.

Further References
------------------

* `XGBoost Hyperparameter Tuning - A Visual Guide <https://kevinvecmanis.io/machine%20learning/hyperparameter%20tuning/dataviz/python/2019/05/11/XGBoost-Tuning-Visual-Guide.html>`_
* `Notes on XGBoost Parameter Tuning <https://xgboost.readthedocs.io/en/latest/tutorials/param_tuning.html>`_
* `Doing XGBoost Hyperparameter Tuning the smart way <https://towardsdatascience.com/doing-xgboost-hyper-parameter-tuning-the-smart-way-part-1-of-2-f6d255a45dde>`_
