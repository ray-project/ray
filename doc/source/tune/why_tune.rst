Why choose Tune?
================

There are many other hyperparameter optimization libraries out there. If you're new to Tune, you're probably wondering what makes Tune different?

Tune offers cutting-edge optimization algorithms
------------------------------------------------

As a user, you're probably looking into hyperparameter optimization because you want to quickly increase your model performance. State of the art algorithms can reduce the cost of tuning by aggressively terminating bad hyperparameter evaluations, intelligently choosing better parameters to evaluate, or even changing the hyperparameters during training to optimize hyperparameter *schedules*.

Tune enables you to leverage wide variety of these cutting edge optimization algorithms, from :ref:`HyperBand <tune-scheduler-hyperband>` to Google Deepmind's :ref:`Population-based training <tune-pbt-guide>`.

For more information, take a look at the documentation on :ref:`Optimization Algorithms <tune-search-alg>`.

Tune simplifies your workflow
-----------------------------

A key problem with machine learning frameworks is the need to restructure all of your code to fit the framework.

However, with Tune, you can optimize your model just by adding a few code snippets.

Further, Tune actually removes boilerplate from your code training workflow, automatically managing checkpoints and logging of results to tools such as MLFlow and TensorBoard.

For more information, see our guide on Getting started and Logging in Tune.


Tune provides First-class multi-GPU & distributed training support
------------------------------------------------------------------

Hyperparameter tuning is known to be highly time-consuming, so it is often necessary to parallelize this process.

Most other tuning frameworks require you to implement your own multi-process framework or build your own distributed system to speed up hyperparameter tuning.

However, Tune allows you to transparently parallelize across multiple GPUs and multiple nodes. Tune even has seamless fault tolerance and cloud support, allowing you to scale up your hyperparameter search by 100x while reducing costs by up to 10x.

For more information, see our :ref:`guide for distributed hyperparameter tuning <tune-distributed>`.


What if I'm already doing hyperparameter tuning?
------------------------------------------------

You might be already using an existing hyperparameter tuning tool such as HyperOpt or Bayesian Optimization.

In this situation, Ray Tune actually allows you to power up your existing workflow. Tune's Search Algorithm integrate with a variety of popular hyperparameter tuning libraries (such as Nevergrad or HyperOpt) and allow you to seamlessly scale up your optimization process -- without sacrificing performance.

For more information, take a look at the documentation on :ref:`Optimization Algorithms <tune-search-alg>`.

Further Information
-------------------

* Read more about :ref:`the core Tune concepts <tune-60-seconds>`.
* Check out :ref:`code examples <tune-general-examples>` using Tune.
