Why choose Tune?
================

There are many other hyperparameter optimization libraries out there. If you're new to Tune, you're probably wondering what makes Tune different?

Tune has a variety of cutting-edge optimization algorithms
----------------------------------------------------------

As a user, you're probably looking into hyperparameter optimization because you want to quickly increase your model performance. State of the art algorithms can reduce the cost of tuning by aggressively terminating bad hyperparameter evaluations, intelligently choosing better parameters to evaluate, or even changing the hyperparameters during training to optimize hyperparameter *schedules*.

Tune enables you to leverage wide variety of these cutting edge optimization algorithms, from HyperBand to Google Deepmind's Population-based training.

For more information, take a look at the Optimization Algorithms guide.

Tune simplifies your workflow
-----------------------------

A key problem with machine learning frameworks is the need to restructure all of your code to fit the framework.

However, with Tune, you can optimize your model just by adding a few code snippets.

Further, Tune actually removes boilerplate from your code training workflow, automatically managing checkpoints and logging of results to tools such as MLFlow and TensorBoard.

For more information, see our guide on Getting started and Logging in Tune.


Tune has first-class multi-GPU & distributed training support
-------------------------------------------------------------

Hyperparameter tuning is known to be highly time-consuming, so it is often necessary to parallelize this process.

Most other tuning frameworks require you to implement your own multi-process framework or build your own distributed system to speed up hyperparameter tuning.

However, Tune allows you to transparently parallelize across multiple GPUs and multiple nodes. Tune even has seamless fault tolerance and cloud support, allowing you to scale up your hyperparameter search by 100x while reducing costs by up to 10x.

For more information, see our guide for distributed hyperparameter tuning.


