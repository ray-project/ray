Tune offers cutting-edge optimization algorithms.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As a user, you're probably looking into hyperparameter optimization because you want to quickly increase your model performance.

Tune enables you to leverage a variety of these cutting edge optimization algorithms, reducing the cost of tuning by `aggressively terminating bad hyperparameter evaluations <tune-scheduler-hyperband>`_, intelligently :ref:`choosing better parameters to evaluate <tune-search-alg>`, or even :ref:`changing the hyperparameters during training <tune-scheduler-pbt>` to optimize hyperparameter schedules.

Tune simplifies your workflow.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A key problem with machine learning frameworks is the need to restructure all of your code to fit the framework.

With Tune, you can optimize your model just by :ref:`adding a few code snippets <tune-tutorial>`.

Further, Tune actually removes boilerplate from your code training workflow, automatically :ref:`managing checkpoints <tune-checkpoint>` and :ref:`logging results to tools <tune-logging>` such as MLFlow and TensorBoard.


Tune provides first-class multi-GPU & distributed training support.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Hyperparameter tuning is known to be highly time-consuming, so it is often necessary to parallelize this process. Most other tuning frameworks require you to implement your own multi-process framework or build your own distributed system to speed up hyperparameter tuning.

However, Tune allows you to transparently :ref:`parallelize across multiple GPUs and multiple nodes <tune-parallelism>`. Tune even has seamless :ref:`fault tolerance and cloud support <tune-distributed>`, allowing you to scale up your hyperparameter search by 100x while reducing costs by up to 10x by using cheap preemptible instances.

What if I'm already doing hyperparameter tuning?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You might be already using an existing hyperparameter tuning tool such as HyperOpt or Bayesian Optimization.

In this situation, Tune actually allows you to power up your existing workflow. Tune's :ref:`Search Algorithms <tune-search-alg>` integrate with a variety of popular hyperparameter tuning libraries (such as Nevergrad or HyperOpt) and allow you to seamlessly scale up your optimization process -- without sacrificing performance.
