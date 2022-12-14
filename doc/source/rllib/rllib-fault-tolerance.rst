.. include:: /_includes/rllib/announcement.rst

.. include:: /_includes/rllib/we_are_hiring.rst

Fault Tolerance And Elastic Training
======================================================

RLlib handles common failures modes (e.g. machine failures, spot isntance preemption,
network outages, or Ray cluster failures) via logics at multiple system levels.
This doc discusses them in details.

Fault Tolerance and Recovery Provided by Ray Tune
-------------------------------------------------

Ray Tune provides fault tolerance and recovery at trial level.

Majority of RLlib runs are launched with Ray Tune, these trials periodically checkpoint
the entire training states to user specified persistent storage locations. If a trial fails,
Ray Tune will automatically restart it from the latest
:ref:`checkpointed <tune-two-types-of-ckpt>` state.

This serves as our last line of defense, catching any unexpected errors that are not
handled by RLlib.

Elastic RolloutWorkers
----------------------

RLlib supports self-recovering and elastic workersets for both rollout and evaluation workers.
This provides fault tolerance at worker level.

Worker fault tolerance can be turned on by setting config ``recreate_failed_workers`` to True.
Under the hood, RLlib relies on Ray Core :ref:`actor fault tolerance <actor-fault-tolerance>`
to automatically recover failed worker actors. Whenever a worker actor is reconstructed,
RLlib will make sure its latest state gets synced before new episodes can be sampled. 

RLlib workers are also elastic meaning that they do not require the full set of workers to
function. For example, if an RLlib trial uses spot instances, it is quite possible for the
trial to only have partial resources at a given time, potentially resulting in a subset
of workers not being able to get scheduled. In this case, RLlib will make sure training and
evaluation carries on with whatever healthy workers left at a reduced speed.
RLlib achieves this by utilizing a
`state-aware and fault tolerant actor manager <https://github.com/ray-project/ray/blob/master/rllib/utils/actor_manager.py>`__.

Env Fault Tolerance
-------------------

In addition to worker fault tolerance, RLlib offers fault tolerance at environment level as well.

It's common for a rollout or evaluation workers to run multiple environments in parallel to take
advantage of, for example, the parallel computing power that GPU offers. It may then be wasteful
if the entire worker needs to be reconstructed because of errors from a single environment.

In that case, RLlib offers the capability to restart individual environments without bubbling the
errors to higher level components. You can do that easily by turning on config
``restart_failed_sub_environments``.

Please note that environment restarts are blocking operations.
A rollout worker will wait until the environment comes back and finishes initialization.
So for on-policy algorithms, it may be better to recover at worker level to make sure
training progresses with elastic worker set while the environments are being reconstructed.

Other Miscellaneous Considerations 
----------------------------------

By default, RLlib runs health checks during initial worker construction.
The whole job will error out if a completely healthy worker fleet can not be established
at the start of a training run. If an environment is by nature flaky, you may want to turn
off this feature by setting config ``validate_workers_after_construction`` to False.

Lastly, in an extreme case where no healthy workers are left for training, RLlib will wait
certain number of iterations for some of the workers to recover before the entire training
job failed.
The number of iterations it waits can be configured with the config
``num_consecutive_worker_failures_tolerance``.
