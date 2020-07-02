.. _tune-wandb-integration:

Using Tune with Weights and Biases (wandb)
==========================================


Instrumenting your trainable with "in-process logging"
------------------------------------------------------


Each wandb "run" are associated with a session. wandb doesn't support logging to multiple concurrent runs within a single process.

Since Tune runs concurrent trials, this means that you can't use the standard Logging package in Tune.

As a workaround you can log within the trainable:

.. code-block:: python

    from collections import defaultdict
    import copy
    import logging
    import os
    import time
    import wandb
    from ray.tune.utils import flatten_dict

    WANDB_COMMIT_PERIOD = 5

    logger = logging.getLogger(__name__)


    def set_wandb_api_key(key_path):
        """Sets env variable for WandB API key."""
        var = "WANDB_API_KEY"
        if not os.environ.get(var):
            if os.path.exists(key_path):
                api_key = open(key_path, "r").read().strip()
                os.environ[var] = api_key
            else:
                logger.warning(f"File not found: {key_path}. "
                               "Distributed wandb logging may break.")
        else:
            logger.info(f"{var} already set. Ignoring {key_path}.")


    class WandbTrainableMixin:
        def __init__(self, config):
            self._wandb_api_key_path = config.get("wandb_api_key_path")
            self._last_commit = time.time()

            config = flatten_dict(config)
            set_wandb_api_key(os.path.expanduser(self._wandb_api_key_path))
            exp_name = config.get("wandb_exp_name")
            exp_trial_id = f"{self.trial_id}-{exp_name}"

           wandb.init(
                project=config.get("wandb_proj_name"),
                group=exp_name,
                name=exp_trial_id,
                id=exp_trial_id,
                resume=exp_trial_id,
                reinit=True,
                allow_val_change=True)
            wandb_config = {
                key: config[key]
                for key in config if isinstance(config[key], (str, int, float))
            }
            wandb.config.update(wandb_config, allow_val_change=True)

        def save(self):
            wandb.log({})

        def log_wandb(self, result):
            res_dict = {
                str(k): v
                for k, v in result.items()
                if (v and "config" not in k and not isinstance(v, str))
            }

            commit = time.time() - self._last_commit > WANDB_COMMIT_PERIOD
            self._last_commit = time.time() if commit else self._last_commit
            wandb.log(res_dict, commit=commit)

        def stop(self):
            wandb.log({})  # Flush logs.


Instrumenting your driver process to log experiment-level metrics
-----------------------------------------------------------------

Create a hook:

.. code-block:: python

    class WandbLogger:
        def __init__(self, refresh=10, **init_kwargs):
            try:
                wandb.init(**init_kwargs)
            except Exception:
                logger.exception()
            self._refresh = refresh
            self.last_update = time.time()

        def log(self, result):
            try:
                now = time.time()
                commit = now - self.last_update > self._refresh
                if commit:
                    self.last_update = now
                wandb.log(result, commit=commit)
            except Exception:
                logger.exception()

        def close(self):
            try:
                wandb.log({})
                wandb.join()
            except Exception:
                logger.exception()

Extend the executor:

.. code-block:: python

    class CloudExecutor(RayTrialExecutor):
        def __init__(self,
                     deadline_s,
                     queue_trials=True,
                     reuse_actors=False,
                     hooks=True,
                     failure_injector=None):
            super(CloudExecutor, self).__init__(
                queue_trials=queue_trials,
                reuse_actors=reuse_actors,
                ray_auto_init=True)
            self._start = time.time()
            self.aggregates = [] if not hooks else None
            self._hooks = hooks
            self._failure_injector = failure_injector

        @staticmethod
        def _get_metrics(trial_runner):
            """Get experiment-level metrics."""
            trials = trial_runner.get_trials()
            if len(trials) == 0:
                return {}

            scheduler = trial_runner.scheduler_alg
            stats = scheduler.stats() if hasattr(scheduler, "stats") else {}

            mode, metric = scheduler.mode, scheduler.metric
            opt, worst = (max, -np.inf) if mode == "max" else (min, np.inf)
            results = [t.last_result for t in trials]

            best_result = opt(results, key=lambda r: r.get(metric, worst))
            metric_val = best_result.get(metric, worst)
            if metric_val == worst:
                return stats

            monotonic_metric = f"{mode}_{metric}"
            mono_metric_val = opt(
                results, key=lambda r: r.get(monotonic_metric, worst)).get(
                    monotonic_metric, worst)

            num_trials = len([t for t in trials if t.status == Trial.RUNNING])
            trial_budget_used = sum(r.get("budget_used", 0) for r in results)

            num_iters = sum(r.get("training_iteration", 0) for r in results)
            num_samples = sum(r.get("cumulative_num_samples", 0) for r in results)

            stats.update({
                "ts": scheduler.timestamp,
                f"top_{metric}": metric_val,
                f"top_{monotonic_metric}": mono_metric_val,
                "trial_budget_used": trial_budget_used,
                "regret": trial_budget_used - best_result.get("budget_used", 0),
                "num_trials": num_trials,
                "total_cumulative_num_samples": num_samples,
                "num_iters": num_iters,
            })

            return stats

        def on_step_end(self, runner):
            if self._hooks:
                metrics = self._get_metrics(runner)
                self._hooks.log(metrics)
            if time.time() - self._start > self.deadline_s:
                logger.debug("Stopping experiment.")
                runner.request_stop_experiment()

    set_wandb_api_key(PATH_TO_WANDB_KEY)  # can do something else
    driver_id = "driver__{}".format(exp_name)
    wandblog = WandbLogger(
        project=proj_name,
        group=exp_name,
        name=driver_id,
        id=driver_id,
        config=vars(args))

    tune.run(executor=WandbExecutor())



Authentication (for cluster mode)
---------------------------------

If you need to distribute your key across the cluster:

.. code-block:: yaml

    file_mounts: {
        # necessary for distributed wandb logging.
        ~/.wandb/api_key: ~/.wandb/api_key,
    }