import copy
import glob
import logging
import os

from ray.util.debug import log_once

logger = logging.getLogger(__name__)


class Searcher:
    """Abstract class for wrapping suggesting algorithms.

    Custom algorithms can extend this class easily by overriding the
    `suggest` method provide generated parameters for the trials.

    Any subclass that implements ``__init__`` must also call the
    constructor of this class: ``super(Subclass, self).__init__(...)``.

    To track suggestions and their corresponding evaluations, the method
    `suggest` will be passed a trial_id, which will be used in
    subsequent notifications.

    Args:
        metric (str): The training result objective value attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.

    .. code-block:: python

        class ExampleSearch(Searcher):
            def __init__(self, metric="mean_loss", mode="min", **kwargs):
                super(ExampleSearch, self).__init__(
                    metric=metric, mode=mode, **kwargs)
                self.optimizer = Optimizer()
                self.configurations = {}

            def suggest(self, trial_id):
                configuration = self.optimizer.query()
                self.configurations[trial_id] = configuration

            def on_trial_complete(self, trial_id, result, **kwargs):
                configuration = self.configurations[trial_id]
                if result and self.metric in result:
                    self.optimizer.update(configuration, result[self.metric])

        tune.run(trainable_function, search_alg=ExampleSearch())


    """
    FINISHED = "FINISHED"
    CKPT_FILE_TMPL = "searcher-state-{}.pkl"

    def __init__(self,
                 metric="episode_reward_mean",
                 mode="max",
                 max_concurrent=None,
                 use_early_stopped_trials=None):
        if use_early_stopped_trials is False:
            raise DeprecationWarning(
                "Early stopped trials are now always used. If this is a "
                "problem, file an issue: https://github.com/ray-project/ray.")
        if max_concurrent is not None:
            logger.warning(
                "DeprecationWarning: `max_concurrent` is deprecated for this "
                "search algorithm. Use tune.suggest.ConcurrencyLimiter() "
                "instead. This will raise an error in future versions of Ray.")
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"
        self._metric = metric
        self._mode = mode

    def on_trial_result(self, trial_id, result):
        """Optional notification for result during training.

        Note that by default, the result dict may include NaNs or
        may not include the optimization metric. It is up to the
        subclass implementation to preprocess the result to
        avoid breaking the optimization process.

        Args:
            trial_id (str): A unique string ID for the trial.
            result (dict): Dictionary of metrics for current training progress.
                Note that the result dict may include NaNs or
                may not include the optimization metric. It is up to the
                subclass implementation to preprocess the result to
                avoid breaking the optimization process.
        """
        pass

    def on_trial_complete(self, trial_id, result=None, error=False):
        """Notification for the completion of trial.

        Typically, this method is used for notifying the underlying
        optimizer of the result.

        Args:
            trial_id (str): A unique string ID for the trial.
            result (dict): Dictionary of metrics for current training progress.
                Note that the result dict may include NaNs or
                may not include the optimization metric. It is up to the
                subclass implementation to preprocess the result to
                avoid breaking the optimization process. Upon errors, this
                may also be None.
            error (bool): True if the training process raised an error.

        """
        raise NotImplementedError

    def suggest(self, trial_id):
        """Queries the algorithm to retrieve the next set of parameters.

        Arguments:
            trial_id (str): Trial ID used for subsequent notifications.

        Returns:
            dict | FINISHED | None: Configuration for a trial, if possible.
                If FINISHED is returned, Tune will be notified that
                no more suggestions/configurations will be provided.
                If None is returned, Tune will skip the querying of the
                searcher for this step.

        """
        raise NotImplementedError

    def save(self, checkpoint_path):
        """Save state to path for this search algorithm.

        Args:
            checkpoint_path (str): File where the search algorithm
                state is saved. This path should be used later when
                restoring from file.

        Example:

        .. code-block:: python

            search_alg = Searcher(...)

            analysis = tune.run(
                cost,
                num_samples=5,
                search_alg=search_alg,
                name=self.experiment_name,
                local_dir=self.tmpdir)

            search_alg.save("./my_favorite_path.pkl")

        .. versionchanged:: 0.8.7
            Save is automatically called by `tune.run`. You can use
            `restore_from_dir` to restore from an experiment directory
            such as `~/ray_results/trainable`.

        """
        raise NotImplementedError

    def restore(self, checkpoint_path):
        """Restore state for this search algorithm


        Args:
            checkpoint_path (str): File where the search algorithm
                state is saved. This path should be the same
                as the one provided to "save".

        Example:

        .. code-block:: python

            search_alg.save("./my_favorite_path.pkl")

            search_alg2 = Searcher(...)
            search_alg2 = ConcurrencyLimiter(search_alg2, 1)
            search_alg2.restore(checkpoint_path)
            tune.run(cost, num_samples=5, search_alg=search_alg2)

        """
        raise NotImplementedError

    def get_state(self):
        raise NotImplementedError

    def set_state(self, state):
        raise NotImplementedError

    def save_to_dir(self, checkpoint_dir, session_str="default"):
        """Automatically saves the given searcher to the checkpoint_dir.

        This is automatically used by tune.run during a Tune job.

        Args:
            checkpoint_dir (str): Filepath to experiment dir.
            session_str (str): Unique identifier of the current run
                session.
        """
        tmp_search_ckpt_path = os.path.join(checkpoint_dir,
                                            ".tmp_searcher_ckpt")
        success = True
        try:
            self.save(tmp_search_ckpt_path)
        except NotImplementedError:
            if log_once("suggest:save_to_dir"):
                logger.warning(
                    "save not implemented for Searcher. Skipping save.")
            success = False

        if success and os.path.exists(tmp_search_ckpt_path):
            os.rename(
                tmp_search_ckpt_path,
                os.path.join(checkpoint_dir,
                             self.CKPT_FILE_TMPL.format(session_str)))

    def restore_from_dir(self, checkpoint_dir):
        """Restores the state of a searcher from a given checkpoint_dir.

        Typically, you should use this function to restore from an
        experiment directory such as `~/ray_results/trainable`.

        .. code-block:: python

            experiment_1 = tune.run(
                cost,
                num_samples=5,
                search_alg=search_alg,
                verbose=0,
                name=self.experiment_name,
                local_dir="~/my_results")

            search_alg2 = Searcher()
            search_alg2.restore_from_dir(
                os.path.join("~/my_results", self.experiment_name)
        """

        pattern = self.CKPT_FILE_TMPL.format("*")
        full_paths = glob.glob(os.path.join(checkpoint_dir, pattern))
        if not full_paths:
            raise RuntimeError(
                "Searcher unable to find checkpoint in {}".format(
                    checkpoint_dir))  # TODO
        most_recent_checkpoint = max(full_paths)
        self.restore(most_recent_checkpoint)

    @property
    def metric(self):
        """The training result objective value attribute."""
        return self._metric

    @property
    def mode(self):
        """Specifies if minimizing or maximizing the metric."""
        return self._mode


class ConcurrencyLimiter(Searcher):
    """A wrapper algorithm for limiting the number of concurrent trials.

    Args:
        searcher (Searcher): Searcher object that the
            ConcurrencyLimiter will manage.

    Example:

    .. code-block:: python

        from ray.tune.suggest import ConcurrencyLimiter
        search_alg = HyperOptSearch(metric="accuracy")
        search_alg = ConcurrencyLimiter(search_alg, max_concurrent=2)
        tune.run(trainable, search_alg=search_alg)
    """

    def __init__(self, searcher, max_concurrent):
        assert type(max_concurrent) is int and max_concurrent > 0
        self.searcher = searcher
        self.max_concurrent = max_concurrent
        self.live_trials = set()
        super(ConcurrencyLimiter, self).__init__(
            metric=self.searcher.metric, mode=self.searcher.mode)

    def suggest(self, trial_id):
        assert trial_id not in self.live_trials, (
            f"Trial ID {trial_id} must be unique: already found in set.")
        if len(self.live_trials) >= self.max_concurrent:
            logger.debug(
                f"Not providing a suggestion for {trial_id} due to "
                "concurrency limit: %s/%s.", len(self.live_trials),
                self.max_concurrent)
            return
        suggestion = self.searcher.suggest(trial_id)
        if suggestion not in (None, Searcher.FINISHED):
            self.live_trials.add(trial_id)
        return suggestion

    def on_trial_complete(self, trial_id, result=None, error=False):
        if trial_id not in self.live_trials:
            return
        else:
            self.searcher.on_trial_complete(
                trial_id, result=result, error=error)
            self.live_trials.remove(trial_id)

    def get_state(self):
        state = self.__dict__.copy()
        del state["searcher"]
        return copy.deepcopy(state)

    def set_state(self, state):
        self.__dict__.update(state)
