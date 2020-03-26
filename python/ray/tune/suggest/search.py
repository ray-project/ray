class SearchAlgorithm:
    """Interface of an event handler API for hyperparameter search.

    Unlike TrialSchedulers, SearchAlgorithms will not have the ability
    to modify the execution (i.e., stop and pause trials).

    Trials added manually (i.e., via the Client API) will also notify
    this class upon new events, so custom search algorithms should
    maintain a list of trials ID generated from this class.

    See also: `ray.tune.suggest.BasicVariantGenerator`.
    """
    _finished = False

    def add_configurations(self, experiments):
        """Tracks given experiment specifications.

        Arguments:
            experiments (Experiment | list | dict): Experiments to run.
        """
        raise NotImplementedError

    def next_trials(self):
        """Provides Trial objects to be queued into the TrialRunner.

        Returns:
            trials (list): Returns a list of trials.
        """
        raise NotImplementedError

    def on_trial_result(self, trial_id, result):
        """Called on each intermediate result returned by a trial.

        This will only be called when the trial is in the RUNNING state.

        Arguments:
            trial_id: Identifier for the trial.
        """
        pass

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        """Notification for the completion of trial.

        Arguments:
            trial_id: Identifier for the trial.
            result (dict): Defaults to None. A dict will
                be provided with this notification when the trial is in
                the RUNNING state AND either completes naturally or
                by manual termination.
            error (bool): Defaults to False. True if the trial is in
                the RUNNING state and errors.
            early_terminated (bool): Defaults to False. True if the trial
                is stopped while in PAUSED or PENDING state.
        """
        pass

    def is_finished(self):
        """Returns True if no trials left to be queued into TrialRunner.

        Can return True before all trials have finished executing.
        """
        return self._finished

    def set_finished(self):
        """Marks the search algorithm as finished."""
        self._finished = True


class Searcher:
    """Abstract class for wrapping suggesting algorithms.

    Custom search algorithms can extend this class easily by overriding the
    `suggest` method provide generated parameters for the trials.

    Any subclass that implements ``__init__`` must also call the
    constructor of this class: ``super(Subclass, self).__init__(...)``.

    To track suggestions and their corresponding evaluations, the method
    `suggest` will be passed a trial_id, which will be used in
    subsequent notifications.

    Args:
        max_concurrent (int): Number of maximum concurrent trials. Defaults
            to 10.
        metric (str): The training result objective value attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        use_clipped_trials (bool): Whether to use early terminated
            trial results in the optimization process.
        repeat (int): Number of times to generate a trial with a repeated
            configuration.

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

    def __init__(self,
                 metric="episode_reward_mean",
                 mode="max",
                 use_clipped_trials=False,
                 max_concurrent=10,
                 repeat=1):
        assert type(max_concurrent) is int and max_concurrent > 0

        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"
        self._metric = metric
        self._mode = mode
        self.use_clipped_trials = use_clipped_trials
        self.max_concurrent = max_concurrent
        self.repeat = repeat

    def on_trial_result(self, trial_id, result):
        """Notification for result during training.

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
        raise NotImplementedError

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          clipped=False):
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
            clipped (bool): True if the trial was terminated by the scheduler.

        """
        raise NotImplementedError

    def suggest(self, trial_id):
        """Queries the algorithm to retrieve the next set of parameters.

        Arguments:
            trial_id: Trial ID used for subsequent notifications.

        Returns:
            dict|None: Configuration for a trial, if possible.
                Else, returns None, which will temporarily stop querying.

        Example:
            >>> searcher = SuggestionAlgorithm(max_concurrent=1)
            >>> searcher.add_configurations({ ... })
            >>> parameters_1 = searcher.suggest()
            >>> parameters_2 = searcher.suggest()
            >>> parameters_2 is None
            >>> searcher.on_trial_complete(trial_id, result)
            >>> parameters_2 = searcher.suggest()
            >>> parameters_2 is not None
        """
        raise NotImplementedError

    def save(self, checkpoint_dir):
        """Save function for this object."""
        raise NotImplementedError

    def restore(self, checkpoint_dir):
        """Restore function for this object."""
        raise NotImplementedError

    @property
    def metric(self):
        """The training result objective value attribute."""
        return self._metric

    @property
    def mode(self):
        """Specifies if minimizing or maximizing the metric."""
        return self._mode


class _MockSuggestionAlgorithm(Searcher):
    def __init__(self, max_concurrent=2, **kwargs):
        self._max_concurrent = max_concurrent
        self.live_trials = {}
        self.counter = {"result": 0, "complete": 0}
        self.final_results = []
        self.stall = False
        self.results = []
        super(_MockSuggestionAlgorithm, self).__init__(**kwargs)

    def suggest(self, trial_id):
        if len(self.live_trials) < self._max_concurrent and not self.stall:
            self.live_trials[trial_id] = 1
            return {"test_variable": 2}
        return None

    def on_trial_result(self, trial_id, result):
        self.counter["result"] += 1
        self.results += [result]

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          clipped=False):
        self.counter["complete"] += 1
        if result:
            self._process_result(result, clipped)
        del self.live_trials[trial_id]

    def _process_result(self, result, clipped):
        if clipped and self._use_clipped:
            self.final_results += [result]
