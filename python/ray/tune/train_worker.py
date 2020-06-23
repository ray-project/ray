class Trainable:
    """Abstract class for trainable models, functions, etc.

    A call to ``train()`` on a trainable will execute one logical iteration of
    training. As a rule of thumb, the execution time of one train call should
    be large enough to avoid overheads (i.e. more than a few seconds), but
    short enough to report progress periodically (i.e. at most a few minutes).

    Calling ``save()`` should save the training state of a trainable to disk,
    and ``restore(path)`` should restore a trainable to the given state.

    Generally you only need to implement ``_setup``, ``_train``,
    ``_save``, and ``_restore`` when subclassing Trainable.

    Other implementation methods that may be helpful to override are
    ``_log_result``, ``reset_config``, ``_stop``, and ``_export_model``.

    When using Tune, Tune will convert this class into a Ray actor, which
    runs on a separate process. Tune will also change the current working
    directory of this process to ``self.logdir``.

    """
    def __init__(self, config=None, logger_creator=None):
        """Subclasses should override this for custom initialization.

        Args:
            config (dict): Hyperparameters and other configs given.
                Copy of `self.config`.
        """
        self.config = config or {}
        trial_info = self.config.pop(TRIAL_INFO, None)

        if logger_creator:
            self._result_logger = logger_creator(self.config)
            self._logdir = self._result_logger.logdir
        else:
            logdir_prefix = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            ray.utils.try_to_create_directory(DEFAULT_RESULTS_DIR)
            self._logdir = tempfile.mkdtemp(
                prefix=logdir_prefix, dir=DEFAULT_RESULTS_DIR)
            self._result_logger = UnifiedLogger(
                self.config, self._logdir, loggers=None)

        self._trial_info = trial_info


    @classmethod
    def default_resource_request(cls, config):
        """Provides a static resource requirement for the given configuration.

        This can be overridden by sub-classes to set the correct trial resource
        allocation, so the user does not need to.

        .. code-block:: python

            @classmethod
            def default_resource_request(cls, config):
                return Resources(
                    cpu=0,
                    gpu=0,
                    extra_cpu=config["workers"],
                    extra_gpu=int(config["use_gpu"]) * config["workers"])

        Returns:
            Resources: A Resources object consumed by Tune for queueing.
        """
        return None

    @classmethod
    def resource_help(cls, config):
        """Returns a help string for configuring this trainable's resources.

        Args:
            config (dict): The Trainer's config dict.
        """
        return ""

    def step(self, **info):
        """Subclasses should override this to implement train().

        The return value will be automatically passed to the loggers. Users
        can also return `tune.result.DONE` or `tune.result.SHOULD_CHECKPOINT`
        as a key to manually trigger termination or checkpointing of this
        trial. Note that manual checkpointing only works when subclassing
        Trainables.

        Returns:
            A dict that describes training progress.

        """

        raise NotImplementedError

    def save(self, tmp_checkpoint_dir=None):
        """Subclasses should override this to implement ``save()``.

        Warning:
            Do not rely on absolute paths in the implementation of ``_save``
            and ``_restore``.

        Use ``validate_save_restore`` to catch ``_save``/``_restore`` errors
        before execution.

        >>> from ray.tune.utils import validate_save_restore
        >>> validate_save_restore(MyTrainableClass)
        >>> validate_save_restore(MyTrainableClass, use_object_store=True)

        Args:
            tmp_checkpoint_dir (str): The directory where the checkpoint
                file must be stored. In a Tune run, if the trial is paused,
                the provided path may be temporary and moved.

        Returns:
            A dict or string. If string, the return value is expected to be
            prefixed by `tmp_checkpoint_dir`. If dict, the return value will
            be automatically serialized by Tune and passed to `_restore()`.

        Examples:
            >>> print(trainable1._save("/tmp/checkpoint_1"))
            "/tmp/checkpoint_1/my_checkpoint_file"
            >>> print(trainable2._save("/tmp/checkpoint_2"))
            {"some": "data"}

            >>> trainable._save("/tmp/bad_example")
            "/tmp/NEW_CHECKPOINT_PATH/my_checkpoint_file" # This will error.
        """


    def restore(self, tmp_checkpoint_dir=None):
        """Subclasses should override this to implement restore().

        Warning:
            In this method, do not rely on absolute paths. The absolute
            path of the checkpoint_dir used in ``_save`` may be changed.

        If ``_save`` returned a prefixed string, the prefix of the checkpoint
        string returned by ``_save`` may be changed. This is because trial
        pausing depends on temporary directories.

        The directory structure under the checkpoint_dir provided to ``_save``
        is preserved.

        See the example below.

        .. code-block:: python

            class Example(Trainable):
                def _save(self, checkpoint_path):
                    print(checkpoint_path)
                    return os.path.join(checkpoint_path, "my/check/point")

                def _restore(self, checkpoint):
                    print(checkpoint)

            >>> trainer = Example()
            >>> obj = trainer.save_to_object()  # This is used when PAUSED.
            <logdir>/tmpc8k_c_6hsave_to_object/checkpoint_0/my/check/point
            >>> trainer.restore_from_object(obj)  # Note the different prefix.
            <logdir>/tmpb87b5axfrestore_from_object/checkpoint_0/my/check/point


        Args:
            checkpoint (str|dict): If dict, the return value is as
                returned by `_save`. If a string, then it is a checkpoint path
                that may have a different prefix than that returned by `_save`.
                The directory structure underneath the `checkpoint_dir`
                `_save` is preserved.
        """

        pass

    def stop(self):
        """Subclasses should override this for any cleanup on stop.

        If any Ray actors are launched in the Trainable (i.e., with a RLlib
        trainer), be sure to kill the Ray actor process here.

        You can kill a Ray actor by calling `actor.__ray_terminate__.remote()`
        on the actor.
        """
        pass

    def reset_config(self, new_config):
        """Resets configuration without restarting the trial.

        This method is optional, but can be implemented to speed up algorithms
        such as PBT, and to allow performance optimizations such as running
        experiments with reuse_actors=True. Note that self.config need to
        be updated to reflect the latest parameter information in Ray logs.

        Args:
            new_config (dir): Updated hyperparameter configuration
                for the trainable.

        Returns:
            True if reset was successful else False.
        """
        return False

    def log_result(self, result):
        """Subclasses can optionally override this to customize logging.

        The logging here is done on the worker process rather than
        the driver. You may want to turn off driver logging via the
        ``loggers`` parameter in ``tune.run`` when overriding this function.

        Args:
            result (dict): Training result returned by _train().
        """
        self._result_logger.on_result(result)


    def stop(self):
        """Releases all resources used by this trainable."""
        self._result_logger.flush()
        self._result_logger.close()
        self._stop()

    @property
    def logdir(self):
        """Directory of the results and checkpoints for this Trainable.

        Tune will automatically sync this folder with the driver if execution
        is distributed.

        Note that the current working directory will also be changed to this.

        """
        return os.path.join(self._logdir, "")

    @property
    def trial_name(self):
        """Trial name for the corresponding trial of this Trainable.

        This is not set if not using Tune.

        .. code-block:: python

            name = self.trial_name
        """
        if self._trial_info:
            return self._trial_info.trial_name
        else:
            return "default"

    @property
    def trial_id(self):
        """Trial ID for the corresponding trial of this Trainable.

        This is not set if not using Tune.

        .. code-block:: python

            trial_id = self.trial_id
        """
        if self._trial_info:
            return self._trial_info.trial_id
        else:
            return "default"

    @property
    def iteration(self):
        """Current training iteration.

        This value is automatically incremented every time `train()` is called
        and is automatically inserted into the training result dict.

        """
        return self._iteration

    @property
    def training_iteration(self):
        """Current training iteration (same as `self.iteration`).

        This value is automatically incremented every time `train()` is called
        and is automatically inserted into the training result dict.

        """
        return self._iteration

    def get_config(self):
        return self.config