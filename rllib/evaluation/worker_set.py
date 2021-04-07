import gym
import logging
from types import FunctionType
from typing import Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union

import ray
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.evaluation.rollout_worker import RolloutWorker, \
    _validate_multiagent_config
from ray.rllib.offline import NoopOutput, JsonReader, MixedInput, JsonWriter, \
    ShuffledInput, D4RLReader
from ray.rllib.env.env_context import EnvContext
from ray.rllib.policy import Policy
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import PolicyID, TrainerConfigDict, EnvType

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)

# Generic type var for foreach_* methods.
T = TypeVar("T")


@DeveloperAPI
class WorkerSet:
    """Represents a set of RolloutWorkers.

    There must be one local worker copy, and zero or more remote workers.
    """

    def __init__(self,
                 *,
                 env_creator: Optional[Callable[[EnvContext], EnvType]] = None,
                 validate_env: Optional[Callable[[EnvType], None]] = None,
                 policy_class: Optional[Type[Policy]] = None,
                 trainer_config: Optional[TrainerConfigDict] = None,
                 num_workers: int = 0,
                 logdir: Optional[str] = None,
                 _setup: bool = True):
        """Create a new WorkerSet and initialize its workers.

        Args:
            env_creator (Optional[Callable[[EnvContext], EnvType]]): Function
                that returns env given env config.
            validate_env (Optional[Callable[[EnvType], None]]): Optional
                callable to validate the generated environment (only on
                worker=0).
            policy (Optional[Type[Policy]]): A rllib.policy.Policy class.
            trainer_config (Optional[TrainerConfigDict]): Optional dict that
                extends the common config of the Trainer class.
            num_workers (int): Number of remote rollout workers to create.
            logdir (Optional[str]): Optional logging directory for workers.
            _setup (bool): Whether to setup workers. This is only for testing.
        """

        if not trainer_config:
            from ray.rllib.agents.trainer import COMMON_CONFIG
            trainer_config = COMMON_CONFIG

        self._env_creator = env_creator
        self._policy_class = policy_class
        self._remote_config = trainer_config
        self._logdir = logdir

        if _setup:
            self._local_config = merge_dicts(
                trainer_config,
                {"tf_session_args": trainer_config["local_tf_session_args"]})

            # Create a number of remote workers.
            self._remote_workers = []
            self.add_workers(num_workers)

            # If num_workers > 0, get the action_spaces and observation_spaces
            # to not be forced to create an Env on the local worker.
            if self._remote_workers:
                remote_spaces = ray.get(self.remote_workers(
                )[0].foreach_policy.remote(
                    lambda p, pid: (pid, p.observation_space, p.action_space)))
                spaces = {
                    e[0]: (getattr(e[1], "original_space", e[1]), e[2])
                    for e in remote_spaces
                }
            else:
                spaces = None

            # Always create a local worker.
            self._local_worker = self._make_worker(
                cls=RolloutWorker,
                env_creator=env_creator,
                validate_env=validate_env,
                policy_cls=self._policy_class,
                worker_index=0,
                num_workers=num_workers,
                config=self._local_config,
                spaces=spaces,
            )

    def local_worker(self) -> RolloutWorker:
        """Return the local rollout worker."""
        return self._local_worker

    def remote_workers(self) -> List["ActorHandle"]:
        """Return a list of remote rollout workers."""
        return self._remote_workers

    def sync_weights(self) -> None:
        """Syncs weights of remote workers with the local worker."""
        if self.remote_workers():
            weights = ray.put(self.local_worker().get_weights())
            for e in self.remote_workers():
                e.set_weights.remote(weights)

    def add_workers(self, num_workers: int) -> None:
        """Creates and add a number of remote workers to this worker set.

        Args:
            num_workers (int): The number of remote Workers to add to this
                WorkerSet.
        """
        remote_args = {
            "num_cpus": self._remote_config["num_cpus_per_worker"],
            "num_gpus": self._remote_config["num_gpus_per_worker"],
            "resources": self._remote_config["custom_resources_per_worker"],
        }
        cls = RolloutWorker.as_remote(**remote_args).remote
        self._remote_workers.extend([
            self._make_worker(
                cls=cls,
                env_creator=self._env_creator,
                validate_env=None,
                policy_cls=self._policy_class,
                worker_index=i + 1,
                num_workers=num_workers,
                config=self._remote_config) for i in range(num_workers)
        ])

    def reset(self, new_remote_workers: List["ActorHandle"]) -> None:
        """Called to change the set of remote workers."""
        self._remote_workers = new_remote_workers

    def stop(self) -> None:
        """Stop all rollout workers."""
        try:
            self.local_worker().stop()
            tids = [w.stop.remote() for w in self.remote_workers()]
            ray.get(tids)
        except Exception:
            logger.exception("Failed to stop workers")
        finally:
            for w in self.remote_workers():
                w.__ray_terminate__.remote()

    @DeveloperAPI
    def foreach_worker(self, func: Callable[[RolloutWorker], T]) -> List[T]:
        """Apply the given function to each worker instance."""

        local_result = [func(self.local_worker())]
        remote_results = ray.get(
            [w.apply.remote(func) for w in self.remote_workers()])
        return local_result + remote_results

    @DeveloperAPI
    def foreach_worker_with_index(
            self, func: Callable[[RolloutWorker, int], T]) -> List[T]:
        """Apply the given function to each worker instance.

        The index will be passed as the second arg to the given function.
        """
        local_result = [func(self.local_worker(), 0)]
        remote_results = ray.get([
            w.apply.remote(func, i + 1)
            for i, w in enumerate(self.remote_workers())
        ])
        return local_result + remote_results

    @DeveloperAPI
    def foreach_policy(self, func: Callable[[Policy, PolicyID], T]) -> List[T]:
        """Apply the given function to each worker's (policy, policy_id) tuple.

        Args:
            func (callable): A function - taking a Policy and its ID - that is
                called on all workers' Policies.

        Returns:
            List[any]: The list of return values of func over all workers'
                policies.
        """
        local_results = self.local_worker().foreach_policy(func)
        remote_results = []
        for worker in self.remote_workers():
            res = ray.get(
                worker.apply.remote(lambda w: w.foreach_policy(func)))
            remote_results.extend(res)
        return local_results + remote_results

    @DeveloperAPI
    def trainable_policies(self) -> List[PolicyID]:
        """Return the list of trainable policy ids."""
        return self.local_worker().foreach_trainable_policy(lambda _, pid: pid)

    @DeveloperAPI
    def foreach_trainable_policy(
            self, func: Callable[[Policy, PolicyID], T]) -> List[T]:
        """Apply `func` to all workers' Policies iff in `policies_to_train`.

        Args:
            func (callable): A function - taking a Policy and its ID - that is
                called on all workers' Policies in `worker.policies_to_train`.

        Returns:
            List[any]: The list of n return values of all
                `func([trainable policy], [ID])`-calls.
        """
        local_results = self.local_worker().foreach_trainable_policy(func)
        remote_results = []
        for worker in self.remote_workers():
            res = ray.get(
                worker.apply.remote(
                    lambda w: w.foreach_trainable_policy(func)))
            remote_results.extend(res)
        return local_results + remote_results

    @staticmethod
    def _from_existing(local_worker: RolloutWorker,
                       remote_workers: List["ActorHandle"] = None):
        workers = WorkerSet(
            env_creator=None,
            policy_class=None,
            trainer_config={},
            _setup=False)
        workers._local_worker = local_worker
        workers._remote_workers = remote_workers or []
        return workers

    def _make_worker(
            self,
            *,
            cls: Callable,
            env_creator: Callable[[EnvContext], EnvType],
            validate_env: Optional[Callable[[EnvType], None]],
            policy_cls: Type[Policy],
            worker_index: int,
            num_workers: int,
            config: TrainerConfigDict,
            spaces: Optional[Dict[PolicyID, Tuple[gym.spaces.Space,
                                                  gym.spaces.Space]]] = None,
    ) -> Union[RolloutWorker, "ActorHandle"]:
        def session_creator():
            logger.debug("Creating TF session {}".format(
                config["tf_session_args"]))
            return tf1.Session(
                config=tf1.ConfigProto(**config["tf_session_args"]))

        if isinstance(config["input"], FunctionType):
            input_creator = config["input"]
        elif config["input"] == "sampler":
            input_creator = (lambda ioctx: ioctx.default_sampler_input())
        elif isinstance(config["input"], dict):
            input_creator = (
                lambda ioctx: ShuffledInput(MixedInput(config["input"], ioctx),
                                            config["shuffle_buffer_size"]))
        elif "d4rl" in config["input"]:
            env_name = config["input"].split(".")[1]
            input_creator = (lambda ioctx: D4RLReader(env_name, ioctx))
        else:
            input_creator = (
                lambda ioctx: ShuffledInput(JsonReader(config["input"], ioctx),
                                            config["shuffle_buffer_size"]))

        if isinstance(config["output"], FunctionType):
            output_creator = config["output"]
        elif config["output"] is None:
            output_creator = (lambda ioctx: NoopOutput())
        elif config["output"] == "logdir":
            output_creator = (lambda ioctx: JsonWriter(
                ioctx.log_dir,
                ioctx,
                max_file_size=config["output_max_file_size"],
                compress_columns=config["output_compress_columns"]))
        else:
            output_creator = (lambda ioctx: JsonWriter(
                config["output"],
                ioctx,
                max_file_size=config["output_max_file_size"],
                compress_columns=config["output_compress_columns"]))

        if config["input"] == "sampler":
            input_evaluation = []
        else:
            input_evaluation = config["input_evaluation"]

        # Fill in the default policy_cls if 'None' is specified in multiagent.
        if config["multiagent"]["policies"]:
            tmp = config["multiagent"]["policies"]
            _validate_multiagent_config(tmp, allow_none_graph=True)
            # TODO: (sven) Allow for setting observation and action spaces to
            #  None as well, in which case, spaces are taken from env.
            #  It's tedious to have to provide these in a multi-agent config.
            for k, v in tmp.items():
                if v[0] is None:
                    tmp[k] = (policy_cls, v[1], v[2], v[3])
            policy_spec = tmp
        # Otherwise, policy spec is simply the policy class itself.
        else:
            policy_spec = policy_cls

        if worker_index == 0:
            extra_python_environs = config.get(
                "extra_python_environs_for_driver", None)
        else:
            extra_python_environs = config.get(
                "extra_python_environs_for_worker", None)

        worker = cls(
            env_creator=env_creator,
            validate_env=validate_env,
            policy_spec=policy_spec,
            policy_mapping_fn=config["multiagent"]["policy_mapping_fn"],
            policies_to_train=config["multiagent"]["policies_to_train"],
            tf_session_creator=(session_creator
                                if config["tf_session_args"] else None),
            rollout_fragment_length=config["rollout_fragment_length"],
            count_steps_by=config["multiagent"]["count_steps_by"],
            batch_mode=config["batch_mode"],
            episode_horizon=config["horizon"],
            preprocessor_pref=config["preprocessor_pref"],
            sample_async=config["sample_async"],
            compress_observations=config["compress_observations"],
            num_envs=config["num_envs_per_worker"],
            observation_fn=config["multiagent"]["observation_fn"],
            observation_filter=config["observation_filter"],
            clip_rewards=config["clip_rewards"],
            clip_actions=config["clip_actions"],
            env_config=config["env_config"],
            model_config=config["model"],
            policy_config=config,
            worker_index=worker_index,
            num_workers=num_workers,
            record_env=config["record_env"],
            log_dir=self._logdir,
            log_level=config["log_level"],
            callbacks=config["callbacks"],
            input_creator=input_creator,
            input_evaluation=input_evaluation,
            output_creator=output_creator,
            remote_worker_envs=config["remote_worker_envs"],
            remote_env_batch_wait_ms=config["remote_env_batch_wait_ms"],
            soft_horizon=config["soft_horizon"],
            no_done_at_end=config["no_done_at_end"],
            seed=(config["seed"] + worker_index)
            if config["seed"] is not None else None,
            fake_sampler=config["fake_sampler"],
            extra_python_environs=extra_python_environs,
            spaces=spaces,
        )

        return worker
