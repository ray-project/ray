import gym
import logging
import importlib.util
from types import FunctionType
from typing import Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union

import ray
from ray.actor import ActorHandle
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.env.base_env import BaseEnv
from ray.rllib.env.env_context import EnvContext
from ray.rllib.offline import NoopOutput, JsonReader, MixedInput, JsonWriter, \
    ShuffledInput, D4RLReader
from ray.rllib.policy.policy import Policy, PolicySpec
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.typing import EnvType, PolicyID, TrainerConfigDict
from ray.tune.registry import registry_contains_input, registry_get_input

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
                # Try to add the actual env's obs/action spaces.
                try:
                    env_spaces = ray.get(self.remote_workers(
                    )[0].foreach_env.remote(
                        lambda env: (env.observation_space, env.action_space))
                                         )[0]
                    spaces["__env__"] = env_spaces
                except Exception:
                    pass
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

    def remote_workers(self) -> List[ActorHandle]:
        """Return a list of remote rollout workers."""
        return self._remote_workers

    def sync_weights(self, policies: Optional[List[PolicyID]] = None) -> None:
        """Syncs weights from the local worker to all remote workers."""
        if self.remote_workers():
            weights = ray.put(self.local_worker().get_weights(policies))
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
                config=self._remote_config,
            ) for i in range(num_workers)
        ])

    def reset(self, new_remote_workers: List[ActorHandle]) -> None:
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
        results = self.local_worker().foreach_policy(func)
        ray_gets = []
        for worker in self.remote_workers():
            ray_gets.append(
                worker.apply.remote(lambda w: w.foreach_policy(func)))
        remote_results = ray.get(ray_gets)
        for r in remote_results:
            results.extend(r)
        return results

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
        results = self.local_worker().foreach_trainable_policy(func)
        ray_gets = []
        for worker in self.remote_workers():
            ray_gets.append(
                worker.apply.remote(
                    lambda w: w.foreach_trainable_policy(func)))
        remote_results = ray.get(ray_gets)
        for r in remote_results:
            results.extend(r)
        return results

    @DeveloperAPI
    def foreach_env(self, func: Callable[[BaseEnv], List[T]]) -> List[List[T]]:
        """Apply `func` to all workers' (unwrapped) environments.

        `func` takes a single unwrapped env as arg.

        Args:
            func (Callable[[BaseEnv], T]): A function - taking a BaseEnv
                object as arg and returning a list of return values over envs
                of the worker.

        Returns:
            List[List[T]]: The list (workers) of lists (environments) of
                results.
        """
        local_results = [self.local_worker().foreach_env(func)]
        ray_gets = []
        for worker in self.remote_workers():
            ray_gets.append(worker.foreach_env.remote(func))
        return local_results + ray.get(ray_gets)

    @DeveloperAPI
    def foreach_env_with_context(
            self,
            func: Callable[[BaseEnv, EnvContext], List[T]]) -> List[List[T]]:
        """Apply `func` to all workers' (unwrapped) environments.

        `func` takes a single unwrapped env and the env_context as args.

        Args:
            func (Callable[[BaseEnv], T]): A function - taking a BaseEnv
                object as arg and returning a list of return values over envs
                of the worker.

        Returns:
            List[List[T]]: The list (workers) of lists (environments) of
                results.
        """
        local_results = [self.local_worker().foreach_env_with_context(func)]
        ray_gets = []
        for worker in self.remote_workers():
            ray_gets.append(worker.foreach_env_with_context.remote(func))
        return local_results + ray.get(ray_gets)

    @staticmethod
    def _from_existing(local_worker: RolloutWorker,
                       remote_workers: List[ActorHandle] = None):
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
    ) -> Union[RolloutWorker, ActorHandle]:
        def session_creator():
            logger.debug("Creating TF session {}".format(
                config["tf_session_args"]))
            return tf1.Session(
                config=tf1.ConfigProto(**config["tf_session_args"]))

        def valid_module(class_path):
            if isinstance(class_path, str) and "." in class_path:
                module_path, class_name = class_path.rsplit(".", 1)
                try:
                    spec = importlib.util.find_spec(module_path)
                    if spec is not None:
                        return True
                except (ModuleNotFoundError, ValueError):
                    print(
                        f"module {module_path} not found while trying to get "
                        f"input {class_path}")
            return False

        if isinstance(config["input"], FunctionType):
            input_creator = config["input"]
        elif config["input"] == "sampler":
            input_creator = (lambda ioctx: ioctx.default_sampler_input())
        elif isinstance(config["input"], dict):
            input_creator = (
                lambda ioctx: ShuffledInput(MixedInput(config["input"], ioctx),
                                            config["shuffle_buffer_size"]))
        elif isinstance(config["input"], str) and \
                registry_contains_input(config["input"]):
            input_creator = registry_get_input(config["input"])
        elif "d4rl" in config["input"]:
            env_name = config["input"].split(".")[-1]
            input_creator = (lambda ioctx: D4RLReader(env_name, ioctx))
        elif valid_module(config["input"]):
            input_creator = (lambda ioctx: ShuffledInput(from_config(
                config["input"], ioctx=ioctx)))
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

        # Assert everything is correct in "multiagent" config dict (if given).
        ma_policies = config["multiagent"]["policies"]
        if ma_policies:
            for pid, policy_spec in ma_policies.copy().items():
                assert isinstance(policy_spec, (PolicySpec, list, tuple))
                # Class is None -> Use `policy_cls`.
                if policy_spec.policy_class is None:
                    ma_policies[pid] = ma_policies[pid]._replace(
                        policy_class=policy_cls)
            policies = ma_policies

        # Create a policy_spec (MultiAgentPolicyConfigDict),
        # even if no "multiagent" setup given by user.
        else:
            policies = policy_cls

        if worker_index == 0:
            extra_python_environs = config.get(
                "extra_python_environs_for_driver", None)
        else:
            extra_python_environs = config.get(
                "extra_python_environs_for_worker", None)

        worker = cls(
            env_creator=env_creator,
            validate_env=validate_env,
            policy_spec=policies,
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
            normalize_actions=config["normalize_actions"],
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
