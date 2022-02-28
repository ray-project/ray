import copy
from typing import Optional

from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.typing import EnvConfigDict


@PublicAPI
class EnvContext(dict):
    """Wraps env configurations to include extra rllib metadata.

    These attributes can be used to parameterize environments per process.
    For example, one might use `worker_index` to control which data file an
    environment reads in on initialization.

    RLlib auto-sets these attributes when constructing registered envs.
    """

    def __init__(
        self,
        env_config: EnvConfigDict,
        worker_index: int,
        vector_index: int = 0,
        remote: bool = False,
        num_workers: Optional[int] = None,
    ):
        """Initializes an EnvContext instance.

        Args:
            env_config: The env's configuration defined under the
                "env_config" key in the Trainer's config.
            worker_index: When there are multiple workers created, this
                uniquely identifies the worker the env is created in.
                0 for local worker, >0 for remote workers.
            num_workers: The total number of (remote) workers in the set.
                0 if only a local worker exists.
            vector_index: When there are multiple envs per worker, this
                uniquely identifies the env index within the worker.
                Starts from 0.
            remote: Whether individual sub-environments (in a vectorized
                env) should be @ray.remote actors or not.
        """
        # Store the env_config in the (super) dict.
        dict.__init__(self, env_config)

        # Set some metadata attributes.
        self.worker_index = worker_index
        self.vector_index = vector_index
        self.remote = remote
        self.num_workers = num_workers

    def copy_with_overrides(
        self,
        env_config: Optional[EnvConfigDict] = None,
        worker_index: Optional[int] = None,
        vector_index: Optional[int] = None,
        remote: Optional[bool] = None,
        num_workers: Optional[int] = None,
    ) -> "EnvContext":
        """Returns a copy of this EnvContext with some attributes overridden.

        Args:
            env_config: Optional env config to use. None for not overriding
                the one from the source (self).
            worker_index: Optional worker index to use. None for not
                overriding the one from the source (self).
            vector_index: Optional vector index to use. None for not
                overriding the one from the source (self).
            remote: Optional remote setting to use. None for not overriding
                the one from the source (self).
            num_workers: Optional num_workers to use. None for not overriding
                the one from the source (self).

        Returns:
            A new EnvContext object as a copy of self plus the provided
            overrides.
        """
        return EnvContext(
            copy.deepcopy(env_config) if env_config is not None else self,
            worker_index if worker_index is not None else self.worker_index,
            vector_index if vector_index is not None else self.vector_index,
            remote if remote is not None else self.remote,
            num_workers if num_workers is not None else self.num_workers,
        )

    def set_defaults(self, defaults: dict) -> None:
        """Sets missing keys of self to the values given in `defaults`.

        If `defaults` contains keys that already exist in self, don't override
        the values with these defaults.

        Args:
            defaults: The key/value pairs to add to self, but only for those
                keys in `defaults` that don't exist yet in self.

        Examples:
             >>> env_ctx = EnvContext({"a": 1, "b": 2}, worker_index=0)
             >>> env_ctx.set_defaults({"a": -42, "c": 3})
             >>> print(env_ctx)
             ... {"a": 1, "b": 2, "c": 3}
        """
        for key, value in defaults.items():
            if key not in self:
                self[key] = value

    def __str__(self):
        return (
            super().__str__()[:-1]
            + f", worker={self.worker_index}/{self.num_workers}, "
            f"vector_idx={self.vector_index}, remote={self.remote}" + "}"
        )
