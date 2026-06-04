from typing import Optional

from ray.rllib.utils.metrics import WEIGHTS_SEQ_NO
from ray.rllib.utils.typing import StateDict
from ray.util.annotations import DeveloperAPI


@DeveloperAPI(stability="alpha")
class EnvRunnerStateServer:
    """A single, global parameter-server-style actor holding the latest EnvRunner state.

    This implements the "pull" half of the EnvRunner state synchronization used by the
    async algorithms (IMPALA/APPO) when ``config.use_env_runner_state_server=True``.

    Instead of the Algorithm broadcasting (pushing) weights to every EnvRunner - a path
    that, under back-pressure (``max_requests_in_flight_per_env_runner``), silently drops
    newer weight broadcasts while an EnvRunner is busy in a long ``sample()`` call and
    thus leaves EnvRunners training on stale weights - the Algorithm pushes the merged
    state to exactly one of these servers, and every EnvRunner pulls the latest state at
    the top of each ``sample()`` call. This guarantees EnvRunners always act with the
    freshest available weights.

    The held state is the same plain ``StateDict`` that
    :py:meth:`~ray.rllib.env.env_runner_group.EnvRunnerGroup.sync_env_runner_states`
    assembles, i.e. it contains (keys are the canonical RLlib component/metric strings):

    - ``COMPONENT_RL_MODULE``: a ``ray.ObjectRef`` pointing to the RLModule weights dict
      (kept as an ObjectRef on purpose - an unchanged pull is then a cheap, local
      ``ray.get`` on the EnvRunner side; the weight tensors only transfer when the
      version actually advances).
    - ``WEIGHTS_SEQ_NO``: the integer weights sequence number. This doubles as the
      state's *version*: EnvRunners apply a pulled state only if its ``WEIGHTS_SEQ_NO``
      is greater than the one they already hold.
    - ``COMPONENT_ENV_TO_MODULE_CONNECTOR`` / ``COMPONENT_MODULE_TO_ENV_CONNECTOR``: the
      merged connector states.
    - ``NUM_ENV_STEPS_SAMPLED_LIFETIME``: the global env-steps-sampled counter.

    This actor must be created with ``max_concurrency > 1`` (see
    ``config.env_runner_state_server_max_concurrency``) so that many EnvRunners can
    ``pull`` concurrently without serializing behind each other. ``push`` only ever
    rebinds the stored reference (never mutates in place), so concurrent reads always
    observe a coherent, non-torn state and no lock is required.

    The actor holds purely *derived* state (reconstructable from the Learner weights),
    so it is created with ``max_restarts=-1`` and the Algorithm keeps a backup of the
    last pushed state; on a restart the Algorithm re-pushes that backup. The server is
    therefore not a single point of failure.
    """

    def __init__(self):
        self._state: Optional[StateDict] = None

    def push(self, state: StateDict) -> None:
        """Stores the latest EnvRunner state (called by the Algorithm, once per sync)."""
        # Atomic rebind (no in-place mutation) -> safe for concurrent `pull`s.
        self._state = state

    def pull(self) -> Optional[StateDict]:
        """Returns the latest stored state, or None if nothing has been pushed yet.

        The returned state contains the RLModule weights as a ``ray.ObjectRef``; it is
        intentionally NOT dereferenced here - the calling EnvRunner does a (cheap, often
        local) ``ray.get`` only when it decides to actually apply the state.
        """
        return self._state

    def get_version(self) -> int:
        """Returns the ``WEIGHTS_SEQ_NO`` of the stored state, or -1 if empty.

        Used by the Algorithm to cheaply detect a restarted (empty) server and re-push
        its backup, without transferring the full state.
        """
        if self._state is None:
            return -1
        return self._state.get(WEIGHTS_SEQ_NO, -1)
