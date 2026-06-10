from typing import Optional

from ray.rllib.utils.metrics import WEIGHTS_SEQ_NO
from ray.rllib.utils.typing import StateDict
from ray.util.annotations import DeveloperAPI


@DeveloperAPI(stability="alpha")
class EnvRunnerStateServer:
    """A single, global actor holding the latest EnvRunner state for pull-based sync.

    Used by async algorithms (IMPALA/APPO) when
    ``config.use_env_runner_state_server=True``: the Algorithm pushes the merged
    EnvRunner state here once per weight sync, and every EnvRunner pulls it at the top
    of each ``sample()`` call.

    The state is the ``StateDict`` built by
    :py:meth:`~ray.rllib.env.env_runner_group.EnvRunnerGroup.get_merged_env_runner_state`.
    The RLModule weights are kept as a ``ray.ObjectRef`` (so an unchanged pull stays
    cheap) and ``WEIGHTS_SEQ_NO`` is the version EnvRunners compare against.

    Create with ``max_concurrency > 1`` so many EnvRunners can ``pull`` concurrently;
    ``push`` only rebinds the stored reference, so no lock is needed.
    """

    # TODO(Artur): Target state (future PR): make this server the single source of truth
    #  for the *full* EnvRunner state (connectors + weights + counters), with the
    #  Algorithm holding a backup copy for server recreation. That collapses the two
    #  state-assembly paths (`get_merged_env_runner_state` + `sync_env_runner_states`)
    #  into one "build state" step plus a transport choice (sync algos push to workers,
    #  async pull from here), and lets us drop `_dont_auto_sync_env_runner_states` and
    #  the merge/broadcast config knobs. Keep merge on the driver; keep this server dumb.

    def __init__(self):
        self._state: Optional[StateDict] = None

    def push(self, state: StateDict) -> None:
        """Stores the latest EnvRunner state (called once per weight sync)."""
        # Atomic rebind -> safe for concurrent `pull`s.
        self._state = state

    def pull(self) -> Optional[StateDict]:
        """Returns the latest stored state, or None if nothing has been pushed yet."""
        return self._state

    def pull_if_newer(self, weights_seq_no: int) -> Optional[StateDict]:
        """Returns the stored state, but only if it is newer than `weights_seq_no`.

        Lets an EnvRunner do its freshness check in a single round-trip: the (heavy)
        state dict crosses the wire only when there actually is a newer version;
        otherwise this returns None and the caller keeps its current weights. Reads
        ``self._state`` exactly once, so it stays lock-free for concurrent pulls.
        """
        state = self._state
        if state is None or state.get(WEIGHTS_SEQ_NO, -1) <= weights_seq_no:
            return None
        return state

    def get_version(self) -> int:
        """Returns the `WEIGHTS_SEQ_NO` of the stored state, or -1 if empty."""
        if self._state is None:
            return -1
        return self._state.get(WEIGHTS_SEQ_NO, -1)
