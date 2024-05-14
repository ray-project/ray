.. note::

    Ray 2.10.0 introduces the alpha stage of RLlib's "new API stack" supporting only PyTorch as DL-framework.
    The Ray Team plans to transition algorithms, example scripts, and documentation to this new code base
    thereby incrementally replacing the "old API stack" (e.g., ModelV2, Policy, RolloutWorker) throughout the subsequent minor releases leading up to Ray 3.0.

    Note, however, that so far only PPO (single- and multi-agent), SAC and DQN (both single-agent only)
    support the "new API stack" and continue to run by default with the old APIs.
    You can continue to use the existing custom (old stack) classes.

    :doc:`See here </rllib/rllib-new-api-stack>` for more details on how to use the new API stack.