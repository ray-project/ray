.. note::

    Ray 2.10.0 introduces the alpha stage of RLlib's "new API stack".
    The Ray Team plans to transition algorithms, example scripts, and documentation to the new code base
    thereby incrementally replacing the "old API stack" (e.g., ModelV2, Policy, RolloutWorker) throughout the subsequent minor releases leading up to Ray 3.0.
    algorithms, example scripts, and the documentation over and into this new code base

    Note, however, that so far only PPO (single- and multi-agent) and SAC (single-agent only)
    support the "new API stack" (and continue to run by default with the old APIs)
    and that you will be able to continue using your existing custom (old stack) classes
    and setups for the foreseeable future.

    `Click here </rllib/package_ref/rllib-new-api-stack.html>`__ for more details on how to use the new API stack.