.. note::

    With Ray 2.10.0, we announce that RLlib's "new API stack" has entered alpha stage.
    Throughout the next minor releases - up to Ray 3 - we will move more
    algorithms, example scripts, and the documentation over and into this new code base
    thereby slowly replacing the "old API stack" (e.g. ModelV2, Policy, RolloutWorker).

    Note, however, that so far only PPO (single- and multi-agent) and SAC (single-agent only)
    support the "new API stack" (and continue to run by default with the old APIs)
    and that you will be able to continue using your existing custom (old stack) classes
    and setups for the foreseeable future.

    `Click here </rllib/package_ref/rllib-new-api-stack.html>`__ for more details on how to use the new API stack.