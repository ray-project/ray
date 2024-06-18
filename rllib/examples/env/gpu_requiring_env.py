import ray
from ray.rllib.examples.env.simple_corridor import SimpleCorridor


class ACCRequiringEnv(SimpleCorridor):
    """A dummy env that requires a ACC in order to work.

    The env here is a simple corridor env that additionally simulates a ACC
    check in its constructor via `ray.get_acc_ids()`. If this returns an
    empty list, we raise an error.

    To make this env work, use `num_accs_per_worker > 0` (RolloutWorkers
    requesting this many ACCs each) and - maybe - `num_accs > 0` in case
    your local worker/driver must have an env as well. However, this is
    only the case if `create_env_on_driver`=True (default is False).
    """

    def __init__(self, config=None):
        super().__init__(config)

        # Fake-require some ACCs (at least one).
        # If your local worker's env (`create_env_on_driver`=True) does not
        # necessarily require a ACC, you can perform the below assertion only
        # if `config.worker_index != 0`.
        accs_available = ray.get_acc_ids()
        assert len(accs_available) > 0, "Not enough ACCs for this env!"
        print("Env can see these ACCs: {}".format(accs_available))
