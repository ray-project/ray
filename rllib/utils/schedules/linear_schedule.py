from ray.rllib.utils.schedules.polynomial_schedule import PolynomialSchedule


class LinearSchedule(PolynomialSchedule):
    """Linear interpolation between `initial_p` and `final_p`.

    Uses `PolynomialSchedule` with power=1.0.


    The formula is:
    value = `final_p` + (`initial_p` - `final_p`) * (1 - `t`/t_max)
    """

    def __init__(self, **kwargs):
        """Initializes a LinearSchedule instance."""
        super().__init__(power=1.0, **kwargs)
