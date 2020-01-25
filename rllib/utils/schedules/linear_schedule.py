from ray.rllib.utils.schedules.polynomial_schedule import PolynomialSchedule


class LinearSchedule(PolynomialSchedule):
    """
    Linear interpolation between `initial_p` and `final_p`. Simply
    uses Polynomial with power=1.0.
    
    final_p + (from_ - to_) * (1 - `t`/t_max)
    """
    def __init__(self, **kwargs):
        super().__init__(power=1.0, **kwargs)
