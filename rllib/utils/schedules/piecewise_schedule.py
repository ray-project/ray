from ray.rllib.utils.schedules.schedule import Schedule


def _linear_interpolation(l, r, alpha):
    return l + alpha * (r - l)


class PiecewiseSchedule(Schedule):
    def __init__(self,
                 endpoints,
                 framework,
                 interpolation=_linear_interpolation,
                 outside_value=None):
        """
        Args:
            endpoints (List[Tuple[int,float]]): A list of tuples
                `(t, value)` such that the output
                is an interpolation (given by the `interpolation` callable)
                between two values.
                E.g.
                t=400 and endpoints=[(0, 20.0),(500, 30.0)]
                output=20.0 + 0.8 * (30.0 - 20.0) = 28.0
                NOTE: All the values for time must be sorted in an increasing
                order.

            interpolation (callable): A function that takes the left-value,
                the right-value and an alpha interpolation parameter
                (0.0=only left value, 1.0=only right value), which is the
                fraction of distance from left endpoint to right endpoint.

            outside_value (Optional[float]): If t in call to `value` is
                outside of all the intervals in `endpoints` this value is
                returned. If None then an AssertionError is raised when outside
                value is requested.
        """
        super().__init__(framework=framework)

        idxes = [e[0] for e in endpoints]
        assert idxes == sorted(idxes)
        self.interpolation = interpolation
        self.outside_value = outside_value
        self.endpoints = endpoints

    def _value(self, t):
        for (l_t, l), (r_t, r) in zip(self.endpoints[:-1], self.endpoints[1:]):
            if l_t <= t < r_t:
                alpha = float(t - l_t) / (r_t - l_t)
                return self.interpolation(l, r, alpha)

        # t does not belong to any of the pieces, return `self.outside_value`.
        assert self.outside_value is not None
        return self.outside_value
