from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.schedules.schedule import Schedule

tf = try_import_tf()


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
        self.endpoints = [(int(e[0]), float(e[1])) for e in endpoints]

    @override(Schedule)
    def _value(self, t):
        # Find t in our list of endpoints.
        for (l_t, l), (r_t, r) in zip(self.endpoints[:-1], self.endpoints[1:]):
            # When found, return an interpolation (default: linear).
            if l_t <= t < r_t:
                alpha = float(t - l_t) / (r_t - l_t)
                return self.interpolation(l, r, alpha)

        # t does not belong to any of the pieces, return `self.outside_value`.
        assert self.outside_value is not None
        return self.outside_value

    @override(Schedule)
    def _tf_value_op(self, t):
        assert self.outside_value is not None, \
            "tf-version of PiecewiseSchedule requires `outside_value` to be " \
            "provided!"

        endpoints = tf.cast(
            tf.stack([e[0] for e in self.endpoints] + [-1]), tf.int32)

        # Create all possible interpolation results.
        results_list = []
        for (l_t, l), (r_t, r) in zip(self.endpoints[:-1], self.endpoints[1:]):
            alpha = tf.cast(t - l_t, tf.float32) / \
                    tf.cast(r_t - l_t, tf.float32)
            results_list.append(self.interpolation(l, r, alpha))
        # If t does not belong to any of the pieces, return `outside_value`.
        results_list.append(self.outside_value)
        results_list = tf.stack(results_list)

        # Return correct results tensor depending on where we find t.
        def _cond(i, x):
            return tf.logical_not(
                tf.logical_or(
                    tf.equal(endpoints[i + 1], -1),
                    tf.logical_and(endpoints[i] <= x, x < endpoints[i + 1])))

        def _body(i, x):
            return (i + 1, t)

        idx_and_t = tf.while_loop(_cond, _body,
                                  [tf.constant(0, dtype=tf.int32), t])
        return results_list[idx_and_t[0]]
