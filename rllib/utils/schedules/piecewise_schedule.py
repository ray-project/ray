from typing import Callable, List, Optional, Tuple

from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.schedules.schedule import Schedule
from ray.rllib.utils.typing import TensorType

tf1, tf, tfv = try_import_tf()


def _linear_interpolation(left, right, alpha):
    return left + alpha * (right - left)


@PublicAPI
class PiecewiseSchedule(Schedule):
    def __init__(
        self,
        endpoints: List[Tuple[int, float]],
        framework: Optional[str] = None,
        interpolation: Callable[
            [TensorType, TensorType, TensorType], TensorType
        ] = _linear_interpolation,
        outside_value: Optional[float] = None,
    ):
        """Initializes a PiecewiseSchedule instance.

        Args:
            endpoints: A list of tuples
                `(t, value)` such that the output
                is an interpolation (given by the `interpolation` callable)
                between two values.
                E.g.
                t=400 and endpoints=[(0, 20.0),(500, 30.0)]
                output=20.0 + 0.8 * (30.0 - 20.0) = 28.0
                NOTE: All the values for time must be sorted in an increasing
                order.
            framework: The framework descriptor string, e.g. "tf",
                "torch", or None.
            interpolation: A function that takes the left-value,
                the right-value and an alpha interpolation parameter
                (0.0=only left value, 1.0=only right value), which is the
                fraction of distance from left endpoint to right endpoint.
            outside_value: If t in call to `value` is
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
    def _value(self, t: TensorType) -> TensorType:
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
    def _tf_value_op(self, t: TensorType) -> TensorType:
        assert self.outside_value is not None, (
            "tf-version of PiecewiseSchedule requires `outside_value` to be "
            "provided!"
        )

        endpoints = tf.cast(tf.stack([e[0] for e in self.endpoints] + [-1]), tf.int64)

        # Create all possible interpolation results.
        results_list = []
        for (l_t, l), (r_t, r) in zip(self.endpoints[:-1], self.endpoints[1:]):
            alpha = tf.cast(t - l_t, tf.float32) / tf.cast(r_t - l_t, tf.float32)
            results_list.append(self.interpolation(l, r, alpha))
        # If t does not belong to any of the pieces, return `outside_value`.
        results_list.append(self.outside_value)
        results_list = tf.stack(results_list)

        # Return correct results tensor depending on where we find t.
        def _cond(i, x):
            x = tf.cast(x, tf.int64)
            return tf.logical_not(
                tf.logical_or(
                    tf.equal(endpoints[i + 1], -1),
                    tf.logical_and(endpoints[i] <= x, x < endpoints[i + 1]),
                )
            )

        def _body(i, x):
            return (i + 1, t)

        idx_and_t = tf.while_loop(_cond, _body, [tf.constant(0, dtype=tf.int64), t])
        return results_list[idx_and_t[0]]
