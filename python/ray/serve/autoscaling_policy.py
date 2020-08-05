from abc import ABCMeta, abstractmethod

from ray.serve.utils import logger


class AutoscalingPolicy:
    """Defines the interface for an autoscaling policy.

    To add a new autoscaling policy, a class should be defined that provides
    this interface. The class may be stateful, in which case it may also want
    to provide a non-default constructor. However, this state will be lost when
    the controller recovers from a failure.
    """
    __metaclass__ = ABCMeta

    def __init__(self, config):
        """Initialize the policy using the specified config dictionary."""
        self.config = config

    @abstractmethod
    def scale(self, router_queue_lens, curr_replicas):
        """Make a decision to scale backends.

        Arguments:
            router_queue_lens: Dict[str: int] mapping routers to their most
                recent queue length for this backend.
            curr_replicas: int The number of replicas that the backend
                currently has.

        Returns:
            int The new number of replicas to scale this backend to. Returns -1
                if there should be no change.
        """
        return -1


class BasicAutoscalingPolicy(AutoscalingPolicy):
    """
    TODO
    """

    def __init__(self, backend, config):
        self.backend = backend
        self.scale_up_threshold = config.get("scale_up_threshold", 5)
        self.scale_down_threshold = config.get("scale_down_threshold", 1)
        self.scale_up_num_replicas = config.get("scale_up_num_replicas", 2)
        self.scale_down_num_replicas = config.get("scale_down_num_replicas", 1)

    def scale(self, router_queue_lens, curr_replicas):
        queue_lens = list(router_queue_lens.values())
        if len(queue_lens) == 0:
            return -1

        avg_queue_len = sum(queue_lens) / len(queue_lens)

        # Scale up.
        if avg_queue_len > self.scale_up_threshold:
            decision = curr_replicas + self.scale_up_num_replicas
            logger.info("Increasing number of replicas for backend '{}' "
                        "from {} to {}".format(self.backend, curr_replicas,
                                               decision))
        # Scale down.
        elif avg_queue_len < self.scale_down_threshold and curr_replicas > 1:
            decision = curr_replicas - self.scale_down_num_replicas
            logger.info("Decreasing number of replicas for backend '{}' "
                        "from {} to {}".format(self.backend, curr_replicas,
                                               decision))

        # Do nothing.
        else:
            decision = -1

        return decision
