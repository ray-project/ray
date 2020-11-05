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
            router_queue_lens (Dict[str, int]): map of routers to their most
                recent queue length of unsent queries for this backend.
            curr_replicas (int): The number of replicas that the backend
                currently has.

        Returns:
            int The new number of replicas to scale this backend to.
        """
        return curr_replicas


class BasicAutoscalingPolicy(AutoscalingPolicy):
    """The default autoscaling policy based on basic thresholds for scaling.

    There is a minimum threshold for the average queue length in the cluster
    to scale up and a maximum threshold to scale down. Each period, a 'scale
    up' or 'scale down' decision is made. This decision must be made for a
    specified number of periods in a row before the number of replicas is
    actually scaled. See config options for more details.
    """

    def __init__(self, backend, config):
        self.backend = backend

        # The minimum number of replicas to scale down to.
        self.min_replicas = config.get("min_replicas", 1)
        # The maximum number of replicas to scale up to. -1 means there is no
        # limit.
        self.max_replicas = config.get("max_replicas", -1)
        if self.max_replicas == -1:
            self.max_replicas = float("inf")
        # The minimum average queue length to trigger scaling up.
        self.scale_up_threshold = config.get("scale_up_threshold", 5)
        # The maximum average queue length to trigger scaling down.
        self.scale_down_threshold = config.get("scale_down_threshold", 1)
        # The number of replicas to be added when scaling up.
        self.scale_up_num_replicas = config.get("scale_up_num_replicas", 2)
        # The number of replicas to be removed when scaling down.
        self.scale_down_num_replicas = config.get("scale_down_num_replicas", 1)
        # The number of consecutive 'scale up' decisions that need to be made
        # before the number of replicas is actually increased.
        self.scale_up_consecutive_periods = config.get(
            "scale_up_consecutive_periods", 2)
        # The number of consecutive 'scale down' decisions that need to be made
        # before the number of replicas is actually decreased.
        self.scale_down_consecutive_periods = config.get(
            "scale_down_consecutive_periods", 5)

        # Keeps track of previous decisions. Each time the load is above
        # 'scale_up_threshold', the counter is incremented and each time it is
        # below 'scale_down_threshold', the counter is decremented. When the
        # load is between the thresholds or a scaling decision is made, the
        # counter is reset to 0.
        self.decision_counter = 0

    def scale(self, router_queue_lens, curr_replicas):
        queue_lens = list(router_queue_lens.values())
        if len(queue_lens) == 0:
            return -1

        new_replicas = curr_replicas
        avg_queue_len = sum(queue_lens) / len(queue_lens)

        # Scale up.
        if avg_queue_len > self.scale_up_threshold:
            # If the previous decision was to scale down (the counter was
            # negative), we reset it and then increment it (set to 1).
            # Otherwise, just increment.
            if self.decision_counter < 0:
                self.decision_counter = 1
            else:
                self.decision_counter += 1

            # Only actually scale the replicas if we've made this decision for
            # 'scale_up_consecutive_periods' in a row.
            if (self.decision_counter >= self.scale_up_consecutive_periods
                    and curr_replicas < self.max_replicas):
                # TODO(edoakes): should we be resetting the counter here?
                self.decision_counter = 0
                new_replicas = min(self.max_replicas,
                                   curr_replicas + self.scale_up_num_replicas)
                logger.info("Increasing number of replicas for backend '{}' "
                            "from {} to {}".format(self.backend, curr_replicas,
                                                   new_replicas))

        # Scale down.
        elif avg_queue_len < self.scale_down_threshold:
            # If the previous decision was to scale up (the counter was
            # positive), reset it to zero before decrementing.
            if self.decision_counter > 0:
                self.decision_counter = -1
            else:
                self.decision_counter -= 1

            # Only actually scale the replicas if we've made this decision for
            # 'scale_down_consecutive_periods' in a row.
            if (self.decision_counter <=
                    -self.scale_down_consecutive_periods + 1
                    and curr_replicas > self.min_replicas):
                # TODO(edoakes): should we be resetting the counter here?
                self.decision_counter = 0
                new_replicas = max(
                    self.min_replicas,
                    curr_replicas - self.scale_down_num_replicas)
                logger.info("Decreasing number of replicas for backend '{}' "
                            "from {} to {}".format(self.backend, curr_replicas,
                                                   new_replicas))

        # Do nothing.
        else:
            self.decision_counter = 0

        return new_replicas
