class WorkerPlacementStrategy:
    """Base class for implementing different worker placement strategies."""


class ColocatedStrategy(WorkerPlacementStrategy):
    """Ensures that the workers are exactly balanced across all hosts.

    Creates 1 bundle for each host.
    Each bundle will be allocated resources for N workers.
    Bundles are STRICT_SPREAD across nodes.
    """


class WorkerBundleStrategy(WorkerPlacementStrategy):
    """Base class for placement strategy where each worker is a bundle."""


class SpreadWorkerStrategy(WorkerBundleStrategy):
    """Ensures that the workers are balanced across all hosts.

    Creates 1 bundle for each worker.
    Each bundle will be allocated resources for 1 worker.
    Bundles are SPREAD across nodes.
    """


class PackWorkerStrategy(WorkerBundleStrategy):
    """Packs workers together but does not guarantee balanced hosts.

    Creates 1 bundle for each worker.
    Each bundle will be allocated resources for 1 worker.
    Bundles are PACKed onto nodes.
    """
