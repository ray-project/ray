from ray.tests.kuberay.utils import (
    setup_kuberay_operator,
    wait_for_raycluster_crd,
    setup_logging,
)

if __name__ == "__main__":
    setup_logging()
    setup_kuberay_operator()
    wait_for_raycluster_crd()
