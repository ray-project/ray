from ray.tests.kuberay.utils import teardown_kuberay_operator, setup_logging

if __name__ == "__main__":
    setup_logging()
    teardown_kuberay_operator()
