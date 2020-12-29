import sys
from ray.tests.conftest import *  # noqa


@pytest.fixture
def ray_start_forcibly(request):
    param = getattr(request, "param", {})
    with ray_start(**param) as res:
        yield res


@pytest.fixture
def stop_ray(request):
    yield
    if ray.is_initialized:
        print("shutdown ray")
        ray.shutdown()


@contextmanager
def ray_start(**kwargs):
    init_kwargs = get_default_fixture_ray_kwargs()
    init_kwargs["num_cpus"] = 32
    init_kwargs.update(kwargs)
    if ray.is_initialized():
        print("Ray is already started, shutdown ray first")
        ray.shutdown()
    # Start the Ray processes.
    address_info = ray.init(**init_kwargs, job_config=ray.job_config.JobConfig(
            code_search_path=sys.path))
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
