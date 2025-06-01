import sys

import pytest

from ray._private.test_utils import async_wait_for_condition
from ray.dashboard.modules.job.tests.conftest import (
    _driver_script_path,
    create_job_manager,
    create_ray_cluster,
)
from ray.dashboard.modules.job.tests.test_job_manager import check_job_succeeded


@pytest.mark.asyncio
class TestRuntimeEnvStandalone:
    """NOTE: PLEASE READ CAREFULLY BEFORE MODIFYING
    This test is extracted into a standalone module such that it can bootstrap its own
    (standalone) Ray cluster while avoiding affecting the shared one used by other
    JobManager tests
    """

    @pytest.mark.parametrize(
        "tracing_enabled",
        [
            False,
            # TODO(issues/38633): local code loading is broken when tracing is enabled
            # True,
        ],
    )
    async def test_user_provided_job_config_honored_by_worker(
        self, tracing_enabled, tmp_path
    ):
        """Ensures that the JobConfig instance injected into ray.init in the driver
        script is honored even in case when job is submitted via JobManager.submit_job
        API (involving RAY_JOB_CONFIG_JSON_ENV_VAR being set in child process env)

        """

        if tracing_enabled:
            tracing_startup_hook = (
                "ray.util.tracing.setup_local_tmp_tracing:setup_tracing"
            )
        else:
            tracing_startup_hook = None

        with create_ray_cluster(_tracing_startup_hook=tracing_startup_hook) as cluster:
            job_manager = create_job_manager(cluster, tmp_path)

            driver_script_path = _driver_script_path(
                "check_code_search_path_is_propagated.py"
            )

            job_id = await job_manager.submit_job(
                entrypoint=f"python {driver_script_path}",
                # NOTE: We inject runtime_env in here, but also specify the JobConfig in
                #       the driver script: settings to JobConfig (other than the
                #       runtime_env) passed in via ray.init(...) have to be respected
                #       along with the runtime_env passed from submit_job API
                runtime_env={"env_vars": {"TEST_SUBPROCESS_RANDOM_VAR": "0xDEEDDEED"}},
            )

            await async_wait_for_condition(
                check_job_succeeded, job_manager=job_manager, job_id=job_id
            )

            logs = job_manager.get_job_logs(job_id)

            assert "Code search path is propagated" in logs, logs
            assert "0xDEEDDEED" in logs, logs


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
