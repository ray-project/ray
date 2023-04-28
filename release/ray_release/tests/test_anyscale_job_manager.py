import tempfile
import os
from ray_release.util import ERROR_LOG_PATTERNS
from ray_release.job_manager.anyscale_job_manager import AnyscaleJobManager


def test_get_ray_error_logs():
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "log01"), "w") as f:
            f.writelines(ERROR_LOG_PATTERNS[:1])
        with open(os.path.join(tmpdir, "log02"), "w") as f:
            f.writelines(ERROR_LOG_PATTERNS + ["haha"])
        with open(os.path.join(tmpdir, "job-driver-w00t"), "w") as f:
            f.writelines("w00t")
        (
            job_driver_log,
            ray_error_log,
        ) = AnyscaleJobManager._find_job_driver_and_ray_error_logs(tmpdir)
        assert ray_error_log == "".join(ERROR_LOG_PATTERNS + ["haha"])
        assert job_driver_log == "w00t"
