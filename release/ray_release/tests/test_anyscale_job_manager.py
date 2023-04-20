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
        assert AnyscaleJobManager._find_ray_error_logs(tmpdir) == "".join(
            ERROR_LOG_PATTERNS + ["haha"]
        )
