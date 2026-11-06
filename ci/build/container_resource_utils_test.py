import io
import unittest
from unittest import mock

from container_resource_utils import (
    get_container_cpu_limit,
    get_container_mem_limit_mb,
    get_system_ram_mb,
    main,
)


class TestResourceUtils(unittest.TestCase):
    def test_get_system_ram_mb_calculates_correctly(self):
        with mock.patch("os.sysconf") as m:
            m.side_effect = lambda k: 4096 if k == "SC_PAGE_SIZE" else 1048576
            # 1048576 pages * 4096 bytes / 1024^2 = 4096 MB
            self.assertEqual(get_system_ram_mb(), 4096)

    def test_get_container_cpu_limit_rounds_up_fractional(self):
        """Ensures container fractional CPUs (e.g., 1.5) round up to 2."""
        with mock.patch("pathlib.Path.exists", return_value=True), mock.patch(
            "pathlib.Path.read_text", return_value="150000 100000"
        ):
            self.assertEqual(get_container_cpu_limit(), 2)

    def test_mem_limit_falls_back_when_unlimited(self):
        """Tests that 'max' or huge values fall back to host system RAM."""
        with mock.patch("pathlib.Path.read_text", return_value="max"), mock.patch(
            "container_resource_utils.get_system_ram_mb", return_value=16384
        ):
            self.assertEqual(get_container_mem_limit_mb(), 16384)

    def test_main_bottleneck_by_ram(self):
        """8GB RAM, 16 CPUs. Reserve 2GB, 3GB/job -> (8-2)/3 = 2 jobs max."""
        with mock.patch(
            "container_resource_utils.get_container_mem_limit_mb", return_value=8192
        ), mock.patch(
            "container_resource_utils.get_container_cpu_limit", return_value=16
        ), mock.patch(
            "sys.argv", ["prog"]
        ), mock.patch(
            "sys.stdout", new=io.StringIO()
        ) as out:
            main()
            self.assertIn("--jobs=2", out.getvalue())

    def test_main_bottleneck_by_cpu(self):
        """32GB RAM, 2 CPUs. RAM allows ~10 jobs, but CPU limits to 2."""
        with mock.patch(
            "container_resource_utils.get_container_mem_limit_mb", return_value=32768
        ), mock.patch(
            "container_resource_utils.get_container_cpu_limit", return_value=2
        ), mock.patch(
            "sys.argv", ["prog"]
        ), mock.patch(
            "sys.stdout", new=io.StringIO()
        ) as out:
            main()
            self.assertIn("--jobs=2", out.getvalue())

    def test_cli_args_override_defaults(self):
        """Verify CLI flags correctly change the calculation."""
        # 8GB RAM, 8 CPUs. Reserve 4GB, 1GB/job -> (8-4)/1 = 4 jobs.
        with mock.patch(
            "container_resource_utils.get_container_mem_limit_mb", return_value=8192
        ), mock.patch(
            "container_resource_utils.get_container_cpu_limit", return_value=8
        ), mock.patch(
            "sys.argv", ["prog", "--reserve-mb", "4096", "--mb-per-job", "1024"]
        ), mock.patch(
            "sys.stdout", new=io.StringIO()
        ) as out:
            main()
            self.assertIn("--jobs=4", out.getvalue())


if __name__ == "__main__":
    unittest.main()
