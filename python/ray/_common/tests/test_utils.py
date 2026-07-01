"""Tests for Ray utility functions.

This module contains pytest-based tests for utility functions in ray._common.utils.
Test utility classes (SignalActor, Semaphore) are in ray._common.test_utils to
ensure they're included in the Ray package distribution.
"""

import asyncio
import os
import sys
import tempfile
import warnings

import pytest

from ray._common.utils import (
    _BACKGROUND_TASKS,
    env_bool,
    get_cgroup_aware_swap_memory,
    get_or_create_event_loop,
    get_system_memory,
    load_class,
    run_background_task,
    try_to_create_directory,
)

# Optional imports for testing
try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


class TestGetOrCreateEventLoop:
    """Tests for the get_or_create_event_loop utility function."""

    def test_existing_event_loop(self):
        # With running event loop
        expect_loop = asyncio.new_event_loop()
        expect_loop.set_debug(True)
        asyncio.set_event_loop(expect_loop)
        with warnings.catch_warnings():
            # Assert no deprecating warnings raised for python>=3.10.
            warnings.simplefilter("error")
            actual_loop = get_or_create_event_loop()

            assert actual_loop == expect_loop, "Loop should not be recreated."

    def test_new_event_loop(self):
        with warnings.catch_warnings():
            # Assert no deprecating warnings raised for python>=3.10.
            warnings.simplefilter("error")
            loop = get_or_create_event_loop()
            assert loop is not None, "new event loop should be created."


class TestEnvBool:
    """Tests for the env_bool utility function."""

    _KEY = "_RAY_TEST_ENV_BOOL"

    @pytest.mark.parametrize(
        "env_value, expected",
        [
            ("1", True),
            ("true", True),
            ("True", True),
            ("TRUE", True),
            ("0", False),
            ("false", False),
            ("False", False),
            ("FALSE", False),
            ("yes", False),
            ("no", False),
            ("", False),
        ],
    )
    def test_env_bool_values(self, env_value, expected, monkeypatch):
        monkeypatch.setenv(self._KEY, env_value)
        assert env_bool(self._KEY, False) is expected

    def test_env_bool_default_when_unset(self, monkeypatch):
        monkeypatch.delenv(self._KEY, raising=False)
        assert env_bool(self._KEY, False) is False
        assert env_bool(self._KEY, True) is True


@pytest.mark.asyncio
async def test_run_background_task():
    """Test the run_background_task utility function."""
    result = {}

    async def co():
        result["start"] = 1
        await asyncio.sleep(0)
        result["end"] = 1

    run_background_task(co())

    # Background task is running.
    assert len(_BACKGROUND_TASKS) == 1
    # co executed.
    await asyncio.sleep(0)
    # await asyncio.sleep(0) from co is reached.
    await asyncio.sleep(0)
    # co finished and callback called.
    await asyncio.sleep(0)
    # The task should be removed from the set once it finishes.
    assert len(_BACKGROUND_TASKS) == 0

    assert result.get("start") == 1
    assert result.get("end") == 1


class TestTryToCreateDirectory:
    """Tests for the try_to_create_directory utility function."""

    def test_create_new_directory(self):
        """Test creating a new directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_dir = os.path.join(temp_dir, "test_dir")
            try_to_create_directory(test_dir)
            assert os.path.exists(test_dir), "Directory should be created"
            assert os.path.isdir(test_dir), "Path should be a directory"

    def test_existing_directory(self):
        """Test creating a directory that already exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_dir = os.path.join(temp_dir, "existing_dir")
            # Create directory first
            os.makedirs(test_dir)
            # Should work without error
            try_to_create_directory(test_dir)
            assert os.path.exists(test_dir), "Directory should still exist"

    def test_nested_directory_creation(self):
        """Test creating nested directory structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            nested_dir = os.path.join(temp_dir, "nested", "deep", "structure")
            try_to_create_directory(nested_dir)
            assert os.path.exists(nested_dir), "Nested directory should be created"

    def test_tilde_expansion(self):
        """Test directory creation with tilde expansion."""
        with tempfile.TemporaryDirectory() as temp_dir:
            fake_home = os.path.join(temp_dir, "fake_home")
            os.makedirs(fake_home, exist_ok=True)

            # Mock the expanduser for this test
            original_expanduser = os.path.expanduser
            os.path.expanduser = (
                lambda path: path.replace("~", fake_home)
                if path.startswith("~")
                else path
            )

            try:
                tilde_dir = "~/test_tilde_dir"
                try_to_create_directory(tilde_dir)
                expected_path = os.path.join(fake_home, "test_tilde_dir")
                assert os.path.exists(
                    expected_path
                ), "Tilde-expanded directory should be created"
            finally:
                # Restore original expanduser
                os.path.expanduser = original_expanduser


class TestLoadClass:
    """Tests for the load_class utility function."""

    def test_load_builtin_class(self):
        """Test loading a builtin class."""
        list_class = load_class("builtins.list")
        assert list_class is list, "Should load the builtin list class"

    def test_load_module(self):
        """Test loading a module."""
        path_module = load_class("os.path")
        import os.path

        assert path_module is os.path, "Should load os.path module"

    def test_load_function(self):
        """Test loading a function from a module."""
        makedirs_func = load_class("os.makedirs")
        assert makedirs_func is os.makedirs, "Should load os.makedirs function"

    def test_load_standard_library_class(self):
        """Test loading a standard library class."""
        temp_dir_class = load_class("tempfile.TemporaryDirectory")
        assert (
            temp_dir_class is tempfile.TemporaryDirectory
        ), "Should load TemporaryDirectory class"

    def test_load_nested_module_class(self):
        """Test loading a class from a nested module."""
        datetime_class = load_class("datetime.datetime")
        import datetime

        assert (
            datetime_class is datetime.datetime
        ), "Should load datetime.datetime class"

    def test_invalid_path_error(self):
        """Test error handling for invalid paths."""
        with pytest.raises(ValueError, match="valid path like mymodule.provider_class"):
            load_class("invalid")

    def test_nonexistent_module_error(self):
        """Test error handling for nonexistent modules."""
        with pytest.raises((ImportError, ModuleNotFoundError)):
            load_class("nonexistent_module.SomeClass")

    def test_nonexistent_attribute_error(self):
        """Test error handling for nonexistent attributes."""
        with pytest.raises(AttributeError):
            load_class("os.NonexistentClass")


class TestGetSystemMemory:
    """Tests for the get_system_memory utility function."""

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_cgroups_v1_with_low_limit(self):
        """Test cgroups v1 with low memory limit."""
        with tempfile.NamedTemporaryFile("w") as memory_limit_file:
            memory_limit_file.write("1073741824")  # 1GB
            memory_limit_file.flush()
            memory = get_system_memory(
                memory_limit_filename=memory_limit_file.name,
                memory_limit_filename_v2="__does_not_exist__",
            )
            assert memory == 1073741824, "Should return cgroup limit when low"

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_cgroups_v1_with_high_limit(self):
        """Test cgroups v1 with high memory limit (should fallback to psutil)."""
        with tempfile.NamedTemporaryFile("w") as memory_limit_file:
            memory_limit_file.write(str(2**63))  # Very high limit
            memory_limit_file.flush()
            psutil_memory = psutil.virtual_memory().total
            memory = get_system_memory(
                memory_limit_filename=memory_limit_file.name,
                memory_limit_filename_v2="__does_not_exist__",
            )
            assert (
                memory == psutil_memory
            ), "Should fallback to psutil when cgroup limit is very high"

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_cgroups_v2_with_limit(self):
        """Test cgroups v2 with memory limit set."""
        with tempfile.NamedTemporaryFile("w") as memory_max_file:
            memory_max_file.write("2147483648\n")  # 2GB with newline
            memory_max_file.flush()
            memory = get_system_memory(
                memory_limit_filename="__does_not_exist__",
                memory_limit_filename_v2=memory_max_file.name,
            )
            assert memory == 2147483648, "Should return cgroups v2 limit"

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_cgroups_v2_unlimited(self):
        """Test cgroups v2 with unlimited memory (max)."""
        with tempfile.NamedTemporaryFile("w") as memory_max_file:
            memory_max_file.write("max")
            memory_max_file.flush()
            psutil_memory = psutil.virtual_memory().total
            memory = get_system_memory(
                memory_limit_filename="__does_not_exist__",
                memory_limit_filename_v2=memory_max_file.name,
            )
            assert (
                memory == psutil_memory
            ), "Should fallback to psutil when cgroups v2 is unlimited"

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_no_cgroup_files(self):
        """Test fallback to psutil when no cgroup files exist."""
        psutil_memory = psutil.virtual_memory().total
        memory = get_system_memory(
            memory_limit_filename="__does_not_exist__",
            memory_limit_filename_v2="__also_does_not_exist__",
        )
        assert memory == psutil_memory, "Should use psutil when no cgroup files exist"


@pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
class TestGetCgroupAwareSwapMemory:
    """Tests for the get_cgroup_aware_swap_memory helper.

    The contract: when any cgroup branch is taken, *all* fields returned are
    cgroup-scoped (host psutil values are never mixed into a cgroup result).
    """

    @staticmethod
    def _patch_paths(monkeypatch, tmp_path, **paths):
        """Override module-level cgroup paths to point into tmp_path.

        Any key passed in is set; anything omitted is redirected to a
        guaranteed-nonexistent path under tmp_path so the helper's
        os.path.exists() guards behave as in a system without that file.
        """
        names = (
            "_CGROUP_V2_SWAP_MAX",
            "_CGROUP_V2_SWAP_CURRENT",
            "_CGROUP_V1_MEMSW_LIMIT",
            "_CGROUP_V1_MEMSW_USAGE",
            "_CGROUP_V1_MEM_LIMIT",
            "_CGROUP_V1_MEM_USAGE",
        )
        for name in names:
            target = paths.get(name, str(tmp_path / f"__missing_{name}__"))
            monkeypatch.setattr(f"ray._common.utils.{name}", target)

    @staticmethod
    def _write(tmp_path, name, content):
        path = tmp_path / name
        path.write_text(content)
        return str(path)

    @staticmethod
    def _patch_host_swap(monkeypatch, total, used):
        class _FakeSwap:
            def __init__(self, total, used):
                self.total = total
                self.used = used

        monkeypatch.setattr(
            "ray._common.utils.psutil.swap_memory",
            lambda: _FakeSwap(total, used),
        )

    def test_no_cgroup_files_uses_host_psutil(self, monkeypatch, tmp_path):
        """With no cgroup swap files, the helper returns host psutil values."""
        self._patch_paths(monkeypatch, tmp_path)
        self._patch_host_swap(monkeypatch, total=8 * 1024**3, used=1 * 1024**3)

        total, used = get_cgroup_aware_swap_memory()

        assert total == 8 * 1024**3
        assert used == 1 * 1024**3

    def test_cgroup_v2_numeric_swap_max_with_current(self, monkeypatch, tmp_path):
        """Numeric swap.max + swap.current → both fields cgroup-scoped.

        Host swap is intentionally smaller than the cgroup limit so this test
        also regression-protects against re-introducing a min(cgroup, host)
        clamp; C++ adds the full cgroup_swap_max regardless of host capacity.
        """
        self._patch_paths(
            monkeypatch,
            tmp_path,
            _CGROUP_V2_SWAP_MAX=self._write(tmp_path, "swap.max", "2147483648"),
            _CGROUP_V2_SWAP_CURRENT=self._write(tmp_path, "swap.current", "536870912"),
        )
        self._patch_host_swap(monkeypatch, total=1 * 1024**3, used=99)

        total, used = get_cgroup_aware_swap_memory()

        assert total == 2 * 1024**3
        assert used == 512 * 1024**2

    def test_cgroup_v2_numeric_swap_max_without_current(self, monkeypatch, tmp_path):
        """Regression: numeric swap.max but missing swap.current must NOT
        return host.used — used is cgroup-scoped and unknown, so it's 0."""
        self._patch_paths(
            monkeypatch,
            tmp_path,
            _CGROUP_V2_SWAP_MAX=self._write(tmp_path, "swap.max", "2147483648"),
        )
        self._patch_host_swap(monkeypatch, total=8 * 1024**3, used=7 * 1024**3)

        total, used = get_cgroup_aware_swap_memory()

        assert total == 2 * 1024**3
        assert used == 0

    @pytest.mark.parametrize(
        "swap_max_value",
        [
            "max",
            "garbage",
            "",
            # Numeric but overflows int64 — kernel's "unlimited" sentinel.
            # C++ catches std::out_of_range from std::stoll; Python's int()
            # has arbitrary precision so we guard explicitly against _INT64_MAX.
            "18446744073709551615",
        ],
    )
    def test_cgroup_v2_unlimited_swap_max_falls_back_to_host_swap(
        self, monkeypatch, tmp_path, swap_max_value
    ):
        """When cgroup v2 swap.max is the kernel's "unlimited" sentinel
        ("max", non-numeric, or overflowing int64), the cgroup imposes no
        cap, so the practical swap budget is the host's swap. Used still
        comes from per-cgroup memory.swap.current — host SwapTotal-SwapFree
        would include other workloads' swap and inflate Ray's view (same
        rule the C++ OOM monitor follows)."""
        cgroup_swap_current = 512 * 1024**2  # 512 MiB
        self._patch_paths(
            monkeypatch,
            tmp_path,
            _CGROUP_V2_SWAP_MAX=self._write(tmp_path, "swap.max", swap_max_value),
            _CGROUP_V2_SWAP_CURRENT=self._write(
                tmp_path, "swap.current", str(cgroup_swap_current)
            ),
        )
        # Host swap used (50 GiB) is intentionally far larger than the
        # cgroup's swap.current (512 MiB) so a regression that returned host
        # used here would fail loudly.
        self._patch_host_swap(monkeypatch, total=100 * 1024**3, used=50 * 1024**3)

        total, used = get_cgroup_aware_swap_memory()

        assert total == 100 * 1024**3
        assert used == cgroup_swap_current

    def test_cgroup_v2_swap_disabled_returns_zero(self, monkeypatch, tmp_path):
        """Regression: when swap.max is explicitly 0, the kernel is saying
        "no swap for this cgroup" — different from "unlimited". Helper must
        return (0, 0) and must NOT read swap.current (C++ guards on > 0)."""
        self._patch_paths(
            monkeypatch,
            tmp_path,
            _CGROUP_V2_SWAP_MAX=self._write(tmp_path, "swap.max", "0"),
            _CGROUP_V2_SWAP_CURRENT=self._write(tmp_path, "swap.current", "12345"),
        )
        self._patch_host_swap(monkeypatch, total=100 * 1024**3, used=50 * 1024**3)

        total, used = get_cgroup_aware_swap_memory()

        assert total == 0
        assert used == 0

    def test_cgroup_v1_memsw_with_ram_limit(self, monkeypatch, tmp_path):
        """memsw - mem_limit gives swap-only; same for usage.

        Host swap is intentionally smaller than the derived cgroup swap so
        this test also regression-protects against re-introducing a clamp
        by host swap; C++ never clamps by host capacity.
        """
        self._patch_paths(
            monkeypatch,
            tmp_path,
            _CGROUP_V1_MEMSW_LIMIT=self._write(
                tmp_path, "memsw.limit", str(3 * 1024**3)
            ),
            _CGROUP_V1_MEMSW_USAGE=self._write(
                tmp_path, "memsw.usage", str(2 * 1024**3)
            ),
            _CGROUP_V1_MEM_LIMIT=self._write(tmp_path, "mem.limit", str(2 * 1024**3)),
            _CGROUP_V1_MEM_USAGE=self._write(
                tmp_path, "mem.usage", str(int(1.5 * 1024**3))
            ),
        )
        self._patch_host_swap(monkeypatch, total=512 * 1024**2, used=0)

        total, used = get_cgroup_aware_swap_memory()

        assert total == 1 * 1024**3
        assert used == int(0.5 * 1024**3)

    def test_cgroup_v1_memsw_without_ram_limit_falls_back_to_host_ram(
        self, monkeypatch, tmp_path
    ):
        """When memsw files exist but memory.limit_in_bytes is missing,
        approximate ram_limit with host RAM so the scheduler's view
        (ram_capacity + swap_total) matches C++ GetCGroupMemoryBytes, which
        uses memsw_limit as the combined total in this case."""
        memsw_limit = 6 * 1024**3
        memsw_usage = 4 * 1024**3
        host_ram = 4 * 1024**3
        self._patch_paths(
            monkeypatch,
            tmp_path,
            _CGROUP_V1_MEMSW_LIMIT=self._write(
                tmp_path, "memsw.limit", str(memsw_limit)
            ),
            _CGROUP_V1_MEMSW_USAGE=self._write(
                tmp_path, "memsw.usage", str(memsw_usage)
            ),
            # mem.limit / mem.usage intentionally missing
        )
        self._patch_host_swap(monkeypatch, total=100 * 1024**3, used=50 * 1024**3)

        class _FakeVMem:
            total = host_ram

        monkeypatch.setattr(
            "ray._common.utils.psutil.virtual_memory", lambda: _FakeVMem()
        )

        total, used = get_cgroup_aware_swap_memory()

        # swap_total = memsw_limit - host_ram (the ram_limit proxy).
        assert total == memsw_limit - host_ram
        # ram_usage still missing → swap_used stays at 0.
        assert used == 0

    @pytest.mark.parametrize("variant", ["v2", "v1"])
    def test_cgroup_branch_returns_zero_on_read_error(
        self, monkeypatch, tmp_path, variant
    ):
        """Regression: once a cgroup branch is committed (file exists), a
        read/parse error must return (0, 0) rather than fall through to host
        swap. Earlier `except: pass` silently leaked host values into a
        cgroup-scoped result."""
        if variant == "v2":
            target = self._write(tmp_path, "swap.max", "1024")
            self._patch_paths(monkeypatch, tmp_path, _CGROUP_V2_SWAP_MAX=target)
        else:
            target = self._write(tmp_path, "memsw.limit", "1024")
            usage = self._write(tmp_path, "memsw.usage", "512")
            self._patch_paths(
                monkeypatch,
                tmp_path,
                _CGROUP_V1_MEMSW_LIMIT=target,
                _CGROUP_V1_MEMSW_USAGE=usage,
            )
        self._patch_host_swap(monkeypatch, total=100 * 1024**3, used=50 * 1024**3)

        real_open = open

        def _raising_open(path, *args, **kwargs):
            if str(path) == target:
                raise OSError("simulated read failure")
            return real_open(path, *args, **kwargs)

        monkeypatch.setattr("builtins.open", _raising_open)

        total, used = get_cgroup_aware_swap_memory()

        assert total == 0
        assert used == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
