import json
import os
import subprocess
import sys
import warnings

import pytest

import ray.train
import ray.tune
from ray.train.constants import ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.util.annotations import RayDeprecationWarning


@pytest.fixture(autouse=True)
def enable_v2_migration_deprecation_messages(monkeypatch):
    monkeypatch.setenv(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR, "1")
    yield
    monkeypatch.delenv(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR)


def test_trainer_restore():
    with pytest.warns(RayDeprecationWarning, match="restore"):
        try:
            DataParallelTrainer.restore("dummy")
        except Exception:
            pass

    with pytest.warns(RayDeprecationWarning, match="can_restore"):
        try:
            DataParallelTrainer.can_restore("dummy")
        except Exception:
            pass


@pytest.mark.parametrize("migration_warnings_enabled", ["0", "1"])
def test_trainer_restore_warns_once_unconditionally(
    monkeypatch, migration_warnings_enabled
):
    """`restore`/`can_restore` warn exactly once, regardless of the migration flag."""
    monkeypatch.setenv(ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR, migration_warnings_enabled)

    for api in (DataParallelTrainer.restore, DataParallelTrainer.can_restore):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                api("dummy")
            except Exception:
                pass

        deprecations = [
            w for w in caught if issubclass(w.category, RayDeprecationWarning)
        ]
        assert len(deprecations) == 1, [str(w.message) for w in deprecations]
        assert "restore" in str(deprecations[0].message)


def test_trainer_valid_configs(ray_start_4_cpus, tmp_path):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        DataParallelTrainer(
            lambda _: None,
            scaling_config=ray.train.ScalingConfig(num_workers=1),
            run_config=ray.train.RunConfig(
                storage_path=tmp_path,
                failure_config=ray.train.FailureConfig(max_failures=1),
            ),
        ).fit()

    for warning in w:
        assert not (
            warning.category == RayDeprecationWarning
            and "`RunConfig` class should be imported from `ray.tune`"
            in str(warning.message)
        )


def test_trainer_deprecated_configs():
    with pytest.warns(RayDeprecationWarning, match="metadata"):
        DataParallelTrainer(
            lambda _: None,
            metadata={"dummy": "dummy"},
        )

    with pytest.warns(RayDeprecationWarning, match="resume_from_checkpoint"):
        DataParallelTrainer(
            lambda _: None,
            resume_from_checkpoint=ray.train.Checkpoint.from_directory("dummy"),
        )

    with pytest.warns(RayDeprecationWarning, match="fail_fast"):
        DataParallelTrainer(
            lambda _: None,
            run_config=ray.train.RunConfig(
                failure_config=ray.train.FailureConfig(fail_fast=True)
            ),
        )

    with pytest.warns(RayDeprecationWarning, match="trainer_resources"):
        DataParallelTrainer(
            lambda _: None,
            scaling_config=ray.train.ScalingConfig(trainer_resources={"CPU": 1}),
        )

    with pytest.warns(RayDeprecationWarning, match="verbose"):
        DataParallelTrainer(
            lambda _: None,
            run_config=ray.train.RunConfig(verbose=True),
        )

    with pytest.warns(RayDeprecationWarning, match="log_to_file"):
        DataParallelTrainer(
            lambda _: None,
            run_config=ray.train.RunConfig(log_to_file=True),
        )

    with pytest.warns(RayDeprecationWarning, match="stop"):
        DataParallelTrainer(
            lambda _: None,
            run_config=ray.train.RunConfig(stop={"training_iteration": 1}),
        )

    with pytest.warns(RayDeprecationWarning, match="callbacks"):
        DataParallelTrainer(
            lambda _: None,
            run_config=ray.train.RunConfig(callbacks=[ray.tune.Callback()]),
        )

    with pytest.warns(RayDeprecationWarning, match="progress_reporter"):
        DataParallelTrainer(
            lambda _: None,
            run_config=ray.train.RunConfig(
                progress_reporter=ray.tune.ProgressReporter()
            ),
        )

    with pytest.warns(RayDeprecationWarning, match="sync_config"):
        DataParallelTrainer(
            lambda _: None,
            run_config=ray.train.RunConfig(
                sync_config=ray.train.SyncConfig(sync_artifacts=True)
            ),
        )


def test_train_context_deprecations(ray_start_4_cpus, tmp_path):
    def train_fn_per_worker(config):
        with pytest.warns(RayDeprecationWarning, match="get_trial_dir"):
            ray.train.get_context().get_trial_dir()

        with pytest.warns(RayDeprecationWarning, match="get_trial_id"):
            ray.train.get_context().get_trial_id()

        with pytest.warns(RayDeprecationWarning, match="get_trial_name"):
            ray.train.get_context().get_trial_name()

        with pytest.warns(RayDeprecationWarning, match="get_trial_resources"):
            ray.train.get_context().get_trial_resources()

    trainer = DataParallelTrainer(
        train_fn_per_worker,
        scaling_config=ray.train.ScalingConfig(num_workers=1),
        run_config=ray.train.RunConfig(storage_path=tmp_path),
    )
    trainer.fit()


def test_v2_enabled_error(monkeypatch):
    """Running a V1 Trainer with V2 enabled should raise an error."""
    from ray.train.v2._internal.constants import V2_ENABLED_ENV_VAR

    monkeypatch.setenv(V2_ENABLED_ENV_VAR, "1")

    with pytest.raises(DeprecationWarning, match="Detected use of a deprecated") as e:
        DataParallelTrainer(
            lambda _: None,
            scaling_config=ray.train.ScalingConfig(num_workers=1),
        )

    message = str(e.value)
    assert "Ray Train V1 is deprecated and will be removed in Ray 3.0" in message
    assert "`from ray.train.torch import TorchTrainer`" in message
    assert "https://github.com/ray-project/ray/issues/49454" in message
    # Disabling V2 stops working in Ray 3.0, so it isn't offered as a fix.
    assert V2_ENABLED_ENV_VAR not in message


def _run_script(tmp_path, code: str, v2_enabled: str) -> str:
    """Runs ``code`` from a file in a new process and returns its stdout.

    ``ray.train`` reads ``RAY_TRAIN_V2_ENABLED`` on import, so tests that set it
    run in a new process. The script is a file so that warnings can point at it.
    """
    from ray.train.v2._internal.constants import V2_ENABLED_ENV_VAR

    script = tmp_path / "user_script.py"
    script.write_text(code)
    proc = subprocess.run(
        [sys.executable, str(script)],
        env={**os.environ, V2_ENABLED_ENV_VAR: v2_enabled},
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr[-5000:]
    return proc.stdout


_RECORD_SYNC_CONFIG_WARNINGS = """
import json
import warnings

from ray.util.annotations import RayDeprecationWarning

def sync_config_warnings(record):
    return [
        {"message": str(w.message), "filename": w.filename}
        for w in record
        if issubclass(w.category, RayDeprecationWarning)
        and "SyncConfig" in str(w.message)
    ]
"""


@pytest.mark.parametrize("access", ["attribute", "from_import"])
def test_sync_config_warns_with_v2(tmp_path, access):
    """With Train V2, `ray.train.SyncConfig` warns and points users at Ray Tune."""
    lookup = {
        "attribute": "obj = ray.train.SyncConfig",
        "from_import": "from ray.train import SyncConfig as obj",
    }[access]
    code = _RECORD_SYNC_CONFIG_WARNINGS + (
        "import ray.train\n"
        "import ray.train._internal.syncer\n"
        "with warnings.catch_warnings(record=True) as record:\n"
        "    warnings.simplefilter('always')\n"
        f"    {lookup}\n"
        "print(json.dumps({\n"
        "    'warnings': sync_config_warnings(record),\n"
        "    'same_class': obj is ray.train._internal.syncer.SyncConfig,\n"
        "    'module': obj.__module__,\n"
        "    'in_all': 'SyncConfig' in ray.train.__all__,\n"
        "    'in_dir': 'SyncConfig' in dir(ray.train),\n"
        "}))\n"
    )
    out = json.loads(_run_script(tmp_path, code, v2_enabled="1"))

    assert out["same_class"]
    # Pickles of the class must not resolve the deprecated alias.
    assert out["module"] == "ray.train._internal.syncer"
    assert not out["in_all"]
    assert out["in_dir"]
    # `from ... import` looks the name up twice (`hasattr`, then `getattr`).
    assert out["warnings"]
    for warning in out["warnings"]:
        message = warning["message"]
        assert "`ray.train.SyncConfig` is deprecated" in message
        assert "removed in Ray 3.0" in message
        assert "`ray.tune.SyncConfig` with Ray Tune" in message
        assert "https://github.com/ray-project/ray/issues/49454" in message
        # The warning points at the user's line.
        assert warning["filename"] == str(tmp_path / "user_script.py")


def test_sync_config_star_import_does_not_warn_with_v2(tmp_path):
    code = _RECORD_SYNC_CONFIG_WARNINGS + (
        "with warnings.catch_warnings(record=True) as record:\n"
        "    warnings.simplefilter('always')\n"
        "    from ray.train import *  # noqa: F403\n"
        "print(json.dumps(sync_config_warnings(record)))\n"
    )
    assert json.loads(_run_script(tmp_path, code, v2_enabled="1")) == []


def test_sync_config_does_not_warn_with_v1(tmp_path):
    """With Train V1, the `import ray.train` warning covers `SyncConfig` already."""
    code = _RECORD_SYNC_CONFIG_WARNINGS + (
        "import ray.train\n"
        "with warnings.catch_warnings(record=True) as record:\n"
        "    warnings.simplefilter('always')\n"
        "    from ray.train import SyncConfig\n"
        "    ray.train.SyncConfig(sync_artifacts=True)\n"
        "print(json.dumps({\n"
        "    'warnings': sync_config_warnings(record),\n"
        "    'module': SyncConfig.__module__,\n"
        "    'in_all': 'SyncConfig' in ray.train.__all__,\n"
        "}))\n"
    )
    out = json.loads(_run_script(tmp_path, code, v2_enabled="0"))
    assert out == {"warnings": [], "module": "ray.train", "in_all": True}


def test_tune_sync_config_is_drop_in():
    """`ray.tune.SyncConfig`, the replacement, accepts everything the old class does."""
    import dataclasses

    from ray.train._internal.syncer import SyncConfig as TrainSyncConfig

    assert issubclass(ray.tune.SyncConfig, TrainSyncConfig)
    assert [f.name for f in dataclasses.fields(ray.tune.SyncConfig)] == [
        f.name for f in dataclasses.fields(TrainSyncConfig)
    ]
    kwargs = dict(
        sync_period=1,
        sync_timeout=2,
        sync_artifacts=True,
        sync_artifacts_on_checkpoint=False,
    )
    sync_config = ray.tune.SyncConfig(**kwargs)
    assert dataclasses.asdict(sync_config) == dataclasses.asdict(
        TrainSyncConfig(**kwargs)
    )
    assert ray.tune.RunConfig(sync_config=sync_config).sync_config is sync_config


def test_sync_config_pickles_with_v2(tmp_path):
    """Tune experiment state pickled with Train V2 loads without the warning.

    A pickle made with Train V1, which references `ray.train.SyncConfig`, still
    loads with Train V2, and warns.
    """
    dump = (
        "import pickle, sys\n"
        "import ray.tune\n"
        "print(pickle.dumps(ray.tune.RunConfig()).hex())\n"
    )
    load = _RECORD_SYNC_CONFIG_WARNINGS + (
        "import pickle, sys\n"
        "import ray.train._internal.syncer\n"
        "with warnings.catch_warnings(record=True) as record:\n"
        "    warnings.simplefilter('always')\n"
        "    run_config = pickle.loads(bytes.fromhex(sys.stdin.read()))\n"
        "print(json.dumps({\n"
        "    'warnings': len(sync_config_warnings(record)),\n"
        "    'same_class': type(run_config.sync_config)\n"
        "    is ray.train._internal.syncer.SyncConfig,\n"
        "}))\n"
    )
    load_script = tmp_path / "load.py"
    load_script.write_text(load)

    def load_with_v2(pickled: str) -> dict:
        from ray.train.v2._internal.constants import V2_ENABLED_ENV_VAR

        proc = subprocess.run(
            [sys.executable, str(load_script)],
            input=pickled,
            env={**os.environ, V2_ENABLED_ENV_VAR: "1"},
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 0, proc.stderr[-5000:]
        return json.loads(proc.stdout)

    pickled_v2 = _run_script(tmp_path, dump, v2_enabled="1").strip()
    assert load_with_v2(pickled_v2) == {"warnings": 0, "same_class": True}

    pickled_v1 = _run_script(tmp_path, dump, v2_enabled="0").strip().splitlines()[-1]
    out = load_with_v2(pickled_v1)
    assert out["same_class"]
    assert out["warnings"] >= 1


@pytest.mark.parametrize("import_stmt", ["import ray.train", "import ray.tune"])
@pytest.mark.parametrize(
    "v2_enabled,migration_warnings_enabled,expect_warning",
    [("0", "1", True), ("0", "0", False), ("1", "1", False)],
)
@pytest.mark.parametrize("warn_always", [False, True])
def test_v1_enabled_deprecation_warning(
    import_stmt, v2_enabled, migration_warnings_enabled, expect_warning, warn_always
):
    """Importing Ray Train with V1 selected warns exactly once.

    Without ``-W`` this checks that users see the warning by default. With
    ``-W always`` the warnings module doesn't deduplicate, so this checks that a
    second import and a reload don't warn again.
    """
    from ray.train.v2._internal.constants import V2_ENABLED_ENV_VAR

    # `ray.train` is already imported in this process, so import it in a new one.
    code = f"import importlib\n{import_stmt}\nimport ray.train\nimportlib.reload(ray.train)"
    warn_flags = ["-W", "always::DeprecationWarning"] if warn_always else []
    env = {
        **os.environ,
        V2_ENABLED_ENV_VAR: v2_enabled,
        ENABLE_V2_MIGRATION_WARNINGS_ENV_VAR: migration_warnings_enabled,
    }
    proc = subprocess.run(
        [sys.executable, *warn_flags, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    count = proc.stderr.count("Ray Train V1 is deprecated")
    assert count == (1 if expect_warning else 0), proc.stderr
    if expect_warning:
        assert "RayDeprecationWarning" in proc.stderr
        assert "Ray 3.0" in proc.stderr
        assert "https://github.com/ray-project/ray/issues/49454" in proc.stderr


def test_v1_deprecation_warning_skipped_in_workers(ray_start_4_cpus, monkeypatch):
    """Ray worker processes don't repeat the driver's V1 deprecation warning."""
    import ray.train.constants as train_constants

    def count_warnings() -> int:
        train_constants._v1_deprecation_warning_logged = False
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            train_constants._log_v1_deprecation_warning()
        return len([w for w in caught if issubclass(w.category, RayDeprecationWarning)])

    monkeypatch.setattr(train_constants, "_v1_deprecation_warning_logged", False)
    assert count_warnings() == 1
    assert ray.get(ray.remote(count_warnings).remote()) == 0


@pytest.mark.parametrize("api", ["accelerate", "prepare_optimizer", "backward"])
def test_torch_amp_utils_removal_warning(monkeypatch, api):
    """The V1 AMP utilities warn that they are removed in Ray 3.0."""
    import ray.train.torch
    import ray.train.torch.train_loop_utils as train_loop_utils

    # The warning is logged before the accelerator is touched. Stub the
    # accelerator out so that the call doesn't reach the Ray runtime outside a
    # Train worker.
    def no_accelerator(*args, **kwargs):
        raise RuntimeError("no accelerator in this test")

    monkeypatch.setattr(train_loop_utils, "get_accelerator", no_accelerator)
    monkeypatch.setattr(train_loop_utils, "set_accelerator", no_accelerator)
    monkeypatch.setattr(train_loop_utils, "_TorchAccelerator", no_accelerator)

    with pytest.warns(RayDeprecationWarning, match="removed in Ray 3.0") as record:
        with pytest.raises(RuntimeError):
            getattr(ray.train.torch, api)(None)
    assert "https://github.com/ray-project/ray/issues/49454" in str(record[0].message)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
