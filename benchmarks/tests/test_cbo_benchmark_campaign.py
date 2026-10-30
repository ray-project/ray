import importlib.util
import json
import sys
import time
from pathlib import Path


def _load_module(module_name: str, relative_path: str):
    repo_root = Path(__file__).resolve().parents[2]
    module_path = repo_root / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


schema = _load_module(
    "cbo_benchmark_schema",
    "python/ray/data/benchmarks/cbo_benchmark_schema.py",
)
harness = _load_module(
    "cbo_benchmark_harness",
    "python/ray/data/benchmarks/cbo_benchmark_harness.py",
)
campaign = _load_module(
    "cbo_benchmark_campaign",
    "python/ray/data/benchmarks/cbo_benchmark_campaign.py",
)


def test_build_campaign_plan_for_matrix_shards_materializes_support_files(tmp_path):
    spec = campaign.build_campaign_spec_template()
    spec["output_dir"] = str(tmp_path / "campaign_out")
    spec["experiments"]["memory_profile"]["enabled"] = False
    spec["experiments"]["per_rule"]["enabled"] = False
    spec["experiments"]["end_to_end"]["enabled"] = False
    spec["experiments"]["r_value_matrix"] = {
        "enabled": True,
        "row_scales": [1000, 2000],
        "operator_counts": [2],
        "sweep_ratios": [0.3, 0.5],
        "repetitions": 1,
        "subtask_count": 2,
    }

    plan = campaign.build_campaign_plan(spec)
    task_ids = [task["task_id"] for task in plan["tasks"]]

    assert task_ids == [
        "r_value_matrix.run.0",
        "r_value_matrix.run.1",
        "r_value_matrix.finalize",
    ]
    campaign.write_campaign_plan(str(tmp_path / "campaign_plan.json"), plan)

    manifest_path = tmp_path / "campaign_out" / "r_value_matrix" / "manifest.json"
    input_results_path = (
        tmp_path / "campaign_out" / "r_value_matrix" / "input_results.json"
    )
    assert manifest_path.exists()
    assert input_results_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    input_results = json.loads(input_results_path.read_text(encoding="utf-8"))

    assert len(manifest["r_value_matrix"]) == 2
    assert len(input_results) == 2
    assert input_results[0].endswith("shard_000_of_2.json")
    assert "r_value_matrix.finalize" in campaign.render_campaign_plan_summary(plan)


def test_campaign_shard_commands_carry_warmup_choice(tmp_path):
    def _shard_command(warmup, output_dir):
        spec = campaign.build_campaign_spec_template()
        spec["output_dir"] = str(output_dir)
        spec["experiments"]["memory_profile"]["enabled"] = False
        spec["experiments"]["per_rule"]["enabled"] = False
        spec["experiments"]["end_to_end"]["enabled"] = False
        spec["experiments"]["r_value_matrix"] = {
            "enabled": True,
            "row_scales": [1000],
            "operator_counts": [2],
            "sweep_ratios": [0.3, 0.5],
            "repetitions": 1,
            "warmup": warmup,
            "subtask_count": 1,
        }
        plan = campaign.build_campaign_plan(spec)
        return next(
            task for task in plan["tasks"] if task["task_id"] == "r_value_matrix.run.0"
        )["command"]

    # The benchmark CLI enables warmup by default, so "off" must be explicit.
    disabled = _shard_command(False, tmp_path / "off")
    assert "--no-warmup" in disabled
    assert "--warmup" not in disabled

    enabled = _shard_command(True, tmp_path / "on")
    assert "--warmup" in enabled


def test_execute_campaign_plan_finalizes_existing_matrix_results_into_bundle(tmp_path):
    payload0 = schema.build_r_value_matrix_payload(
        [
            {
                "workload_id": "chain_rows1000_ops2",
                "num_rows": 1000,
                "transform_ops": 2,
                "derived_ratio": 0.50,
                "derived_duration_s": 1.4,
                "best_ratio": 0.30,
                "best_duration_s": 1.0,
                "default_ratio": 0.50,
                "default_duration_s": 1.4,
                "r_error": 0.20,
                "operator_count": 4,
                "subtask_index": 0,
                "subtask_count": 2,
            }
        ],
        {
            "row_scales": [1000, 2000],
            "operator_counts": [2],
            "sweep_ratios": [0.3, 0.5],
            "repetitions": 1,
            "subtask_index": 0,
            "subtask_count": 2,
            "selected_workload_ids": ["chain_rows1000_ops2"],
        },
    )
    payload1 = schema.build_r_value_matrix_payload(
        [
            {
                "workload_id": "chain_rows2000_ops2",
                "num_rows": 2000,
                "transform_ops": 2,
                "derived_ratio": 0.34,
                "derived_duration_s": 1.8,
                "best_ratio": 0.30,
                "best_duration_s": 1.6,
                "default_ratio": 0.50,
                "default_duration_s": 2.2,
                "r_error": 0.04,
                "operator_count": 4,
                "subtask_index": 1,
                "subtask_count": 2,
            }
        ],
        {
            "row_scales": [1000, 2000],
            "operator_counts": [2],
            "sweep_ratios": [0.3, 0.5],
            "repetitions": 1,
            "subtask_index": 1,
            "subtask_count": 2,
            "selected_workload_ids": ["chain_rows2000_ops2"],
        },
    )
    shard0_path = tmp_path / "existing" / "shard0.json"
    shard1_path = tmp_path / "existing" / "shard1.json"
    manifest_path = tmp_path / "existing" / "manifest.json"
    shard0_path.parent.mkdir(parents=True, exist_ok=True)
    shard0_path.write_text(json.dumps(payload0), encoding="utf-8")
    shard1_path.write_text(json.dumps(payload1), encoding="utf-8")
    manifest_path.write_text(
        json.dumps(
            {
                "r_value_matrix": [
                    {
                        "experiment_type": "r_value_matrix",
                        "subtask_index": 0,
                        "subtask_count": 2,
                        "item_ids": ["chain_rows1000_ops2"],
                    },
                    {
                        "experiment_type": "r_value_matrix",
                        "subtask_index": 1,
                        "subtask_count": 2,
                        "item_ids": ["chain_rows2000_ops2"],
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    spec = campaign.build_campaign_spec_template()
    spec["output_dir"] = str(tmp_path / "campaign_out")
    spec["experiments"]["memory_profile"]["enabled"] = False
    spec["experiments"]["per_rule"]["enabled"] = False
    spec["experiments"]["end_to_end"]["enabled"] = False
    spec["experiments"]["r_value_matrix"] = {
        "enabled": True,
        "input_results": [str(shard0_path), str(shard1_path)],
        "manifest_path": str(manifest_path),
    }

    plan = campaign.build_campaign_plan(spec)
    assert [task["task_id"] for task in plan["tasks"]] == ["r_value_matrix.finalize"]

    result = campaign.execute_campaign_plan(plan)
    assert result["task_results"][0]["status"] == "completed"

    bundle_manifest_path = (
        tmp_path
        / "campaign_out"
        / "r_value_matrix"
        / "bundle"
        / "bundle_manifest.json"
    )
    community_qa_path = (
        tmp_path
        / "campaign_out"
        / "r_value_matrix"
        / "bundle"
        / "cbo_benchmark_community_qa.md"
    )
    assert bundle_manifest_path.exists()
    assert community_qa_path.exists()

    rerun = campaign.execute_campaign_plan(plan)
    assert rerun["task_results"][0]["status"] == "skipped_existing"


def test_execute_campaign_plan_runs_independent_shards_in_parallel(tmp_path):
    helper_script = tmp_path / "write_after_sleep.py"
    helper_script.write_text(
        "\n".join(
            [
                "import sys",
                "import time",
                "from pathlib import Path",
                "time.sleep(float(sys.argv[2]))",
                "Path(sys.argv[1]).write_text('done', encoding='utf-8')",
            ]
        ),
        encoding="utf-8",
    )

    output0 = tmp_path / "parallel" / "out0.txt"
    output1 = tmp_path / "parallel" / "out1.txt"
    final_output = tmp_path / "parallel" / "final.txt"
    plan = {
        "campaign_name": "parallel-smoke",
        "output_dir": str(tmp_path / "parallel"),
        "executor": {
            "max_parallelism": 2,
            "local_max_parallelism": 2,
            "allow_local_fallback_for_remote_stages": True,
            "targets": [],
            "state_path": str(tmp_path / "parallel" / "execution_state.json"),
        },
        "tasks": [
            {
                "task_id": "run.0",
                "experiment_type": "dummy",
                "stage": "run_shard",
                "description": "parallel task 0",
                "command": [sys.executable, str(helper_script), str(output0), "0.3"],
                "output_path": str(output0),
                "log_path": str(tmp_path / "parallel" / "logs" / "run0.log"),
                "dependency_ids": [],
                "cwd": str(tmp_path),
                "resume_skip_if_output_exists": True,
            },
            {
                "task_id": "run.1",
                "experiment_type": "dummy",
                "stage": "run_shard",
                "description": "parallel task 1",
                "command": [sys.executable, str(helper_script), str(output1), "0.3"],
                "output_path": str(output1),
                "log_path": str(tmp_path / "parallel" / "logs" / "run1.log"),
                "dependency_ids": [],
                "cwd": str(tmp_path),
                "resume_skip_if_output_exists": True,
            },
            {
                "task_id": "finalize",
                "experiment_type": "dummy",
                "stage": "finalize",
                "description": "finalize task",
                "command": [sys.executable, str(helper_script), str(final_output), "0.0"],
                "output_path": str(final_output),
                "log_path": str(tmp_path / "parallel" / "logs" / "finalize.log"),
                "dependency_ids": ["run.0", "run.1"],
                "cwd": str(tmp_path),
                "resume_skip_if_output_exists": True,
            },
        ],
        "support_files": [],
    }

    start = time.perf_counter()
    result = campaign.execute_campaign_plan(plan)
    elapsed = time.perf_counter() - start

    assert len(result["task_results"]) == 3
    assert elapsed < 0.75
    state = json.loads(
        (tmp_path / "parallel" / "execution_state.json").read_text(encoding="utf-8")
    )
    assert state["tasks"]["run.0"]["status"] == "completed"
    assert state["tasks"]["finalize"]["status"] == "completed"


def test_execute_campaign_plan_dispatches_run_shard_to_remote_wrapper(tmp_path):
    helper_script = tmp_path / "remote_helper.py"
    helper_script.write_text(
        "\n".join(
            [
                "import json",
                "import sys",
                "from pathlib import Path",
                "Path(sys.argv[1]).write_text(json.dumps({'ok': True}), encoding='utf-8')",
            ]
        ),
        encoding="utf-8",
    )

    output_path = tmp_path / "remote" / "out.json"
    plan = {
        "campaign_name": "remote-smoke",
        "output_dir": str(tmp_path / "remote"),
        "executor": {
            "max_parallelism": 1,
            "local_max_parallelism": 1,
            "allow_local_fallback_for_remote_stages": False,
            "targets": [
                {
                    "name": "loopback",
                    "capacity": 1,
                    "stages": ["run_shard"],
                    "backend": {
                        "type": "template",
                        "command_template": ["sh", "-lc", "{command}"],
                    },
                }
            ],
            "state_path": str(tmp_path / "remote" / "execution_state.json"),
        },
        "tasks": [
            {
                "task_id": "run.0",
                "experiment_type": "dummy",
                "stage": "run_shard",
                "description": "remote wrapped task",
                "command": [sys.executable, str(helper_script), str(output_path)],
                "output_path": str(output_path),
                "log_path": str(tmp_path / "remote" / "logs" / "run0.log"),
                "dependency_ids": [],
                "cwd": str(tmp_path),
                "resume_skip_if_output_exists": True,
            }
        ],
        "support_files": [],
    }

    result = campaign.execute_campaign_plan(plan)
    assert result["task_results"][0]["assigned_target"] == "loopback"
    state = json.loads(
        (tmp_path / "remote" / "execution_state.json").read_text(encoding="utf-8")
    )
    assert state["tasks"]["run.0"]["assigned_target"] == "loopback"
    assert output_path.exists()


def test_build_backend_command_renders_standard_backends(tmp_path):
    task = {
        "task_id": "r_value_matrix.run.0",
        "command": ["python3", "cbo_benchmark.py", "--r-value-matrix"],
        "cwd": "/workspace/ray",
        "output_path": str(tmp_path / "out.json"),
        "log_path": str(tmp_path / "run.log"),
    }

    ssh_command, _ = campaign.build_backend_command(
        task,
        {
            "name": "ssh-a",
            "backend": {
                "type": "ssh",
                "host": "worker-a",
                "ssh_binary": "ssh",
                "ssh_args": ["-p", "2222"],
                "remote_working_dir": "/remote/ray",
            },
        },
    )
    ray_job_command, _ = campaign.build_backend_command(
        task,
        {
            "name": "ray-job-a",
            "backend": {
                "type": "ray_job",
                "address": "http://127.0.0.1:8265",
                "submission_id_prefix": "phase62",
                "working_dir": "/workspace/ray",
            },
        },
    )
    k8s_command, _ = campaign.build_backend_command(
        task,
        {
            "name": "k8s-a",
            "backend": {
                "type": "k8s_job",
                "image": "python:3.12",
                "namespace": "bench",
                "job_name_prefix": "phase62",
            },
        },
    )

    assert ssh_command[0] == "ssh"
    assert "worker-a" in ssh_command
    assert "cd /remote/ray &&" in ssh_command[-1]
    assert ray_job_command[:4] == ["ray", "job", "submit", "--address"]
    assert "phase62-r-value-matrix-run-0" in " ".join(ray_job_command)
    assert k8s_command[:4] == ["kubectl", "create", "job", "phase62-r-value-matrix-run-0"]


def test_execute_campaign_plan_restores_directory_artifacts_from_remote_backend(tmp_path):
    helper_script = tmp_path / "bundle_helper.py"
    helper_script.write_text(
        "\n".join(
            [
                "import sys",
                "from pathlib import Path",
                "bundle_dir = Path(sys.argv[1])",
                "bundle_dir.mkdir(parents=True, exist_ok=True)",
                "(bundle_dir / 'bundle_manifest.json').write_text('{\"ok\": true}', encoding='utf-8')",
                "(bundle_dir / 'report.md').write_text('# bundle', encoding='utf-8')",
                "print('bundle ready')",
            ]
        ),
        encoding="utf-8",
    )

    bundle_dir = tmp_path / "template_remote" / "bundle"
    bundle_manifest_path = bundle_dir / "bundle_manifest.json"
    plan = {
        "campaign_name": "template-artifact-sync",
        "output_dir": str(tmp_path / "template_remote"),
        "executor": {
            "max_parallelism": 1,
            "local_max_parallelism": 1,
            "allow_local_fallback_for_remote_stages": False,
            "targets": [
                {
                    "name": "template-remote",
                    "capacity": 1,
                    "stages": ["run"],
                    "backend": {
                        "type": "template",
                        "command_template": ["sh", "-lc", "{command}"],
                    },
                }
            ],
            "state_path": str(tmp_path / "template_remote" / "execution_state.json"),
        },
        "tasks": [
            {
                "task_id": "bundle.run",
                "experiment_type": "dummy",
                "stage": "run",
                "description": "remote bundle artifact sync",
                "command": [sys.executable, str(helper_script), str(bundle_dir)],
                "output_path": str(bundle_manifest_path),
                "log_path": str(tmp_path / "template_remote" / "logs" / "run.log"),
                "artifact_paths": [str(bundle_dir)],
                "dependency_ids": [],
                "cwd": str(tmp_path),
                "resume_skip_if_output_exists": True,
            }
        ],
        "support_files": [],
    }

    result = campaign.execute_campaign_plan(plan)

    assert result["task_results"][0]["assigned_target"] == "template-remote"
    assert bundle_manifest_path.exists()
    assert (bundle_dir / "report.md").exists()
    state = json.loads(
        (tmp_path / "template_remote" / "execution_state.json").read_text(
            encoding="utf-8"
        )
    )
    assert state["tasks"]["bundle.run"]["artifacts"][0]["kind"] == "directory"


def test_execute_campaign_plan_collects_ray_job_status_and_logs(tmp_path):
    task_script = tmp_path / "ray_task.py"
    task_script.write_text(
        "\n".join(
            [
                "import sys",
                "from pathlib import Path",
                "Path(sys.argv[1]).write_text('ray job output', encoding='utf-8')",
                "print('ray task logs')",
            ]
        ),
        encoding="utf-8",
    )
    fake_ray = tmp_path / "fake_ray.py"
    fake_ray.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json, subprocess, sys",
                "from pathlib import Path",
                "args = sys.argv[1:]",
                "state_dir = Path(args[args.index('--state-dir') + 1])",
                "state_dir.mkdir(parents=True, exist_ok=True)",
                "clean_args = []",
                "skip_next = False",
                "for arg in args:",
                "    if skip_next:",
                "        skip_next = False",
                "        continue",
                "    if arg == '--state-dir':",
                "        skip_next = True",
                "        continue",
                "    clean_args.append(arg)",
                "args = clean_args",
                "if args[:2] == ['job', 'submit']:",
                "    sid = args[args.index('--submission-id') + 1]",
                "    cmd = args[args.index('--') + 1:]",
                "    result = subprocess.run(cmd, capture_output=True, text=True, check=False)",
                "    payload = {'polls': 0, 'status': 'SUCCEEDED' if result.returncode == 0 else 'FAILED', 'logs': result.stdout + result.stderr, 'returncode': result.returncode}",
                "    (state_dir / f'{sid}.json').write_text(json.dumps(payload), encoding='utf-8')",
                "    print(f'Submitted {sid}')",
                "elif args[:2] == ['job', 'status']:",
                "    sid = args[-1]",
                "    path = state_dir / f'{sid}.json'",
                "    payload = json.loads(path.read_text(encoding='utf-8'))",
                "    payload['polls'] += 1",
                "    path.write_text(json.dumps(payload), encoding='utf-8')",
                "    status = 'RUNNING' if payload['polls'] == 1 else payload['status']",
                "    print(f\"Status for job '{sid}': {status}\")",
                "elif args[:2] == ['job', 'logs']:",
                "    sid = args[-1]",
                "    payload = json.loads((state_dir / f'{sid}.json').read_text(encoding='utf-8'))",
                "    print(payload['logs'], end='')",
                "else:",
                "    raise SystemExit(f'unsupported args: {args}')",
            ]
        ),
        encoding="utf-8",
    )
    fake_ray.chmod(0o755)

    output_path = tmp_path / "ray_job" / "result.txt"
    state_dir = tmp_path / "ray_job" / "state"
    plan = {
        "campaign_name": "ray-job-smoke",
        "output_dir": str(tmp_path / "ray_job"),
        "executor": {
            "max_parallelism": 1,
            "local_max_parallelism": 1,
            "allow_local_fallback_for_remote_stages": False,
            "targets": [
                {
                    "name": "ray-job-target",
                    "capacity": 1,
                    "stages": ["run_shard"],
                    "backend": {
                        "type": "ray_job",
                        "address": "http://127.0.0.1:8265",
                        "ray_binary": str(fake_ray),
                        "ray_args": ["--state-dir", str(state_dir)],
                        "submission_id_prefix": "phase63",
                        "poll_interval_s": 0.01,
                        "timeout_s": 5,
                        "collect_logs": True,
                    },
                }
            ],
            "state_path": str(tmp_path / "ray_job" / "execution_state.json"),
        },
        "tasks": [
            {
                "task_id": "run.0",
                "experiment_type": "dummy",
                "stage": "run_shard",
                "description": "ray job task",
                "command": [sys.executable, str(task_script), str(output_path)],
                "output_path": str(output_path),
                "log_path": str(tmp_path / "ray_job" / "logs" / "run0.log"),
                "dependency_ids": [],
                "cwd": str(tmp_path),
                "resume_skip_if_output_exists": True,
            }
        ],
        "support_files": [],
    }
    result = campaign.execute_campaign_plan(plan)

    assert result["task_results"][0]["backend_type"] == "ray_job"
    assert result["task_results"][0]["backend_handle"] == "phase63-run-0"
    assert result["task_results"][0]["artifacts"]
    assert output_path.read_text(encoding="utf-8") == "ray job output"
    state = json.loads(
        (tmp_path / "ray_job" / "execution_state.json").read_text(encoding="utf-8")
    )
    assert state["tasks"]["run.0"]["status_history"][-1]["status"] == "SUCCEEDED"
    assert state["tasks"]["run.0"]["artifacts"]
    log_text = Path(tmp_path / "ray_job" / "logs" / "run0.log").read_text(
        encoding="utf-8"
    )
    assert "ray task logs" in log_text
    assert "__CBO_ARTIFACT_" not in log_text


def test_execute_campaign_plan_collects_k8s_job_status_logs_and_cleanup(tmp_path):
    task_script = tmp_path / "k8s_task.py"
    task_script.write_text(
        "\n".join(
            [
                "import sys",
                "from pathlib import Path",
                "Path(sys.argv[1]).write_text('k8s job output', encoding='utf-8')",
                "print('k8s task logs')",
            ]
        ),
        encoding="utf-8",
    )
    fake_kubectl = tmp_path / "fake_kubectl.py"
    fake_kubectl.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json, subprocess, sys",
                "from pathlib import Path",
                "args = sys.argv[1:]",
                "state_dir = Path(args[args.index('--state-dir') + 1])",
                "state_dir.mkdir(parents=True, exist_ok=True)",
                "clean_args = []",
                "skip_next = False",
                "for arg in args:",
                "    if skip_next:",
                "        skip_next = False",
                "        continue",
                "    if arg == '--state-dir':",
                "        skip_next = True",
                "        continue",
                "    clean_args.append(arg)",
                "args = clean_args",
                "if args[:2] == ['create', 'job']:",
                "    job_name = args[2]",
                "    cmd = args[args.index('--') + 1:]",
                "    result = subprocess.run(cmd, capture_output=True, text=True, check=False)",
                "    payload = {'polls': 0, 'status': 'SUCCEEDED' if result.returncode == 0 else 'FAILED', 'logs': result.stdout + result.stderr, 'deleted': False}",
                "    (state_dir / f'{job_name}.json').write_text(json.dumps(payload), encoding='utf-8')",
                "    print(f'job.batch/{job_name} created')",
                "elif args[:2] == ['get', 'job']:",
                "    job_name = args[2]",
                "    path = state_dir / f'{job_name}.json'",
                "    payload = json.loads(path.read_text(encoding='utf-8'))",
                "    payload['polls'] += 1",
                "    path.write_text(json.dumps(payload), encoding='utf-8')",
                "    status = {'active': 1} if payload['polls'] == 1 else ({'succeeded': 1, 'conditions': [{'type': 'Complete', 'status': 'True'}]} if payload['status'] == 'SUCCEEDED' else {'failed': 1, 'conditions': [{'type': 'Failed', 'status': 'True'}]})",
                "    print(json.dumps({'status': status}))",
                "elif args[:1] == ['logs'] and len(args) >= 2 and args[1].startswith('job/'):",
                "    job_name = args[1].split('/', 1)[1]",
                "    payload = json.loads((state_dir / f'{job_name}.json').read_text(encoding='utf-8'))",
                "    print(payload['logs'], end='')",
                "elif args[:2] == ['delete', 'job']:",
                "    job_name = args[2]",
                "    path = state_dir / f'{job_name}.json'",
                "    payload = json.loads(path.read_text(encoding='utf-8'))",
                "    payload['deleted'] = True",
                "    path.write_text(json.dumps(payload), encoding='utf-8')",
                "    print(f'job.batch \"{job_name}\" deleted')",
                "else:",
                "    raise SystemExit(f'unsupported args: {args}')",
            ]
        ),
        encoding="utf-8",
    )
    fake_kubectl.chmod(0o755)

    output_path = tmp_path / "k8s_job" / "result.txt"
    state_dir = tmp_path / "k8s_job" / "state"
    plan = {
        "campaign_name": "k8s-job-smoke",
        "output_dir": str(tmp_path / "k8s_job"),
        "executor": {
            "max_parallelism": 1,
            "local_max_parallelism": 1,
            "allow_local_fallback_for_remote_stages": False,
            "targets": [
                {
                    "name": "k8s-target",
                    "capacity": 1,
                    "stages": ["run_shard"],
                    "backend": {
                        "type": "k8s_job",
                        "image": "python:3.12",
                        "kubectl_binary": str(fake_kubectl),
                        "kubectl_args": ["--state-dir", str(state_dir)],
                        "job_name_prefix": "phase63",
                        "poll_interval_s": 0.01,
                        "timeout_s": 5,
                        "collect_logs": True,
                        "cleanup": True,
                    },
                }
            ],
            "state_path": str(tmp_path / "k8s_job" / "execution_state.json"),
        },
        "tasks": [
            {
                "task_id": "run.0",
                "experiment_type": "dummy",
                "stage": "run_shard",
                "description": "k8s job task",
                "command": [sys.executable, str(task_script), str(output_path)],
                "output_path": str(output_path),
                "log_path": str(tmp_path / "k8s_job" / "logs" / "run0.log"),
                "dependency_ids": [],
                "cwd": str(tmp_path),
                "resume_skip_if_output_exists": True,
            }
        ],
        "support_files": [],
    }
    result = campaign.execute_campaign_plan(plan)

    assert result["task_results"][0]["backend_type"] == "k8s_job"
    assert result["task_results"][0]["backend_handle"] == "phase63-run-0"
    assert result["task_results"][0]["artifacts"]
    assert output_path.read_text(encoding="utf-8") == "k8s job output"
    state = json.loads(
        (tmp_path / "k8s_job" / "execution_state.json").read_text(encoding="utf-8")
    )
    assert state["tasks"]["run.0"]["status_history"][-1]["status"] == "SUCCEEDED"
    assert state["tasks"]["run.0"]["artifacts"]
    log_text = Path(tmp_path / "k8s_job" / "logs" / "run0.log").read_text(
        encoding="utf-8"
    )
    assert "k8s task logs" in log_text
    assert "__CBO_ARTIFACT_" not in log_text
    kubectl_state = json.loads(
        (state_dir / "phase63-run-0.json").read_text(encoding="utf-8")
    )
    assert kubectl_state["deleted"] is True
