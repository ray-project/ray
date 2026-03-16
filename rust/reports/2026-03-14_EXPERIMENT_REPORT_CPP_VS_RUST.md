# Experiment Report: C++ GCS vs Rust GCS — RDT Test Comparison

**Date**: March 14, 2026
**Branch**: `cc-to-rust-experimental` (commit `63fe601fa8`)
**Ray Version**: 3.0.0.dev0 (nightly wheel)
**NIXL**: 1.0.0
**PyTorch**: 2.10.0+cu128
**Rust**: 1.94.0
**Instance Type**: g4dn.12xlarge (4x T4 GPUs, us-west-2)
**AMI**: Deep Learning OSS Nvidia Driver AMI GPU PyTorch 2.7 (Ubuntu 22.04) — `ami-0fb0010639c839abe`

## Repository Version

To reproduce this report, check out the exact commit used:

```bash
git clone https://github.com/istoica/ray.git
cd ray
git checkout 63fe601fa8  # cc-to-rust-experimental branch, March 14, 2026
```

Key files at this commit:
- `rust/ray-gcs/src/actor_manager.rs` — Rust GCS actor manager with RegisterActor/CreateActor fixes
- `rust/ray-gcs/` — Full Rust GCS server implementation

## Test Configuration

| Component       | C++ GCS Instance          | Rust GCS Instance          |
|-----------------|---------------------------|----------------------------|
| Instance ID     | i-0aeda82fa4ba0e674       | i-057cc108fefd889b4        |
| IP Address      | 16.145.231.253            | 44.250.246.192             |
| GCS Server      | C++ (default from Ray wheel) | Rust (`RAY_USE_RUST_GCS=1`) |
| Core Worker     | C++ (`_raylet.so`)        | C++ (`_raylet.so`)         |
| Raylet          | C++ (standard)            | C++ (standard)             |
| RDT Store       | Python (standard)         | Python (standard)          |
| RDT Transport   | Python (NIXL + NCCL)      | Python (NIXL + NCCL)       |

## Results

### NIXL Tests (`test_rdt_nixl.py` — 21 tests)

| Test Name                                          | C++ GCS  | Rust GCS |
|----------------------------------------------------|----------|----------|
| test_ray_get_rdt_ref_created_by_actor_task         | PASSED   | PASSED   |
| test_p2p                                           | PASSED   | PASSED   |
| test_intra_rdt_tensor_transfer                     | PASSED   | PASSED   |
| test_put_and_get_object_with_nixl                  | PASSED   | PASSED   |
| test_put_and_get_object_with_object_store          | PASSED   | PASSED   |
| test_put_gc                                        | PASSED   | PASSED   |
| test_send_duplicate_tensor                         | PASSED   | PASSED   |
| test_nixl_abort_sender_dies_before_creating        | PASSED   | PASSED   |
| test_nixl_abort_sender_dies_before_sending         | PASSED   | PASSED   |
| test_nixl_del_before_creating                      | PASSED   | PASSED   |
| test_nixl_owner_gets_from_launched_task            | PASSED   | PASSED   |
| test_out_of_order_actors                           | PASSED   | PASSED   |
| test_nixl_borrow_after_abort                       | SKIPPED  | SKIPPED  |
| test_shared_tensor_deduplication                   | PASSED   | PASSED   |
| test_nixl_agent_reuse                              | PASSED   | PASSED   |
| test_nixl_agent_reuse_with_partial_tensors         | PASSED   | PASSED   |
| test_storage_level_overlapping_views_reference_count | PASSED | PASSED   |
| test_storage_level_overlapping_views               | PASSED   | PASSED   |
| test_wait_tensor_freed_views                       | PASSED   | PASSED   |
| test_nixl_get_into_tensor_buffers                  | PASSED   | PASSED   |
| test_register_nixl_memory                          | PASSED   | PASSED   |
| **Totals**                                         | **20 passed, 1 skipped** | **20 passed, 1 skipped** |
| **Duration**                                       | 174.63s  | 187.55s  |

### NCCL Tests (`test_rdt_nccl.py` — 1 test)

| Test Name    | C++ GCS  | Rust GCS |
|-------------|----------|----------|
| test_p2p    | PASSED   | PASSED   |
| **Totals**  | **1 passed** | **1 passed** |
| **Duration** | 10.88s  | 12.85s   |

### Summary

| Suite | C++ GCS | Rust GCS | Difference |
|-------|---------|----------|------------|
| NIXL  | 20 passed, 1 skipped | 20 passed, 1 skipped | **IDENTICAL** |
| NCCL  | 1 passed | 1 passed | **IDENTICAL** |
| **Total** | **21 passed, 0 failed, 1 skipped** | **21 passed, 0 failed, 1 skipped** | **IDENTICAL** |

## Warnings

Both backends produce the same 5 warnings during NIXL tests:
- 1x `FutureWarning` about `RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO` (Ray deprecation)
- 4x `PytestUnhandledThreadExceptionWarning` from `_monitor_failures` thread (RDT cleanup racing with Ray session teardown — benign, happens identically on both backends)

## Files Changed

**No files on `origin/master` were changed.** The only change is in `rust/ray-gcs/src/actor_manager.rs` on the `cc-to-rust-experimental` branch (commit `63fe601fa8`):

1. **Store `task_spec` during `RegisterActor`** (line 264-266): Ensures `GetNamedActorInfo` returns `TaskSpec` before `CreateActor` is called, fixing NCCL `ray.get_actor()` cross-process lookup.

2. **Reorder `publish_actor_state` before `notify_raylet_to_kill_actor`** (lines 680-696): Ensures core worker receives `ActorDeathCause` via pubsub before the worker process is killed, fixing NIXL actor-death tests.

## Reproducibility Steps

### Prerequisites

- AWS account with access to us-west-2
- SSH key pair `rdt-nixl-experiment` imported to us-west-2 (private key at `~/.ssh/rdt-nixl-experiment.pem`)
- Security group `sg-069da16cfd254e014` allowing SSH inbound

### Step 1: Launch Instance

```bash
aws ec2 run-instances \
  --region us-west-2 \
  --image-id ami-0fb0010639c839abe \
  --instance-type g4dn.12xlarge \
  --key-name rdt-nixl-experiment \
  --security-group-ids sg-069da16cfd254e014 \
  --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":200,"VolumeType":"gp3"}}]' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=rdt-test}]' \
  --query 'Instances[0].[InstanceId,PublicIpAddress]' --output text
```

Wait for instance to be running and get the public IP:
```bash
aws ec2 wait instance-running --region us-west-2 --instance-ids <INSTANCE_ID>
aws ec2 describe-instances --region us-west-2 --instance-ids <INSTANCE_ID> \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text
```

### Step 2: Install Common Dependencies

SSH into the instance and run:
```bash
# Install Ray nightly
pip3 install -U 'ray[default] @ https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp310-cp310-manylinux2014_x86_64.whl'

# Install cupy for NCCL
pip3 install cupy-cuda12x

# Install NIXL
pip3 install nixl

# Install pytest
pip3 install pytest pytest-timeout

# Create test directory (isolated from ray source to avoid parent conftest.py issues)
mkdir -p ~/rdt-tests
```

### Step 3: Copy Test Files

From local machine (must be on `cc-to-rust-experimental` branch):
```bash
KEY=~/.ssh/rdt-nixl-experiment.pem
IP=<INSTANCE_IP>

cd /path/to/ray
git show cc-to-rust-experimental:python/ray/tests/rdt/test_rdt_nixl.py > /tmp/test_rdt_nixl.py
git show cc-to-rust-experimental:python/ray/tests/rdt/test_rdt_nccl.py > /tmp/test_rdt_nccl.py
git show cc-to-rust-experimental:python/ray/tests/conftest.py > /tmp/conftest.py

scp -i $KEY /tmp/test_rdt_nixl.py /tmp/test_rdt_nccl.py /tmp/conftest.py ubuntu@$IP:~/rdt-tests/
```

### Step 4: Run C++ GCS Tests

```bash
ssh -i $KEY ubuntu@$IP "cd ~/rdt-tests && ~/.local/bin/pytest test_rdt_nixl.py -v --timeout=300"
ssh -i $KEY ubuntu@$IP "cd ~/rdt-tests && ~/.local/bin/pytest test_rdt_nccl.py -v --timeout=300"
```

### Step 5: Build and Configure Rust GCS (for Rust backend testing)

```bash
ssh -i $KEY ubuntu@$IP bash <<'EOF'
set -ex

# Install protoc >= 28.x
curl -sL "https://github.com/protocolbuffers/protobuf/releases/download/v28.3/protoc-28.3-linux-x86_64.zip" -o /tmp/protoc.zip
sudo unzip -o /tmp/protoc.zip -d /usr/local

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

# Clone and build Rust GCS
cd ~
git clone --depth=1 --branch cc-to-rust-experimental https://github.com/ray-project/ray.git
cd ~/ray/rust
cargo build --release -p ray-gcs

# Patch services.py to use Rust GCS when RAY_USE_RUST_GCS=1
python3 -c "
filepath = '$HOME/.local/lib/python3.10/site-packages/ray/_private/services.py'
with open(filepath) as f: c = f.read()
old = '''GCS_SERVER_EXECUTABLE = os.path.join(
    RAY_PATH, \"core\", \"src\", \"ray\", \"gcs\", \"gcs_server\" + EXE_SUFFIX
)'''
new = old + '''
if os.environ.get(\"RAY_USE_RUST_GCS\") == \"1\":
    GCS_SERVER_EXECUTABLE = os.path.expanduser(\"~/ray/rust/target/release/gcs_server\")'''
with open(filepath, 'w') as f: f.write(c.replace(old, new))
print('PATCHED')
"
EOF
```

### Step 6: Run Rust GCS Tests

```bash
ssh -i $KEY ubuntu@$IP "cd ~/rdt-tests && RAY_USE_RUST_GCS=1 ~/.local/bin/pytest test_rdt_nixl.py -v --timeout=300"
ssh -i $KEY ubuntu@$IP "cd ~/rdt-tests && RAY_USE_RUST_GCS=1 ~/.local/bin/pytest test_rdt_nccl.py -v --timeout=300"
```

### Step 7: Terminate Instance

```bash
aws ec2 terminate-instances --region us-west-2 --instance-ids <INSTANCE_ID>
```

### Parallel Execution (Optional)

To run C++ and Rust tests in parallel, launch 2 instances. Set up both with common dependencies. Only apply Rust GCS setup (Step 5) to the Rust instance. Run tests on both simultaneously.

## Notes

- The `test_nixl_borrow_after_abort` test is marked `@pytest.mark.skip` in the test file — expected behavior on both backends.
- The 5 pytest warnings are identical between backends and are benign (RDT cleanup thread racing with session teardown).
- Test directory must be isolated (e.g., `~/rdt-tests/`) — do NOT run from inside `python/ray/tests/` to avoid conftest.py conflicts.
- The Rust GCS binary is at `~/ray/rust/target/release/gcs_server` after `cargo build --release -p ray-gcs`.
- The `services.py` patch must be re-applied after any `pip install ray` since it overwrites the file.
