"""Constants of the NCCL hang detector. Its env vars are in
``ray.train.v2._internal.constants``."""
import re
from enum import Enum

# The stack dump: py-spy's reason for no dump when it isn't installed, and the
# longest wait for `sudo -n true`.
PY_SPY_NOT_INSTALLED = "py-spy not installed"
PASSWORDLESS_SUDO_CHECK_TIMEOUT_S = 5.0

# The diagnostics, by the folder each is uploaded to:
# `<experiment_fs_path>/<HANG_DETECTOR_DIRNAME>/<tool>/`.
FLIGHT_RECORDER_TOOL = "flight_recorder"
NVIDIA_SMI_TOOL = "nvidia_smi"
NCCL_RAS_TOOL = "nccl_ras"
STACK_TRACES_TOOL = "stack_traces"

# When a frozen communicator is first warned about, then how often after.
FIRST_SUSPICION_AFTER_S = 60.0
PERIODIC_WARN_EVERY_S = 120.0

# Polls of RAS history kept on top of the ones a confirmation consumes, so the
# saved history always starts before the communicator stalled.
RAS_HISTORY_MARGIN_POLLS = 10

# Characters not kept when a RAS timestamp is turned into a filename.
UNSAFE_FILENAME_CHARS = re.compile(r"[^0-9A-Za-z._-]")

# Rank ranges named per list in a log line; the rest are counted, so a
# 1000-rank job doesn't log 1000 ranks per poll.
MAX_RANKS_LOGGED = 8

# Grafana dashboard metrics
GRAFANA_STATE = "train_nccl_hang_detector_state"
GRAFANA_STALL_DURATION_S = "train_nccl_hang_stall_duration_s"
GRAFANA_STATE_TAG_KEY = "ray_train_nccl_hang_detector_state"


class NCCLHangDetectorState(Enum):
    """What the NCCL hang detector believes about the run."""

    HEALTHY = "HEALTHY"
    SUSPECTED = "SUSPECTED"
    CONFIRMED = "CONFIRMED"
    DEGRADED = "DEGRADED"
