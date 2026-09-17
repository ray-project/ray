"""Inject synthetic device telemetry through DCGM, as NVSentinel's demo does.

NVSentinel's GPU health monitor reads DCGM. DCGM will accept injected field
values, so a GPU fault is reproducible without a broken GPU -- you fake the
counter, not the silicon. This is exactly what NVSentinel's own
``demos/local-fault-injection-demo`` does:

    kubectl exec -n gpu-operator <dcgm-pod> -- dcgmi test --inject --gpuid 0 -f 84 -v 0

and with DCGM's fake-GPU mode it runs in a kind cluster on a laptop, no GPU
anywhere. That gives the inbound adapter a real end-to-end test: injected field
-> DCGM -> NVSentinel health event -> node condition -> cordon -> our
``NVSentinelProbe`` -> ``Evict``.

    from ray.train.v2._internal.execution.health.testing import dcgm

    inj = dcgm.DcgmInjector(node="nvsentinel-demo-worker")
    inj.inject(dcgm.INFOROM_VALID, 0)          # the demo's fatal fault
    inj.inject(dcgm.XID_ERRORS, 74)            # NVLink error
    inj.inject(dcgm.GPU_TEMP, 95)              # thermal
"""
import logging
import subprocess
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)

# DCGM field ids, from dcgmlib/dcgm_fields.h.
INFOROM_VALID = 84  # DCGM_FI_DEV_INFOROM_CONFIG_VALID; 0 = corrupt (demo's fault)
MEMORY_TEMP = 140  # DCGM_FI_DEV_MEMORY_TEMP
GPU_TEMP = 150  # DCGM_FI_DEV_GPU_TEMP
POWER_USAGE = 155  # DCGM_FI_DEV_POWER_USAGE
PCIE_REPLAY_COUNTER = 202  # DCGM_FI_DEV_PCIE_REPLAY_COUNTER
XID_ERRORS = 230  # DCGM_FI_DEV_XID_ERRORS
ECC_SBE_VOL_TOTAL = 310  # single-bit, volatile
ECC_DBE_VOL_TOTAL = 311  # double-bit, volatile -- the uncorrectable one
RETIRED_DBE = 391  # DCGM_FI_DEV_RETIRED_DBE
NVLINK_CRC_FLIT_ERRORS = 409  # DCGM_FI_DEV_NVLINK_CRC_FLIT_ERROR_COUNT_TOTAL

#: Common XID codes, for the ``XID_ERRORS`` field.
XID_NVLINK_ERROR = 74
XID_ECC_UNCORRECTABLE = 94
XID_GPU_FALLEN_OFF_BUS = 79
XID_GSP_RPC_TIMEOUT = 119

#: A 55-day study of a 504-GPU pre-training run saw exactly these, in this
#: order of frequency: NVLink (6), ECC (2), GPU dropout (2), GSP timeout (1).
#: Injecting the real distribution beats inventing one.
OBSERVED_XIDS = (
    XID_NVLINK_ERROR,
    XID_NVLINK_ERROR,
    XID_ECC_UNCORRECTABLE,
    XID_GPU_FALLEN_OFF_BUS,
    XID_GSP_RPC_TIMEOUT,
)


class DcgmInjectionFailed(RuntimeError):
    """``dcgmi test --inject`` did not succeed."""


@dataclass
class DcgmInjector:
    """Runs ``dcgmi test --inject`` locally or inside a DCGM pod.

    Args:
        node: Kubernetes node to target. When set, the command is run with
            ``kubectl exec`` inside the DCGM pod on that node, which is how
            NVSentinel's demo reaches a kind cluster's fake GPU. When unset the
            command runs locally against the host's own ``nv-hostengine``.
        namespace: Namespace holding the DCGM daemonset.
        selector: Label selector identifying DCGM pods.
        gpu_id: Which GPU on the node to inject into.
        dry_run: Build the command but do not run it. Used by unit tests to
            assert we issue exactly the call the demo does.
    """

    node: Optional[str] = None
    namespace: str = "gpu-operator"
    selector: str = "app=nvidia-dcgm"
    gpu_id: int = 0
    dry_run: bool = False

    def dcgm_pod(self) -> str:
        """The DCGM pod scheduled on ``node``."""
        if not self.node:
            raise ValueError("dcgm_pod() needs a node")
        out = self._run(
            [
                "kubectl",
                "get",
                "pods",
                "-n",
                self.namespace,
                "-l",
                self.selector,
                "-o",
                "json",
            ]
        )
        import json

        pods = json.loads(out)["items"]
        for pod in pods:
            if pod["spec"].get("nodeName") == self.node:
                return pod["metadata"]["name"]
        raise DcgmInjectionFailed(
            f"no pod matching {self.selector!r} in {self.namespace!r} on {self.node!r}"
        )

    def command(self, field: int, value: int, pod: Optional[str] = None) -> List[str]:
        """The exact argv for one injection."""
        inject = [
            "dcgmi",
            "test",
            "--inject",
            "--gpuid",
            str(self.gpu_id),
            "-f",
            str(field),
            "-v",
            str(value),
        ]
        if not self.node:
            return inject
        return [
            "kubectl",
            "exec",
            "-n",
            self.namespace,
            pod or "<dcgm-pod>",
            "--",
            *inject,
        ]

    def inject(self, field: int, value: int) -> List[str]:
        """Inject one field value. Returns the command that was issued.

        There is no general way to withdraw an injected sample, so a node that
        has been injected into stays dirty for the life of the cluster. In a
        kind cluster that is fine: throw the cluster away.
        """
        pod = self.dcgm_pod() if self.node and not self.dry_run else None
        cmd = self.command(field, value, pod=pod)
        logger.warning("[fault-injection] %s", " ".join(cmd))
        if not self.dry_run:
            self._run(cmd)
        return cmd

    def inject_fatal_gpu_fault(self) -> List[str]:
        """The fault NVSentinel's demo uses: a corrupt InfoROM.

        Fatal, so NVSentinel writes a node condition and cordons -- which is
        the signal our inbound probe consumes.
        """
        return self.inject(INFOROM_VALID, 0)

    @staticmethod
    def _run(cmd: List[str]) -> str:
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        except FileNotFoundError as e:
            raise DcgmInjectionFailed(f"{cmd[0]} not found on PATH") from e
        except subprocess.TimeoutExpired as e:
            raise DcgmInjectionFailed(f"{' '.join(cmd)} timed out") from e
        if proc.returncode != 0:
            raise DcgmInjectionFailed(
                f"{' '.join(cmd)} exited {proc.returncode}: "
                f"{(proc.stderr or '').strip()[:500]}"
            )
        return proc.stdout
