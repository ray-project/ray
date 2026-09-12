"""Generates CDI (Container Device Interface) specs and merges their
device edits into an OCI runtime spec, vendor-agnostically.

CDI (https://github.com/cncf-tags/container-device-interface) is a
CNCF-sponsored standard for injecting complex devices like GPUs into
containers without runtime-specific logic. Docker, containerd, CRI-O,
Podman, and Singularity/Apptainer all support it, and in Kubernetes
it's both what device plugins produce and the foundation Dynamic
Resource Allocation (DRA) builds on. A CDI spec names a device "kind"
(e.g. "nvidia.com/gpu") and lists the OCI runtime edits (env vars,
device nodes, mounts, hooks) each of its devices needs. This module
implements just enough of the spec to generate one, look up devices by
kind, and merge their edits into an OCI runtime spec (the same merge a
CDI-aware runtime performs internally).

This module is modeled on the canonical Go implementation for CDI at
https://github.com/cncf-tags/container-device-interface/tree/main/pkg/cdi.
Unlike that implementation, this one always generates a spec fresh, via
a vendor-supplied callback, rather than reading one from disk. Reading
from disk would mean depending on some external tool having already
generated and placed a correct spec there; generating it directly
keeps that entirely within Ray's own control.

Ideally this module would build on a canonical Python CDI
implementation. None exists, so it hand-rolls the subset it needs
instead.

Callers shouldn't call into this module directly. Instead, they make
calls to `ray._common.cdi.get_spec`, passing it a resource type (e.g.
"GPU"). This resource type then gets resolved by Ray to the right
accelerator manager (e.g. `NvidiaGPUAcceleratorManager`), whose
`generate_cdi_spec` gets passed in as the callback this module needs.
Once a `CDISpec` is in hand, `select_devices` looks up specific
devices by id, and `apply_edits` merges their `containerEdits` into an
OCI runtime spec in place.

Lives in `ray/_common/`, not `ray/_private/`, since libraries like
`ray.experimental.sandbox` depend on it and can't depend on
`ray._private` directly.
"""

import os
from typing import Any, Callable, Dict, List, Optional

# OCI hook stage names that a CDI spec's containerEdits.hooks may target.
_OCI_HOOK_STAGES = (
    "prestart",
    "createRuntime",
    "createContainer",
    "startContainer",
    "poststart",
    "poststop",
)


class CDIError(Exception):
    """Raised for any CDI spec generation/validation/merge failure.
    Deliberately not a sandbox-specific exception — see this module's
    docstring."""


class CDISpec:
    """A CDI spec for a single kind (e.g. "nvidia.com/gpu"), generated in
    memory, then usable to select devices from it and apply their
    `containerEdits` to an OCI runtime spec.

    Construct via `CDISpec.generate`, not directly.
    """

    def __init__(self, kind: str, spec: Dict[str, Any]):
        self.kind = kind
        self._spec = spec

    @classmethod
    def generate(
        cls,
        kind: str,
        generate_fn: Callable[[], Optional[Dict[str, Any]]],
    ) -> Optional["CDISpec"]:
        """Generate a CDI spec of the given kind via `generate_fn`. The
        full policy any vendor-specific "get my CDI spec" function (e.g.
        `ray._common.cdi.get_spec`) needs, parameterized so it doesn't
        have to be reimplemented per vendor.

        Never caches: calls `generate_fn` fresh every time, and never
        writes anything to disk. Caching, if a caller wants it, is that
        caller's own policy decision, not this library's.

        Args:
            kind: CDI kind to generate for, e.g. "nvidia.com/gpu".
            generate_fn: Returns the parsed CDI spec, or None on failure.
                Vendor-specific (e.g. shells out to nvidia-ctk).

        Returns:
            A `CDISpec`, or None if generation failed.
        """
        spec = generate_fn()
        return cls(kind, spec) if spec is not None else None

    def select_devices(self, ids: List[str]) -> List[Dict[str, Any]]:
        """Look up devices of this spec's kind by id.

        Args:
            ids: Device ids/UUIDs to look up — the bare CDI device "name"
                (e.g. "0" or a GPU UUID), *not* the kind-qualified
                "<kind>=<name>" form used only to reference a device
                externally (see the CDI spec's Kind section).

        Returns:
            The matching CDI device entries, in the same order as `ids`.

        Raises:
            CDIError: if any id has no matching device in this spec.
        """
        devices_by_name = {d.get("name"): d for d in self._spec.get("devices", [])}
        selected = []
        missing = []
        for device_id in ids:
            device = devices_by_name.get(device_id)
            if device is None:
                missing.append(device_id)
            else:
                selected.append(device)
        if missing:
            raise CDIError(
                f"CDI device(s) {missing} not found in spec; available devices: "
                f"{sorted(d for d in devices_by_name if d is not None)}."
            )
        return selected

    def apply_edits(
        self, oci_spec: Dict[str, Any], devices: List[Dict[str, Any]]
    ) -> None:
        """Merge this spec's shared `containerEdits` and each of `devices`'
        own `containerEdits` into an OCI runtime spec, in place.

        Not transactional: a validation error partway through (e.g. an
        invalid deviceNodes path found after env/mounts were already
        merged) leaves `oci_spec` partially mutated. Callers must discard
        `oci_spec` on any exception rather than reuse it.

        Args:
            oci_spec: The OCI runtime spec (config.json) dict to modify.
            devices: CDI device entries (see `select_devices`) to apply.
        """
        _apply_container_edits(oci_spec, self._spec.get("containerEdits", {}))
        for device in devices:
            _apply_container_edits(oci_spec, device.get("containerEdits", {}))


def _require_field(entry: Dict[str, Any], key: str, kind: str) -> Any:
    if key not in entry:
        raise CDIError(
            f"CDI {kind} entry is missing required field '{key}': {entry!r}."
        )
    return entry[key]


def _require_valid_device_node_path(path: str) -> None:
    if not path.startswith("/dev/"):
        raise CDIError(
            f"Refusing to inject CDI device node with non-'/dev/' path: '{path}'."
        )


def _require_valid_host_path(path: str, kind: str) -> None:
    if not os.path.isabs(path):
        raise CDIError(
            f"Refusing to inject CDI {kind} with non-absolute host path: '{path}'."
        )
    if not os.path.exists(path):
        raise CDIError(
            f"Refusing to inject CDI {kind}: host path does not exist: '{path}'."
        )


def _apply_container_edits(oci_spec: Dict[str, Any], edits: Dict[str, Any]) -> None:
    if not edits:
        return

    env = edits.get("env")
    if env:
        # A duplicate name already in process.env (e.g. the base image's
        # own default NVIDIA_VISIBLE_DEVICES=all) is replaced, not merely
        # shadowed: runsc's own NVIDIA_VISIBLE_DEVICES handling only
        # recognizes a single CDI-provided value, so leaving a stale
        # duplicate behind makes runsc invoke nvidia-container-cli anyway.
        new_keys = {e.split("=", 1)[0] for e in env}
        process_env = oci_spec.setdefault("process", {}).setdefault("env", [])
        process_env[:] = [e for e in process_env if e.split("=", 1)[0] not in new_keys]
        process_env.extend(env)

    device_nodes = edits.get("deviceNodes")
    if device_nodes:
        linux_spec = oci_spec.setdefault("linux", {})
        devices = linux_spec.setdefault("devices", [])
        resources = linux_spec.setdefault("resources", {})
        device_rules = resources.setdefault("devices", [])
        for node in device_nodes:
            path = _require_field(node, "path", "deviceNode")
            _require_valid_device_node_path(path)
            device_entry = {
                k: node[k]
                for k in ("path", "major", "minor", "fileMode", "uid", "gid")
                if k in node
            }
            # Per the CDI spec, "type" defaults to a character device
            # ("c") when absent -- gVisor rejects a device entry with an
            # empty type, and nvidia-ctk's own output relies on this
            # default.
            device_entry["type"] = node.get("type") or "c"
            major, minor = node.get("major"), node.get("minor")
            if major is None or minor is None:
                # An absent major/minor on a device cgroup rule matches
                # *any* device of that type, per the OCI runtime spec --
                # so instead of leaving them out (over-granting rwm access
                # to every device of this type), stat the node's hostPath
                # (the CDI spec's own fallback when hostPath is unset is
                # `path`, but `path` alone is the *container*-side path --
                # not guaranteed to exist yet here, before the container
                # does), the same way the reference CDI implementation's
                # fillMissingInfo does.
                host_path = node.get("hostPath") or path
                try:
                    rdev = os.stat(host_path).st_rdev
                    major, minor = os.major(rdev), os.minor(rdev)
                    device_entry.setdefault("major", major)
                    device_entry.setdefault("minor", minor)
                except OSError as err:
                    raise CDIError(
                        f"CDI deviceNode '{path}' has no major/minor and "
                        f"its hostPath '{host_path}' couldn't be stat'd to "
                        f"fill them in: {err}."
                    ) from err
            # Matching the reference CDI implementation's RemoveDevice +
            # AddDevice: drop any existing device already at this path
            # (e.g. from an earlier edit in this same merge) before
            # adding the new one, rather than ending up with two
            # conflicting entries for the same container-side path.
            devices[:] = [d for d in devices if d.get("path") != path]
            devices.append(device_entry)
            device_rule = {
                "allow": True,
                "type": device_entry["type"],
                "major": major,
                "minor": minor,
                "access": "rwm",
            }
            device_rules.append(device_rule)

    mounts = edits.get("mounts")
    if mounts:
        oci_mounts = oci_spec.setdefault("mounts", [])
        for mount in mounts:
            host_path = _require_field(mount, "hostPath", "mount")
            container_path = _require_field(mount, "containerPath", "mount")
            _require_valid_host_path(host_path, "mount")
            # Matching the reference CDI implementation's RemoveMount +
            # AddMount: drop any existing mount already at this
            # destination before adding the new one, rather than ending
            # up with two conflicting mounts for the same path.
            oci_mounts[:] = [
                m for m in oci_mounts if m.get("destination") != container_path
            ]
            oci_mounts.append(
                {
                    "destination": container_path,
                    "type": "bind",
                    "source": host_path,
                    "options": mount.get("options") or ["rbind", "ro"],
                }
            )
        # Matching the reference CDI implementation's sortMounts: shallower
        # destinations first, so a parent-directory mount can't shadow a
        # deeper one that should overlay it, if the runtime applies mounts
        # in array order.
        oci_mounts.sort(
            key=lambda m: os.path.normpath(m.get("destination", "/")).count(os.sep)
        )

    hooks = edits.get("hooks")
    if hooks:
        oci_hooks = oci_spec.setdefault("hooks", {})
        for hook in hooks:
            stage = hook.get("hookName")
            if stage not in _OCI_HOOK_STAGES:
                raise CDIError(
                    f"CDI hook has unsupported hookName '{stage}'; expected one of "
                    f"{_OCI_HOOK_STAGES}."
                )
            path = _require_field(hook, "path", "hook")
            _require_valid_host_path(path, "hook")
            oci_hooks.setdefault(stage, []).append(
                {k: hook[k] for k in ("path", "args", "env") if k in hook}
            )
