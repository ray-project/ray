"""Minimal, vendor-agnostic CDI (Container Device Interface) spec generator
and OCI-spec merger.

Implements just the subset of the CDI spec
(https://github.com/cncf-tags/container-device-interface/blob/main/SPEC.md)
needed here: generating a spec (via a vendor-supplied callback), looking up
devices of a given "kind" (e.g. "nvidia.com/gpu"), and merging their
`containerEdits` into a hand-built OCI runtime spec (config.json) — the same
merge a CDI-aware container runtime (containerd/CRI-O/Podman) performs
internally.

Deliberately does not read a spec from disk (e.g. `/etc/cdi`): an on-disk
spec written by tooling outside Ray (e.g. a stock `nvidia-ctk cdi generate`
run by an NVIDIA GPU Operator daemonset) isn't guaranteed to carry whatever
flags a given accelerator manager's own `generate_cdi_spec()` depends on
for correctness (e.g. NVIDIA's gVisor-compatibility flags) — see
`ray._common.cdi.get_spec`. Always generating avoids silently picking
up an incompatible spec.

Lives under `ray/_common/`, not `ray/_private/` or under
`ray.experimental.sandbox`: `ray/_common/README.md` reserves `_common` for
non-public APIs shared between Ray Core and the libraries (Serve, Train,
Data, Tune, and experimental features like Sandboxes) — libraries must not
depend on `ray._private` directly, and this is depended on by a library
(`ray.experimental.sandbox`, via `ray._common.cdi`), which is enough on its
own to rule out `ray._private` as this module's home.

Genuinely vendor-agnostic in scope, not just NVIDIA-shaped with room to
grow: `CDISpec.generate`/`select_devices` both take `kind` as a plain
parameter, and know nothing about any specific accelerator vendor.
`ray._common.cdi` builds NVIDIA GPU support (and, mechanically, any future
vendor's) on top of this by resolving `kind` from
`ray._private.accelerators.get_accelerator_manager_for_resource` — the same
mechanism that already decides a node's "GPU" resource means NVIDIA vs.
AMD vs. Apple vs. Metax.

Also raises its own `CDIError` rather than a sandbox-specific exception,
even though sandbox's NVIDIA GPU support is its only consumer today: CDI
itself is vendor-agnostic and not tied to sandboxes at all (a different
accelerator vendor, or a non-sandbox use case) would need this same
generation/merging logic, and coupling it to
`ray.experimental.sandbox.exceptions.SandboxCreationError` would force that
use case to either import from the sandbox module or duplicate this code.
Callers that want their own error type translate `CDIError` at their own
boundary — see `ray.experimental.sandbox.image_manager` for that
translation.

There is no existing Python implementation of CDI to depend on — the
reference implementation (tags.cncf.io/container-device-interface) is
Go-only. A Rust port exists under the same CNCF org
(github.com/cncf-tags/container-device-interface-rs) and could in principle
be wrapped for Python via PyO3/maturin as a compiled extension rather than a
subprocess call; that's the natural path if CDI use in Ray grows beyond this
one alpha module, but it means standing up a new cross-compiled-wheel
toolchain Ray's build doesn't have today, so this module hand-rolls just the
subset it needs for now. Being vendor-agnostic (no NVIDIA/Ray specifics),
it's the piece that could be lifted out wholesale into a standalone
`python-cdi` package later.
"""

import os
from typing import Any, Callable, Dict, List, Optional

# In-memory cache of generated CDI specs, by kind, for CDISpec.generate.
# Never written to disk. A None value means generation was attempted and
# failed (cached too, so it isn't retried for every subsequent caller in
# this process); a fresh process gets a clean retry.
_generated_spec_cache: Dict[str, Optional[Dict[str, Any]]] = {}

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
        """Generate (or return the cached) CDI spec of the given kind via
        `generate_fn`. The full policy any vendor-specific "get my CDI
        spec" function (e.g. `ray._common.cdi.get_spec`) needs,
        parameterized so it doesn't have to be reimplemented per vendor.

        Caches the parsed result **in memory only, never written to
        disk** — deliberately, so this stays simple to swap for a real
        CDI generator library later (see the future-improvement notes on
        `generate_fn` implementations, e.g.
        `ray._private.accelerators.nvidia_gpu.generate_cdi_spec`) without
        this cache format needing to track whatever encoding that library
        happens to use for a saved spec. A failed generation (`generate_fn`
        raising or returning None) is cached too, so it isn't retried for
        every subsequent caller in the same process; a fresh process gets
        a clean retry. Nothing here provides cross-process caching — see
        `ray._common.cdi`'s module docstring for why that's fine.

        Args:
            kind: CDI kind to generate for, e.g. "nvidia.com/gpu".
            generate_fn: Returns the parsed CDI spec, or None on failure.
                Vendor-specific (e.g. shells out to nvidia-ctk).

        Returns:
            A `CDISpec`, or None if generation failed.
        """
        if kind in _generated_spec_cache:
            spec = _generated_spec_cache[kind]
        else:
            spec = generate_fn()
            _generated_spec_cache[kind] = spec
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
