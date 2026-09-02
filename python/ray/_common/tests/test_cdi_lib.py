import os
import sys

import pytest

from ray._common import cdi_lib

_KIND = "nvidia.com/gpu"


def _sample_cdi_spec(dev_node_path: str, mount_host_path: str, hook_path: str) -> dict:
    return {
        "cdiVersion": "0.6.0",
        "kind": _KIND,
        "containerEdits": {
            "mounts": [
                {
                    "hostPath": mount_host_path,
                    "containerPath": "/usr/lib/x86_64-linux-gnu/libcuda.so.1",
                    "options": ["ro", "nosuid", "nodev", "bind"],
                }
            ],
        },
        "devices": [
            {
                "name": "0",
                "containerEdits": {
                    "env": ["NVIDIA_VISIBLE_DEVICES=0"],
                    "deviceNodes": [
                        {
                            "path": dev_node_path,
                            "type": "c",
                            "major": 195,
                            "minor": 0,
                        }
                    ],
                    "hooks": [
                        {
                            "hookName": "createContainer",
                            "path": hook_path,
                            "args": [hook_path, "--symlink"],
                        }
                    ],
                },
            },
            {
                "name": "1",
                "containerEdits": {"env": ["NVIDIA_VISIBLE_DEVICES=1"]},
            },
        ],
    }


@pytest.fixture
def sample_cdi_spec(tmp_path):
    dev_node_path = "/dev/nvidia0"
    mount_host_path = tmp_path / "libcuda.so.1"
    mount_host_path.write_text("fake")
    hook_path = tmp_path / "nvidia-cdi-hook"
    hook_path.write_text("#!/bin/sh\n")
    return _sample_cdi_spec(dev_node_path, str(mount_host_path), str(hook_path))


def test_cdi_spec_generate_is_vendor_agnostic(monkeypatch):
    """CDISpec.generate isn't NVIDIA-specific: a hypothetical non-NVIDIA
    vendor module would call this the same way ray._common.cdi.get_spec
    does, just with its own kind/generate_fn. Never touches disk -- purely
    in-memory."""
    other_kind = "acme.com/widget"
    monkeypatch.setattr(cdi_lib, "_generated_spec_cache", {})

    generate_calls = []

    def fake_generate():
        generate_calls.append(1)
        return {"kind": other_kind, "devices": []}

    resolved = cdi_lib.CDISpec.generate(other_kind, fake_generate)
    assert resolved.kind == other_kind
    assert len(generate_calls) == 1

    # Second call hits the in-memory cache; generation isn't invoked again.
    resolved_again = cdi_lib.CDISpec.generate(other_kind, fake_generate)
    assert resolved_again.kind == other_kind
    assert len(generate_calls) == 1


def test_cdi_spec_generate_returns_none_and_caches_failure(monkeypatch):
    monkeypatch.setattr(cdi_lib, "_generated_spec_cache", {})
    generate_calls = []

    def fail_generate():
        generate_calls.append(1)
        return None

    assert cdi_lib.CDISpec.generate("acme.com/widget", fail_generate) is None
    assert cdi_lib.CDISpec.generate("acme.com/widget", fail_generate) is None
    assert len(generate_calls) == 1


def test_select_devices(sample_cdi_spec):
    spec = cdi_lib.CDISpec(_KIND, sample_cdi_spec)

    devices = spec.select_devices(["0"])
    assert len(devices) == 1
    assert devices[0]["name"] == "0"

    devices = spec.select_devices(["0", "1"])
    assert [d["name"] for d in devices] == ["0", "1"]


def test_select_devices_missing_id_raises(sample_cdi_spec):
    spec = cdi_lib.CDISpec(_KIND, sample_cdi_spec)
    with pytest.raises(cdi_lib.CDIError, match="7"):
        spec.select_devices(["7"])


def test_apply_edits_merges_env_devices_mounts_hooks(sample_cdi_spec):
    oci_spec = {"process": {"env": ["PATH=/usr/bin"]}, "mounts": []}
    spec = cdi_lib.CDISpec(_KIND, sample_cdi_spec)
    devices = spec.select_devices(["0"])

    spec.apply_edits(oci_spec, devices)

    assert "PATH=/usr/bin" in oci_spec["process"]["env"]
    assert "NVIDIA_VISIBLE_DEVICES=0" in oci_spec["process"]["env"]

    device_nodes = oci_spec["linux"]["devices"]
    assert any(d["path"] == "/dev/nvidia0" for d in device_nodes)
    device_rules = oci_spec["linux"]["resources"]["devices"]
    assert any(r["major"] == 195 and r["minor"] == 0 for r in device_rules)


def test_apply_edits_fills_in_major_minor_via_stat_when_unspecified(monkeypatch):
    """Per the OCI runtime-spec, an absent major/minor on a device cgroup
    rule matches *any* device of that type -- an over-broad grant, not a
    narrowly-scoped one. So when a CDI deviceNode omits them, stat its
    hostPath to fill in the real numbers, the same way the reference CDI
    implementation's fillMissingInfo does."""

    fake_rdev = os.makedev(234, 5)
    expected_major, expected_minor = os.major(fake_rdev), os.minor(fake_rdev)

    class _FakeStat:
        st_rdev = fake_rdev

    monkeypatch.setattr(
        cdi_lib.os,
        "stat",
        lambda p: _FakeStat() if p == "/fake/host/uvm" else os.stat(p),
    )
    raw_spec = {
        "kind": _KIND,
        "devices": [
            {
                "name": "0",
                "containerEdits": {
                    "deviceNodes": [
                        {
                            "path": "/dev/nvidia-uvm",
                            "hostPath": "/fake/host/uvm",
                            "type": "c",
                        }
                    ]
                },
            }
        ],
    }
    spec = cdi_lib.CDISpec(_KIND, raw_spec)
    devices = spec.select_devices(["0"])

    oci_spec = {}
    spec.apply_edits(oci_spec, devices)

    device_rules = oci_spec["linux"]["resources"]["devices"]
    assert len(device_rules) == 1
    assert device_rules[0]["major"] == expected_major
    assert device_rules[0]["minor"] == expected_minor
    assert oci_spec["linux"]["devices"][0]["major"] == expected_major
    assert oci_spec["linux"]["devices"][0]["minor"] == expected_minor


def test_apply_edits_raises_when_major_minor_missing_and_stat_fails(tmp_path):
    """If a CDI deviceNode has no major/minor and its hostPath can't be
    stat'd either, fail loudly rather than silently falling back to an
    over-broad wildcard cgroup rule."""
    raw_spec = {
        "kind": _KIND,
        "devices": [
            {
                "name": "0",
                "containerEdits": {
                    "deviceNodes": [
                        {
                            "path": "/dev/nvidia-uvm",
                            "hostPath": str(tmp_path / "does_not_exist"),
                            "type": "c",
                        }
                    ]
                },
            }
        ],
    }
    spec = cdi_lib.CDISpec(_KIND, raw_spec)
    devices = spec.select_devices(["0"])
    with pytest.raises(cdi_lib.CDIError, match="major/minor"):
        spec.apply_edits({}, devices)


def test_apply_edits_replaces_existing_device_and_mount_at_same_path(tmp_path):
    """Matching the reference CDI implementation's RemoveDevice/RemoveMount
    + Add: a later edit at the same device path or mount destination
    replaces the earlier one rather than producing two conflicting
    entries. The spec-level containerEdits are applied first, then each
    selected device's own -- so the device-level edit here should win."""
    old_lib = tmp_path / "old.so"
    old_lib.write_text("old")
    new_lib = tmp_path / "new.so"
    new_lib.write_text("new")

    raw_spec = {
        "kind": _KIND,
        "containerEdits": {
            "deviceNodes": [
                {"path": "/dev/nvidiactl", "major": 195, "minor": 255, "type": "c"}
            ],
            "mounts": [
                {
                    "hostPath": str(old_lib),
                    "containerPath": "/usr/lib/libcuda.so.1",
                    "options": ["ro"],
                }
            ],
        },
        "devices": [
            {
                "name": "0",
                "containerEdits": {
                    "deviceNodes": [
                        {
                            "path": "/dev/nvidiactl",
                            "major": 195,
                            "minor": 254,
                            "type": "c",
                        }
                    ],
                    "mounts": [
                        {
                            "hostPath": str(new_lib),
                            "containerPath": "/usr/lib/libcuda.so.1",
                            "options": ["ro", "nosuid"],
                        }
                    ],
                },
            }
        ],
    }
    spec = cdi_lib.CDISpec(_KIND, raw_spec)
    devices = spec.select_devices(["0"])

    oci_spec = {}
    spec.apply_edits(oci_spec, devices)

    device_nodes = oci_spec["linux"]["devices"]
    assert [d["path"] for d in device_nodes].count("/dev/nvidiactl") == 1
    assert device_nodes[0]["minor"] == 254

    mounts = oci_spec["mounts"]
    assert [m["destination"] for m in mounts].count("/usr/lib/libcuda.so.1") == 1
    assert mounts[0]["source"] == str(new_lib)


def test_apply_edits_sorts_mounts_by_destination_depth(tmp_path):
    """Matching the reference CDI implementation's sortMounts: shallower
    mount destinations come first, so a parent-directory mount can't
    shadow a deeper one it should instead be overlaid by."""
    shallow_src = tmp_path / "shallow"
    shallow_src.write_text("x")
    deep_src = tmp_path / "deep"
    deep_src.write_text("x")

    raw_spec = {
        "kind": _KIND,
        "devices": [
            {
                "name": "0",
                "containerEdits": {
                    "mounts": [
                        {
                            "hostPath": str(deep_src),
                            "containerPath": "/a/b/c/deep",
                            "options": ["ro"],
                        },
                        {
                            "hostPath": str(shallow_src),
                            "containerPath": "/a/shallow",
                            "options": ["ro"],
                        },
                    ]
                },
            }
        ],
    }
    spec = cdi_lib.CDISpec(_KIND, raw_spec)
    devices = spec.select_devices(["0"])

    oci_spec = {}
    spec.apply_edits(oci_spec, devices)

    assert [m["destination"] for m in oci_spec["mounts"]] == [
        "/a/shallow",
        "/a/b/c/deep",
    ]


def test_apply_edits_defaults_missing_device_type_to_char():
    """Per the CDI spec, "type" defaults to a character device ("c") when
    absent on a deviceNodes entry -- nvidia-ctk's own output relies on
    this, and gVisor rejects a device entry with an empty type outright
    (confirmed against a real gVisor build). major/minor are both given
    explicitly so this doesn't also exercise the hostPath-stat fallback
    (see test_apply_edits_fills_in_major_minor_via_stat_when_unspecified)
    -- this test is only about the type default, and /dev/nvidia-uvm
    doesn't exist on a CPU-only CI machine."""
    raw_spec = {
        "kind": _KIND,
        "devices": [
            {
                "name": "0",
                "containerEdits": {
                    "deviceNodes": [
                        {"path": "/dev/nvidia-uvm", "major": 234, "minor": 0}
                    ]
                },
            }
        ],
    }
    spec = cdi_lib.CDISpec(_KIND, raw_spec)
    devices = spec.select_devices(["0"])

    oci_spec = {}
    spec.apply_edits(oci_spec, devices)

    device_nodes = oci_spec["linux"]["devices"]
    assert len(device_nodes) == 1
    assert device_nodes[0]["type"] == "c"


def test_apply_edits_rejects_non_dev_device_node_path(tmp_path):
    raw_spec = _sample_cdi_spec(
        dev_node_path="/etc/passwd",
        mount_host_path=str(tmp_path / "lib.so"),
        hook_path=str(tmp_path / "hook"),
    )
    (tmp_path / "lib.so").write_text("x")
    (tmp_path / "hook").write_text("x")
    spec = cdi_lib.CDISpec(_KIND, raw_spec)
    devices = spec.select_devices(["0"])
    with pytest.raises(cdi_lib.CDIError, match="/dev/"):
        spec.apply_edits({}, devices)


def test_apply_edits_rejects_nonexistent_mount_host_path(tmp_path):
    raw_spec = _sample_cdi_spec(
        dev_node_path="/dev/nvidia0",
        mount_host_path=str(tmp_path / "does_not_exist.so"),
        hook_path=str(tmp_path / "hook"),
    )
    (tmp_path / "hook").write_text("x")
    spec = cdi_lib.CDISpec(_KIND, raw_spec)
    devices = spec.select_devices(["0"])
    with pytest.raises(cdi_lib.CDIError, match="does not exist"):
        spec.apply_edits({}, devices)


def test_apply_edits_rejects_unsupported_hook_stage(tmp_path):
    mount_host_path = tmp_path / "lib.so"
    mount_host_path.write_text("x")
    hook_path = tmp_path / "hook"
    hook_path.write_text("x")
    raw_spec = _sample_cdi_spec("/dev/nvidia0", str(mount_host_path), str(hook_path))
    raw_spec["devices"][0]["containerEdits"]["hooks"][0]["hookName"] = "bogusStage"
    spec = cdi_lib.CDISpec(_KIND, raw_spec)
    devices = spec.select_devices(["0"])
    with pytest.raises(cdi_lib.CDIError, match="hookName"):
        spec.apply_edits({}, devices)


@pytest.mark.parametrize(
    "edits,missing_field",
    [
        ({"deviceNodes": [{"type": "c"}]}, "path"),
        (
            {"mounts": [{"containerPath": "/usr/lib/libcuda.so.1"}]},
            "hostPath",
        ),
        ({"hooks": [{"hookName": "createContainer"}]}, "path"),
    ],
)
def test_apply_edits_raises_on_missing_required_field(edits, missing_field):
    """A malformed CDI entry (e.g. from a future/unexpected generator
    schema) must fail with a clear CDIError, not a bare
    KeyError."""
    with pytest.raises(cdi_lib.CDIError, match=missing_field):
        cdi_lib._apply_container_edits({}, edits)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
