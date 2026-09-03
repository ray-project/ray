import sys

import pytest

from ray.experimental.sandbox._internal import idmap as idmap_mod
from ray.experimental.sandbox._internal.idmap import (
    IdMap,
    detect_idmap,
    parse_subid_file,
)


@pytest.fixture(autouse=True)
def _fresh_detect_cache():
    detect_idmap.cache_clear()
    yield
    detect_idmap.cache_clear()


def test_parse_subid_file_name_and_uid_keyed(tmp_path):
    path = tmp_path / "subuid"
    path.write_text(
        "# comment\n"
        "other:1000:65536\n"
        "malformed line\n"
        "ray:too:many:fields\n"
        "1000:300000:65536\n"
        "ray:100000:65536\n"
    )
    # Numeric-uid entry appears first and wins.
    assert parse_subid_file(str(path), "ray", 1000) == (300000, 65536)
    # Name-only match.
    assert parse_subid_file(str(path), "ray", 4242) == (100000, 65536)
    # No match at all.
    assert parse_subid_file(str(path), "nobody", 4242) is None


def test_parse_subid_file_skips_small_ranges(tmp_path):
    path = tmp_path / "subuid"
    path.write_text("ray:5000:1\nray:100000:65536\n")
    assert parse_subid_file(str(path), "ray", 1000) == (100000, 65536)


def test_parse_subid_file_missing_file(tmp_path):
    assert parse_subid_file(str(tmp_path / "absent"), "ray", 1000) is None


def _capable_node(monkeypatch, tmp_path):
    """Monkeypatch an environment where multi-uid detection succeeds."""
    subuid = tmp_path / "subuid"
    subgid = tmp_path / "subgid"
    subuid.write_text("ray:100000:65536\n")
    subgid.write_text("ray:200000:65536\n")
    monkeypatch.setattr(idmap_mod.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(idmap_mod, "_no_new_privs", lambda: False)
    monkeypatch.setattr(idmap_mod, "_user_name", lambda: "ray")
    monkeypatch.setattr(idmap_mod, "_probe_sudo_mapfile", lambda *a: False)
    monkeypatch.setattr(idmap_mod.os, "geteuid", lambda: 1000)
    monkeypatch.setattr(idmap_mod.os, "getegid", lambda: 1001)

    real_parse = parse_subid_file

    def _redirected(path, user_name, uid):
        redirect = {"/etc/subuid": str(subuid), "/etc/subgid": str(subgid)}
        return real_parse(redirect.get(path, path), user_name, uid)

    monkeypatch.setattr(idmap_mod, "parse_subid_file", _redirected)


def test_detect_idmap_success(monkeypatch, tmp_path):
    monkeypatch.delenv("RAY_SANDBOX_SINGLE_UID", raising=False)
    _capable_node(monkeypatch, tmp_path)
    assert detect_idmap() == IdMap(
        euid=1000,
        egid=1001,
        subuid_base=100000,
        subuid_count=65536,
        subgid_base=200000,
        subgid_count=65536,
    )


def test_detect_idmap_sudo_mapfile(monkeypatch, tmp_path):
    """A stripped-setuid node maps via privileged map-file writes."""
    monkeypatch.delenv("RAY_SANDBOX_SINGLE_UID", raising=False)
    _capable_node(monkeypatch, tmp_path)
    monkeypatch.setattr(idmap_mod, "_probe_sudo_mapfile", lambda *a: True)
    idmap = detect_idmap()
    assert idmap is not None and idmap.sudo_mapfile is True


def test_detect_idmap_probe_failure(monkeypatch, tmp_path):
    """Neither native helpers nor sudo map-file writes working degrades to
    single-uid."""
    monkeypatch.delenv("RAY_SANDBOX_SINGLE_UID", raising=False)
    _capable_node(monkeypatch, tmp_path)
    monkeypatch.setattr(idmap_mod, "_probe_sudo_mapfile", lambda *a: None)
    assert detect_idmap() is None


def test_detect_idmap_kill_switch(monkeypatch, tmp_path):
    _capable_node(monkeypatch, tmp_path)
    monkeypatch.setenv("RAY_SANDBOX_SINGLE_UID", "1")
    assert detect_idmap() is None


def test_detect_idmap_missing_helpers(monkeypatch, tmp_path):
    monkeypatch.delenv("RAY_SANDBOX_SINGLE_UID", raising=False)
    _capable_node(monkeypatch, tmp_path)
    monkeypatch.setattr(
        idmap_mod.shutil,
        "which",
        lambda name: None if name == "newgidmap" else f"/usr/bin/{name}",
    )
    assert detect_idmap() is None


def test_detect_idmap_no_new_privs(monkeypatch, tmp_path):
    monkeypatch.delenv("RAY_SANDBOX_SINGLE_UID", raising=False)
    _capable_node(monkeypatch, tmp_path)
    monkeypatch.setattr(idmap_mod, "_no_new_privs", lambda: True)
    assert detect_idmap() is None


def test_detect_idmap_missing_range(monkeypatch, tmp_path):
    monkeypatch.delenv("RAY_SANDBOX_SINGLE_UID", raising=False)
    _capable_node(monkeypatch, tmp_path)
    monkeypatch.setattr(idmap_mod, "parse_subid_file", lambda *a: None)
    assert detect_idmap() is None


def test_detect_idmap_numeric_keyed_subgid(monkeypatch, tmp_path):
    """A uid-keyed /etc/subgid resolves even when euid != egid.

    Regression: /etc/subgid's numeric key is the uid, not the gid, so a purely
    uid-keyed subgid file must be found using euid. _capable_node runs with
    euid=1000, egid=1001, so a lookup keyed by egid would miss this entry.
    """
    monkeypatch.delenv("RAY_SANDBOX_SINGLE_UID", raising=False)
    _capable_node(monkeypatch, tmp_path)
    (tmp_path / "subuid").write_text("1000:100000:65536\n")
    (tmp_path / "subgid").write_text("1000:200000:65536\n")
    assert detect_idmap() == IdMap(
        euid=1000,
        egid=1001,
        subuid_base=100000,
        subuid_count=65536,
        subgid_base=200000,
        subgid_count=65536,
    )


def test_probe_sudo_mapfile_missing_unshare(monkeypatch):
    """A missing/unrunnable unshare yields None, not an unhandled OSError."""

    def _no_unshare(*args, **kwargs):
        raise FileNotFoundError("unshare")

    monkeypatch.setattr(idmap_mod.subprocess, "Popen", _no_unshare)
    assert (
        idmap_mod._probe_sudo_mapfile(1000, 1000, (100000, 65536), (200000, 65536))
        is None
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
