"""Stand-ins for the docker and crane calls scan_images makes."""

import contextlib
import json
from pathlib import Path
from typing import Any, Callable, Iterator, Optional
from unittest import mock

from ci.ray_ci.automation.trivy import scan_images
from ci.ray_ci.automation.trivy.scan_images import ScanError

FIXTURE = Path(__file__).parent / "testdata" / "ray-py3.12-cpu.json"


class FakeDocker:
    """Stands in for _run: records docker commands, fakes Trivy's output."""

    def __init__(self, fail_scan_of: Optional[str] = None) -> None:
        self.calls: list[list[str]] = []
        self.fail_scan_of = fail_scan_of

    def __call__(self, args, check=True, timeout=None):
        args = list(args)
        self.calls.append(args)
        if args[:2] == ["docker", "run"] and self.fail_scan_of:
            if f"/work/out/{self.fail_scan_of}.json" in args:
                raise ScanError("trivy exited 1")
        if args[:2] == ["docker", "cp"] and ":/work/out/" in args[2]:
            Path(args[3]).write_bytes(FIXTURE.read_bytes())
        return ""

    def commands(self) -> list[str]:
        return [" ".join(c[:3]) for c in self.calls]


def fake_pull(refs, layout, platform):
    layout.mkdir()
    digests = [ref.rsplit("@", 1)[1] for ref in refs]
    (layout / "index.json").write_text(
        json.dumps({"manifests": [{"digest": d} for d in digests]})
    )


def fake_stage(workdir, layout, dbs, image_types, ignore_files=None):
    (workdir / "staging").mkdir()
    return workdir / "staging"


@contextlib.contextmanager
def faked_scan_tools(
    tmp_path: Path,
    docker: FakeDocker,
    digest: Callable[..., str],
    manifest: Callable[[str], dict[str, Any]],
    stage: Callable[..., Path] = fake_stage,
) -> Iterator[None]:
    """Fake everything outside scan_group: crane through registry, docker
    through _run, and fetching and staging the DBs."""
    with mock.patch.multiple(
        scan_images.registry,
        manifest=manifest,
        config=mock.Mock(return_value={"architecture": "amd64"}),
        digest=digest,
        pull=fake_pull,
    ), mock.patch.object(scan_images, "_run", docker), mock.patch.object(
        scan_images, "load_db_pins", return_value={}
    ), mock.patch.object(
        scan_images, "fetch_db", side_effect=lambda s, *_: (tmp_path, {"blob": s.name})
    ), mock.patch.object(
        scan_images, "stage", stage
    ):
        yield
