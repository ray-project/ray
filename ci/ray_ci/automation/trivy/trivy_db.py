"""Stage Trivy's vulnerability DBs in this build's S3 folder.

Runs once per scanning build, before the scan jobs. Each DB is resolved to a
digest once, and its layer is downloaded by that digest (crane checks the
sha256) and uploaded to the build's ``db/`` folder (see ``scan_store``). Every
scan job in the build then uses the same DB snapshot, and the public mirror
sees one pull per build instead of one per scan job.

``metadata.json`` records the Trivy pin and the DB pins. It goes up last,
so a scan job that finds it can count on both DBs being in place; it is also
written to ``--output`` for ``trivy_report --metadata``.

Example:
    bazel run //ci/ray_ci/automation/trivy:trivy_db -- \\
        --output /artifact-mount/trivy/metadata.json
"""

import json
import logging
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

import click
import runfiles

from ci.ray_ci.automation.trivy import registry, scan_store
from ci.ray_ci.automation.trivy.scan_store import ScanStore

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

TRIVY_PIN_RUNFILE = "io_ray/ci/ray_ci/automation/trivy/trivy_pin.json"
CREATED_ANNOTATION = "org.opencontainers.image.created"
DB_FOLDER = "db"
DB_METADATA = "metadata.json"


class TrivyDbError(Exception):
    """The DB could not be staged or verified."""


@dataclass(frozen=True)
class DbSource:
    name: str
    upstream: str
    ghcr: str
    media_type: str

    @property
    def upstream_repo(self) -> str:
        return self.upstream.rsplit(":", 1)[0]

    @property
    def file_name(self) -> str:
        return f"trivy-{self.name.replace('_', '-')}.tar.gz"


DB_SOURCES = (
    DbSource(
        name="db",
        upstream="public.ecr.aws/aquasecurity/trivy-db:2",
        ghcr="ghcr.io/aquasecurity/trivy-db:2",
        media_type="application/vnd.aquasec.trivy.db.layer.v1.tar+gzip",
    ),
    DbSource(
        name="java_db",
        upstream="public.ecr.aws/aquasecurity/trivy-java-db:1",
        ghcr="ghcr.io/aquasecurity/trivy-java-db:1",
        media_type="application/vnd.aquasec.trivy.javadb.layer.v1.tar+gzip",
    ),
)


def db_layer(manifest: Mapping[str, Any], media_type: str) -> Mapping[str, Any]:
    layers = [
        layer
        for layer in manifest.get("layers") or []
        if layer.get("mediaType") == media_type
    ]
    if len(layers) != 1:
        raise TrivyDbError(f"expected one {media_type} layer, found {len(layers)}")
    return layers[0]


def _created(manifest: Mapping[str, Any]) -> str:
    created = (manifest.get("annotations") or {}).get(CREATED_ANNOTATION)
    if not created:
        layers = manifest.get("layers") or [{}]
        created = (layers[0].get("annotations") or {}).get(CREATED_ANNOTATION, "")
    return created


def _ghcr_layer_matches(source: DbSource, layer_digest: str) -> Optional[bool]:
    """Compare the DB layer with ghcr.io's copy.

    The two registries wrap the same layer in different manifests, so only
    the layer digest is comparable. A mismatch is logged, not fatal: the
    mirrors can disagree for a while during a DB refresh.
    """
    try:
        ghcr = db_layer(registry.manifest(source.ghcr), source.media_type)
    except (registry.RegistryError, TrivyDbError) as e:
        logger.warning(f"Could not compare {source.name} with ghcr.io: {e}")
        return None
    if ghcr["digest"] != layer_digest:
        logger.warning(
            f"{source.name}: ghcr.io serves layer {ghcr['digest']}, "
            f"public.ecr.aws serves {layer_digest}"
        )
        return False
    return True


def stage_db(
    source: DbSource, store: ScanStore, workdir: Path, check_ghcr: bool = True
) -> dict[str, Any]:
    """Upload one DB, pinned by digest, to the build's db/ folder."""
    manifest_digest = registry.digest(source.upstream)
    pinned = f"{source.upstream_repo}@{manifest_digest}"
    manifest = registry.manifest(pinned)
    layer = db_layer(manifest, source.media_type)
    ghcr_matches = _ghcr_layer_matches(source, layer["digest"]) if check_ghcr else None

    tarball = workdir / source.file_name
    registry.fetch_blob(source.upstream_repo, layer["digest"], tarball)
    store.upload(tarball, DB_FOLDER)
    tarball.unlink()
    url = store.url(DB_FOLDER) + source.file_name
    logger.info(f"Uploaded {pinned} ({layer['digest']}) to {url}")
    return {
        "source": source.upstream,
        "manifest": manifest_digest,
        "blob": layer["digest"],
        "size": layer.get("size", 0),
        "media_type": source.media_type,
        "created": _created(manifest),
        "object": url,
        "ghcr_layer_matches": ghcr_matches,
    }


def load_trivy_pin() -> dict[str, Any]:
    path = runfiles.Create().Rlocation(TRIVY_PIN_RUNFILE)
    return json.loads(Path(path).read_text())


@click.command()
@click.option(
    "--output",
    required=True,
    type=click.Path(dir_okay=False, path_type=Path),
    help="Where to write metadata.json.",
)
@click.option(
    "--work-dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Where to download the DBs (default: a temp dir).",
)
@click.option(
    "--check-ghcr/--no-check-ghcr",
    default=True,
    help="Compare each DB layer with ghcr.io's copy.",
)
@scan_store.build_options
def main(
    output: Path, work_dir: Optional[Path], check_ghcr: bool, store: ScanStore
) -> None:
    if not output.is_absolute():
        output = Path(os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")) / output
    metadata = {"trivy": load_trivy_pin()}
    with tempfile.TemporaryDirectory(dir=work_dir) as tmp:
        for source in DB_SOURCES:
            metadata[source.name] = stage_db(source, store, Path(tmp), check_ghcr)
        staged = Path(tmp) / DB_METADATA
        staged.write_text(json.dumps(metadata, indent=2) + "\n")
        # Last: scan jobs take metadata.json to mean every DB is in place.
        store.upload(staged, DB_FOLDER)
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(staged, output)
    logger.info(f"Wrote {output} and {store.url(DB_FOLDER)}{DB_METADATA}")


if __name__ == "__main__":
    main()
