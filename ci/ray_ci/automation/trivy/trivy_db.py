"""Mirror Trivy's vulnerability DBs into this build's ECR work repo.

Runs once per scanning build, before the scan jobs. Each DB is resolved to a
digest once, then copied by that digest to
``${RAYCI_WORK_REPO}:${RAYCI_BUILD_ID}-trivy-db`` (and ``-trivy-java-db``), so
every scan job in the build uses the same DB snapshot, and the public mirror
sees one pull per build instead of one per scan job.

Also writes ``metadata.json`` (the Trivy pin and the DB pins) for
``trivy_report --metadata``.

Example:
    bazel run //ci/ray_ci/automation/trivy:trivy_db -- \\
        --output /artifact-mount/trivy/metadata.json
"""

import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

import click
import runfiles

from ci.ray_ci.automation.trivy import registry
from ci.ray_ci.utils import ecr_docker_login

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

TRIVY_PIN_RUNFILE = "io_ray/ci/ray_ci/automation/trivy/trivy_pin.json"
CREATED_ANNOTATION = "org.opencontainers.image.created"


class TrivyDbError(Exception):
    """The DB could not be mirrored or verified."""


@dataclass(frozen=True)
class DbSource:
    name: str
    upstream: str
    ghcr: str
    media_type: str

    @property
    def upstream_repo(self) -> str:
        return self.upstream.rsplit(":", 1)[0]


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


def mirror_ref(work_repo: str, build_id: str, source: DbSource) -> str:
    return f"{work_repo}:{build_id}-trivy-{source.name.replace('_', '-')}"


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


def mirror_db(
    source: DbSource, work_repo: str, build_id: str, check_ghcr: bool = True
) -> dict[str, Any]:
    manifest_digest = registry.digest(source.upstream)
    pinned = f"{source.upstream_repo}@{manifest_digest}"
    manifest = registry.manifest(pinned)
    layer = db_layer(manifest, source.media_type)
    ghcr_matches = _ghcr_layer_matches(source, layer["digest"]) if check_ghcr else None

    mirror = mirror_ref(work_repo, build_id, source)
    registry.copy(pinned, mirror)
    mirrored = registry.digest(mirror)
    if mirrored != manifest_digest:
        raise TrivyDbError(
            f"{mirror} has digest {mirrored} after copying {manifest_digest}"
        )
    logger.info(f"Mirrored {source.upstream} ({manifest_digest}) to {mirror}")
    return {
        "source": source.upstream,
        "manifest": manifest_digest,
        "blob": layer["digest"],
        "size": layer.get("size", 0),
        "media_type": source.media_type,
        "created": _created(manifest),
        "mirror": mirror,
        "ghcr_layer_matches": ghcr_matches,
    }


def load_trivy_pin() -> dict[str, Any]:
    path = runfiles.Create().Rlocation(TRIVY_PIN_RUNFILE)
    return json.loads(Path(path).read_text())


@click.command()
@click.option("--rayci-work-repo", required=True, envvar="RAYCI_WORK_REPO")
@click.option("--rayci-build-id", required=True, envvar="RAYCI_BUILD_ID")
@click.option(
    "--output",
    required=True,
    type=click.Path(dir_okay=False, path_type=Path),
    help="Where to write metadata.json.",
)
@click.option(
    "--check-ghcr/--no-check-ghcr",
    default=True,
    help="Compare each DB layer with ghcr.io's copy.",
)
def main(
    rayci_work_repo: str, rayci_build_id: str, output: Path, check_ghcr: bool
) -> None:
    if not output.is_absolute():
        output = Path(os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")) / output
    registry.login_if_ecr(rayci_work_repo, ecr_docker_login)
    metadata = {"trivy": load_trivy_pin()}
    for source in DB_SOURCES:
        metadata[source.name] = mirror_db(
            source, rayci_work_repo, rayci_build_id, check_ghcr
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(metadata, indent=2) + "\n")
    logger.info(f"Wrote {output}")


if __name__ == "__main__":
    main()
