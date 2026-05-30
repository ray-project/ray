"""Generate a Buildkite pipeline that triggers the Cursor template upgrade
workflow for every template in the anyscale/templates BUILD.yaml.

Reads RAY_VERSION from the environment, fetches BUILD.yaml from the templates
repo, and emits one parallel webhook step per template name to stdout. The
output is meant to be piped into `buildkite-agent pipeline upload`.
"""

import json
import os
import sys
import urllib.request

import yaml

BUILD_YAML_URL = "https://raw.githubusercontent.com/anyscale/templates/main/BUILD.yaml"
FETCH_TIMEOUT_SEC = 30

WEBHOOK_SECRET_ID = "oss-ci/cursor-template-upgrade-webhook-url"
API_KEY_SECRET_ID = "oss-ci/cursor-template-upgrade-api-key"


def fetch_template_names(url: str) -> list:
    """Fetch BUILD.yaml and return the list of top-level template names."""
    with urllib.request.urlopen(url, timeout=FETCH_TIMEOUT_SEC) as resp:
        build = yaml.safe_load(resp.read())
    if not isinstance(build, list):
        raise ValueError(f"Expected BUILD.yaml to be a list, got {type(build)}")
    names = [
        entry["name"] for entry in build if isinstance(entry, dict) and "name" in entry
    ]
    if not names:
        raise ValueError("No template names found in BUILD.yaml")
    return names


def make_step(template_name: str, ray_version: str) -> dict:
    """Build a single soft-failing webhook step for one template."""
    payload = json.dumps({"template_name": template_name, "ray_version": ray_version})
    return {
        "label": f":cursor: Trigger Cursor upgrade: {template_name}",
        "key": f"cursor-upgrade-{template_name}".replace("_", "-"),
        "depends_on": [],
        "instance_type": "small_branch",
        "mount_buildkite_agent": True,
        "soft_fail": True,
        "commands": [
            'export CURSOR_WEBHOOK_URL="$(aws secretsmanager get-secret-value '
            f"--region us-west-2 --secret-id {WEBHOOK_SECRET_ID} "
            "--query SecretString --output text | tr -d '\\n\\r')\"",
            'export CURSOR_API_KEY="$(aws secretsmanager get-secret-value '
            f"--region us-west-2 --secret-id {API_KEY_SECRET_ID} "
            "--query SecretString --output text | tr -d '\\n\\r')\"",
            'curl -sSf -X POST "$CURSOR_WEBHOOK_URL" '
            '-H "Authorization: Bearer $CURSOR_API_KEY" '
            '-H "Content-Type: application/json" '
            f"-d {json.dumps(payload)}",
        ],
    }


def main() -> None:
    ray_version = os.environ.get("RAY_VERSION", "").strip()
    if not ray_version:
        raise SystemExit("RAY_VERSION must be set in the environment")

    names = fetch_template_names(BUILD_YAML_URL)
    print(
        f"Generating {len(names)} Cursor upgrade steps for ray {ray_version}",
        file=sys.stderr,
    )

    pipeline = {"steps": [make_step(name, ray_version) for name in names]}
    yaml.safe_dump(pipeline, sys.stdout, sort_keys=False, default_flow_style=False)


if __name__ == "__main__":
    main()
