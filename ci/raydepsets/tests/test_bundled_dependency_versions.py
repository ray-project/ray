import ast
import re
from pathlib import Path


REPO_ROOT = Path(__file__).absolute().parents[3]
LOG4J_ARTIFACTS = {
    "log4j-api",
    "log4j-core",
    "log4j-slf4j-impl",
}


def _assignment_value(module: ast.Module, name: str):
    for node in module.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return ast.literal_eval(node.value)
    raise AssertionError(f"{name} assignment not found")


def test_runtime_env_agent_bundle_uses_fixed_dependency_versions():
    setup_py = REPO_ROOT / "python" / "setup.py"
    module = ast.parse(setup_py.read_text())

    assert _assignment_value(module, "RUNTIME_ENV_AGENT_PIP_PACKAGES") == [
        "aiohttp==3.14.0",
        "idna==3.15",
    ]


def test_ray_dist_jar_uses_fixed_log4j_version():
    java_deps = (REPO_ROOT / "java" / "dependencies.bzl").read_text()
    log4j_versions = dict(
        re.findall(
            r"org\.apache\.logging\.log4j:(log4j-(?:api|core|slf4j-impl)):"
            r"([0-9][^\"\s]*)",
            java_deps,
        )
    )

    assert {
        artifact: log4j_versions[artifact] for artifact in LOG4J_ARTIFACTS
    } == {
        "log4j-api": "2.25.4",
        "log4j-core": "2.25.4",
        "log4j-slf4j-impl": "2.25.4",
    }
