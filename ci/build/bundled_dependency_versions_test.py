import ast
from pathlib import Path

REPO_ROOT = Path(__file__).absolute().parents[2]


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
        "aiohttp==3.14.3",
        "idna==3.15",
    ]
