import subprocess

from ray.ray_operator.operator_utils import NAMESPACED_OPERATOR
from ray.ray_operator.operator_utils import OPERATOR_NAMESPACE


def main() -> None:
    args = ["--namespace", OPERATOR_NAMESPACE] if NAMESPACED_OPERATOR else [
        "--all-namespaces"
    ]
    subprocess.run(
        ["kopf", "run", *args, "-m", "ray.ray_operator.operator"],
        check=True,
    )


if __name__ == "__main__":
    main()
