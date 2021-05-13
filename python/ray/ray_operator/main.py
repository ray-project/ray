import subprocess

from python.ray.ray_operator.operator_utils import OPERATOR_NAMESPACE, NAMESPACED_OPERATOR


def main() -> None:
    args = ["--namespace", OPERATOR_NAMESPACE] if NAMESPACED_OPERATOR else ["--all-namespaces"]
    subprocess.run(
        ["kopf", "run", *args, "-m", "ray.ray_operator.operator"],
        check=True,
    )


if __name__ == "__main__":
    main()
