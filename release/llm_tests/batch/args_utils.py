import argparse


def get_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--tp-size",
        type=int,
        default=1,
        help="Tensor parallel size",
    )
    parser.add_argument(
        "--pp-size",
        type=int,
        default=1,
        help="Pipeline parallel size.",
    )
    parser.add_argument(
        "--concurrency", type=int, default=1, help="Number of concurrency (replicas)."
    )
    parser.add_argument(
        "--vllm-use-v1",
        action="store_true",
        default=False,
    )
    return parser
