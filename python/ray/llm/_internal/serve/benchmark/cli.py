"""CLI entry point for the multi-turn OpenAI-compatible HTTP benchmark."""

import argparse
import sys


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ray.llm._internal.serve.benchmark",
        description="Multi-turn OpenAI-compatible HTTP benchmark",
    )

    ## Mode flags ##
    mode = parser.add_argument_group("mode")
    mode.add_argument(
        "-s", "--smoke",
        action="store_true",
        help="Smoke test (single request, exit)",
    )
    mode.add_argument(
        "-i", "--interactive",
        action="store_true",
        help="Interactive mode (server by default)",
    )
    mode.add_argument(
        "--client",
        action="store_true",
        help="Interactive client mode (used with -i)",
    )

    ## Server / API ##
    server = parser.add_argument_group("server/API")
    server.add_argument(
        "-u", "--base-url",
        default="http://127.0.0.1:8000",
        help="Base URL of the OpenAI-compatible API (default: %(default)s)",
    )
    server.add_argument(
        "-m", "--model",
        default="test-model",
        help="Model name to send in requests (default: %(default)s)",
    )
    server.add_argument(
        "--tokenizer",
        default=None,
        help="HuggingFace tokenizer name/path (default: same as --model)",
    )

    ## Workload ##
    workload = parser.add_argument_group("workload")
    workload.add_argument(
        "--isl",
        type=int,
        default=None,
        help="Average input sequence length",
    )
    workload.add_argument(
        "--hit-rate",
        type=float,
        default=None,
        help="Prefix cache hit rate [0, 1]",
    )
    workload.add_argument(
        "--num-turns",
        type=int,
        default=None,
        help="Number of turns per session",
    )
    workload.add_argument(
        "--osl",
        type=int,
        default=None,
        help="Output tokens per turn",
    )
    workload.add_argument(
        "--cross-sharing",
        type=float,
        default=1.0,
        help="Cross-session prefix sharing factor (default: %(default)s)",
    )
    workload.add_argument(
        "--think-time",
        type=float,
        default=0.0,
        help="Simulated user think-time between turns in seconds (default: %(default)s)",
    )
    workload.add_argument(
        "--chunk-size",
        type=int,
        default=16,
        help="Number of content chunks before recording first-chunk latency (default: %(default)s)",
    )

    ## Traffic ##
    traffic = parser.add_argument_group("traffic")
    traffic.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="Number of concurrent sessions",
    )
    traffic.add_argument(
        "--request-rate",
        type=float,
        default=None,
        help="Request rate (requests per second)",
    )
    traffic.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Duration in seconds",
    )
    traffic.add_argument(
        "--num-sessions",
        type=int,
        default=None,
        help="Total number of sessions to run",
    )
    traffic.add_argument(
        "--warm-up",
        type=float,
        default=0,
        help="Warm-up period in seconds (default: %(default)s)",
    )
    traffic.add_argument(
        "--ramp-interval",
        type=float,
        default=-1,
        help="Ramp interval in seconds (default: %(default)s)",
    )

    ## Interactive-only ##
    interactive = parser.add_argument_group("interactive-only")
    interactive.add_argument(
        "--status-interval",
        type=int,
        default=5,
        help="Status reporting interval in seconds (default: %(default)s)",
    )
    interactive.add_argument(
        "--cmd",
        type=str,
        default=None,
        help="Command to send in interactive client mode",
    )
    interactive.add_argument(
        "--log-failures",
        action="store_true",
        help="Log individual request failures",
    )
    interactive.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility",
    )
    interactive.add_argument(
        "--save-result",
        type=str,
        default=None,
        help="Filename to save results",
    )
    interactive.add_argument(
        "--save-dir",
        type=str,
        default=None,
        help="Directory to save results",
    )
    interactive.add_argument(
        "--num-workers",
        type=int,
        default=None,
        help="Number of worker tasks",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.smoke:
        from ray.llm._internal.serve.benchmark.multiturn_bench import run_smoke

        sys.exit(run_smoke(args))
    elif args.interactive and args.client:
        print(
            "Interactive client mode is not implemented yet.", file=sys.stderr
        )
        sys.exit(1)
    elif args.interactive:
        print(
            "Interactive server mode is not implemented yet.", file=sys.stderr
        )
        sys.exit(1)
    elif args.concurrency or args.request_rate:
        print(
            "Direct benchmark mode is not implemented yet.", file=sys.stderr
        )
        sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)
