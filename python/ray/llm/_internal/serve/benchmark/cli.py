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
        "-s",
        "--smoke",
        action="store_true",
        help="Smoke test (single request, exit)",
    )
    mode.add_argument(
        "-i",
        "--interactive",
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
        "-u",
        "--base-url",
        default="http://127.0.0.1:8000",
        help="Base URL of the OpenAI-compatible API (default: %(default)s)",
    )
    server.add_argument(
        "-m",
        "--model",
        required=True,
        help="Model name to send in requests",
    )
    server.add_argument(
        "--tokenizer",
        default=None,
        help="HuggingFace tokenizer name/path (default: same as --model)",
    )
    server.add_argument(
        "--api-key",
        default=None,
        help="API key for Authorization header (default: None)",
    )

    ## Workload ##
    workload = parser.add_argument_group("workload")
    workload.add_argument(
        "--isl",
        type=int,
        default=1000,
        help="Average input sequence length (default: %(default)s)",
    )
    workload.add_argument(
        "--hit-rate",
        type=float,
        default=0.5,
        help="Prefix cache hit rate [0, 1] (default: %(default)s)",
    )
    workload.add_argument(
        "--num-turns",
        type=int,
        default=1,
        help="Number of turns per session (default: %(default)s)",
    )
    workload.add_argument(
        "--osl",
        type=int,
        default=100,
        help="Output tokens per turn (default: %(default)s)",
    )
    workload.add_argument(
        "--shared-system-prompt-ratio",
        dest="shared_system_prompt_ratio",
        type=float,
        default=1.0,
        help="Fraction of the system prompt shared across all sessions "
        "(1.0 = identical, 0.0 = all unique) (default: %(default)s)",
    )
    workload.add_argument(
        "--think-time",
        type=float,
        default=0.0,
        help="Simulated user think-time between turns in seconds (default: %(default)s)",
    )
    workload.add_argument(
        "-fc",
        "--first-chunk-threshold",
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
        "--warmup-jitter-max",
        type=float,
        default=10.0,
        help="Max random delay (seconds) between turns during entropy warm-up "
        "in concurrency mode. Jitter desynchronizes sessions so the benchmark "
        "reaches steady-state faster (default: %(default)s)",
    )
    traffic.add_argument(
        "--ramp-interval",
        type=float,
        default=-1,
        help="Seconds between launching successive sessions at benchmark start. "
        "Use this to avoid a thundering-herd of simultaneous first requests. "
        "-1 = auto-derive from request rate or concurrency (default: %(default)s)",
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
        default=1,
        help="Number of process-pool workers for conversation generation (default: %(default)s)",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.smoke:
        from ray.llm._internal.serve.benchmark.multiturn_bench import run_smoke

        sys.exit(run_smoke(args))
    elif args.interactive and args.client:
        from ray.llm._internal.serve.benchmark.interactive import run_interactive_client

        sys.exit(run_interactive_client(args))
    elif args.interactive:
        from ray.llm._internal.serve.benchmark.interactive import run_interactive_server

        sys.exit(run_interactive_server(args))
    elif args.concurrency or args.request_rate:
        from ray.llm._internal.serve.benchmark.multiturn_bench import run_direct

        sys.exit(run_direct(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
