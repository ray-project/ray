import argparse
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import ray
import random
import ray.util

# The goal of the this script is to simulate cross AZ transient network failures periodically on a Ray job.
# We do this by modifying the iptables to drop all inbound and outbound traffic for a given duration
# except for intra-node and SSH traffic. After the duration, the iptables rules are restored.
# The failure script is run in a background thread while the main command is run in the foreground.
# NOTE: The script itself does not spin up a Ray cluster, it operates on the assumption that an existing
# Ray cluster is running and we are able to SSH into the nodes (like on Anyscale).

PARALLEL = 500  # concurrent SSH sessions
SSH_USER = "ubuntu"  # Anyscale default
AFFECT_WORKER_RATIO = 0.50  # failure affects 50% of worker nodes
EXTRA_SSH = [
    "-o",
    "BatchMode=yes",
    "-o",
    "StrictHostKeyChecking=accept-new",
    "-o",
    "ConnectTimeout=10",
]


def iptables_cmd(self_ip: str, seconds: int) -> str:
    return f"""\
nohup setsid bash -lc '
  sudo iptables -w -A INPUT  -p tcp --dport 22 -j ACCEPT
  sudo iptables -w -A OUTPUT -p tcp --sport 22 -j ACCEPT
  sudo iptables -w -A INPUT  -s 127.0.0.0/8 -d 127.0.0.0/8 -j ACCEPT
  sudo iptables -w -A OUTPUT -s 127.0.0.0/8 -d 127.0.0.0/8 -j ACCEPT
  sudo iptables -w -A INPUT  -s {self_ip} -d {self_ip} -j ACCEPT
  sudo iptables -w -A OUTPUT -s {self_ip} -d {self_ip} -j ACCEPT
  sudo iptables -w -A INPUT  -j DROP
  sudo iptables -w -A OUTPUT -j DROP
  sleep {seconds}
  sudo iptables -w -D OUTPUT -j DROP
  sudo iptables -w -D INPUT  -j DROP
  sudo iptables -w -D OUTPUT -s {self_ip} -d {self_ip} -j ACCEPT
  sudo iptables -w -D INPUT  -s {self_ip} -d {self_ip} -j ACCEPT
  sudo iptables -w -D OUTPUT -s 127.0.0.0/8 -d 127.0.0.0/8 -j ACCEPT
  sudo iptables -w -D INPUT  -s 127.0.0.0/8 -d 127.0.0.0/8 -j ACCEPT
  sudo iptables -w -D OUTPUT -p tcp --sport 22 -j ACCEPT
  sudo iptables -w -D INPUT  -p tcp --dport 22 -j ACCEPT
' &>/dev/null &
"""


def ssh_run(ip: str, cmd: str) -> tuple[bool, str]:
    """Run SSH command on remote host."""
    target = f"{SSH_USER}@{ip}"
    res = subprocess.run(
        ["ssh", *EXTRA_SSH, target, cmd], capture_output=True, text=True
    )
    ok = res.returncode == 0
    msg = res.stdout.strip() if ok else (res.stderr.strip() or res.stdout.strip())
    return ok, msg


def simulate_cross_az_network_failure(seconds: int):
    if not ray.is_initialized():
        ray.init(address="auto")

    nodes = ray.nodes()
    all_ips = [n["NodeManagerAddress"] for n in nodes if n.get("Alive", False)]
    # Always inject failures on the head node
    head_ip = next(
        (
            n["NodeManagerAddress"]
            for n in nodes
            if n.get("NodeManagerAddress") == ray.util.get_node_ip_address()
        ),
        None,
    )

    print(f"Discovered {len(all_ips)} alive nodes")
    print(f"Head node: {head_ip}")

    worker_ips = [ip for ip in all_ips if ip != head_ip]
    print(f"Eligible worker nodes: {len(worker_ips)}")
    if not worker_ips:
        print("ERROR: No worker nodes found")
        return

    k = max(1, int(len(worker_ips) * AFFECT_WORKER_RATIO))
    affected = random.sample(worker_ips, k)
    # NOTE: When running this script on Anyscale with longer failure durations the blacked out head node could
    # cause your workspace to lag and die. To avoid this, comment out the below line.
    affected.append(head_ip)
    print(
        f"Affecting {len(affected)} nodes (~{AFFECT_WORKER_RATIO*100:.0f}% of workers + head node):"
    )
    print(", ".join(affected[:10]) + (" ..." if len(affected) > 10 else ""))

    cmds = {ip: iptables_cmd(ip, seconds) for ip in affected}

    print(f"\nTriggering {seconds}s of transient network failure...")
    successes, failures = [], {}

    with ThreadPoolExecutor(max_workers=PARALLEL) as ex:
        futs = {ex.submit(ssh_run, ip, cmds[ip]): ip for ip in affected}
        for fut in as_completed(futs):
            ip = futs[fut]
            try:
                ok, msg = fut.result()
                if ok:
                    successes.append(ip)
                else:
                    failures[ip] = msg
            except Exception as e:
                failures[ip] = str(e)

    print("\n=== Summary ===")
    print(f"Succeeded: {len(successes)} nodes")
    print(f"Failed   : {len(failures)} nodes")
    if failures:
        for ip, msg in list(failures.items()):
            print(f"  {ip}: {msg}")


def network_failure_loop(interval, network_failure_duration):
    """
    Run the network failure loop in a background thread at regular intervals.

    Args:
        interval: Interval in seconds between network failure events
        network_failure_duration: Duration in seconds of each network failure
    """
    print(
        f"[NETWORK FAILURE {time.strftime('%H:%M:%S')}] Starting network failure thread with interval: {interval} seconds"
    )

    while True:
        # Sleep for the interval duration
        time.sleep(interval)

        # Simulate a network failure
        print(
            f"[NETWORK FAILURE {time.strftime('%H:%M:%S')}] Triggering network failure simulation..."
        )
        try:
            simulate_cross_az_network_failure(network_failure_duration)
        except Exception as e:
            print(
                f"[NETWORK FAILURE {time.strftime('%H:%M:%S')}] ERROR: Network failure simulation failed: {e}"
            )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run benchmark with network failure injection at regular intervals",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run map_benchmark with network failures injected every 300 seconds, each lasting 5 seconds
  python simulate_cross_az_network_failure.py --network-failure-interval 300 --network-failure-duration 5 --command python map_benchmark.py --api map_batches --sf 1000
        """,
    )
    parser.add_argument(
        "--network-failure-interval",
        type=int,
        required=True,
        help="Interval in seconds between network failure events",
    )
    parser.add_argument(
        "--network-failure-duration",
        type=int,
        required=True,
        help="Duration in seconds of each network failure",
    )
    parser.add_argument(
        "--command",
        nargs=argparse.REMAINDER,
        required=True,
        help="The main command to run (e.g., 'python map_benchmark.py --api map_batches ...')",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Validate command (argparse catches missing --command, but not empty --command)
    if not args.command:
        print("ERROR: --command requires at least one argument")
        print(
            "Usage: python simulate_cross_az_network_failure.py --network-failure-interval <seconds> --network-failure-duration <seconds> --command <command>"
        )
        sys.exit(1)

    print("=" * 80)
    print("Running with Network Failure Injection")
    print("=" * 80)
    print(f"Network failure interval: {args.network_failure_interval} seconds")
    print(f"Network failure duration: {args.network_failure_duration} seconds")
    print(f"Command: {' '.join(args.command)}")
    print("=" * 80)
    print()

    # Start network failure thread as daemon - it will die with the process
    network_failure_thread = threading.Thread(
        target=network_failure_loop,
        args=(args.network_failure_interval, args.network_failure_duration),
        daemon=True,
    )
    network_failure_thread.start()

    try:
        # Run the main command in the foreground
        print(
            f"[MAIN {time.strftime('%H:%M:%S')}] Starting command: {' '.join(args.command)}"
        )
        main_result = subprocess.run(args.command)
        print(
            f"\n[MAIN {time.strftime('%H:%M:%S')}] Command completed with exit code: {main_result.returncode}"
        )
        exit_code = main_result.returncode

    except KeyboardInterrupt:
        print("\n[MAIN] Interrupted by user")
        exit_code = 130

    except Exception as e:
        print(f"[MAIN] ERROR: {e}")
        exit_code = 1

    print("\n" + "=" * 80)
    print(f"Execution completed with exit code: {exit_code}")
    print("=" * 80)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
