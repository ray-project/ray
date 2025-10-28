#!/usr/bin/env python3
import random
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

import ray

# The goal of the this script is to simulate cross AZ transient network failures.
# We do this by modifying the iptables to drop all inbound and outbound traffic for a given duration
# except for intra-node and SSH traffic. After the duration, the iptables rules are restored.
# NOTE: The script itself does not spin up a Ray cluster, it operates on the assumption that an existing
# Ray cluster is running and we are able to SSH into the nodes (like on Anyscale).


SECONDS = 5  # blackout duration (seconds)
PARALLEL = 500  # concurrent SSH sessions
SSH_USER = "ubuntu"  # Anyscale default
AFFECT_WORKER_RATIO = 0.50  # blackout affects 50% of worker nodes
EXTRA_SSH = [
    "-o",
    "BatchMode=yes",
    "-o",
    "StrictHostKeyChecking=accept-new",
    "-o",
    "ConnectTimeout=10",
]


def iptables_cmd(self_ip: str) -> str:
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
  sleep {SECONDS}
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
    target = f"{SSH_USER}@{ip}"
    res = subprocess.run(
        ["ssh", *EXTRA_SSH, target, cmd], capture_output=True, text=True
    )
    ok = res.returncode == 0
    msg = res.stdout.strip() if ok else (res.stderr.strip() or res.stdout.strip())
    return ok, msg


def main():
    ray.init(address="auto")

    nodes = ray.nodes()
    all_ips = [n["NodeManagerAddress"] for n in nodes if n.get("Alive", False)]
    # Causing a network failure on the head node causes the cluster to die on Anyscale so filter it out
    head_ip = next(
        (
            n["NodeManagerAddress"]
            for n in nodes
            if n.get("NodeManagerAddress")
            == ray.worker._global_node.address_info["node_ip_address"]
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
    print(
        f"Affecting {len(affected)} worker nodes (~{AFFECT_WORKER_RATIO*100:.0f}% of total):"
    )
    print(", ".join(affected[:10]) + (" ..." if len(affected) > 10 else ""))

    cmds = {ip: iptables_cmd(ip) for ip in affected}

    print(f"\nTriggering {SECONDS}s of transient network failure...")
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


if __name__ == "__main__":
    main()
