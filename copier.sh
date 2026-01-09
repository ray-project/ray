#!/usr/bin/env bash
set -uo pipefail

# Default source: ray's site-packages location
DEFAULT_SRC=$(python -c "import ray; import os; print(os.path.dirname(ray.__path__[0]))")

# Parse arguments - if none provided, use default
if [[ $# -eq 0 ]]; then
    FILES=("$DEFAULT_SRC")
else
    FILES=("$@")
fi

PORT=2222
SSH_OPTS=(-p "$PORT" -o StrictHostKeyChecking=accept-new)
SCP_OPTS=(-P "$PORT" -o StrictHostKeyChecking=accept-new)

echo "Files to copy:"
for f in "${FILES[@]}"; do
    echo "  - $f"
done
echo ""

# Get node IPs using Python/Ray
echo "==> Getting node IPs from Ray cluster..."
NODE_INFO=$(python -c "
import ray
ray.init(ignore_reinit_error=True)
nodes = ray.nodes()
head_ip = None
worker_ips = []
for node in nodes:
    if node['Alive']:
        ip = node['NodeManagerAddress']
        if node.get('Resources', {}).get('node:__internal_head__'):
            head_ip = ip
        else:
            worker_ips.append(ip)
# If no explicit head marker, use first node as head
if head_ip is None and worker_ips:
    head_ip = worker_ips.pop(0)
print(f'HEAD:{head_ip or \"\"}')
for ip in worker_ips:
    print(f'WORKER:{ip}')
" 2>/dev/null)

if [[ -z "$NODE_INFO" ]]; then
    echo "Error: Could not get node info from Ray. Is Ray running?"
    exit 1
fi

echo "$NODE_INFO"
echo ""

# Parse head and worker IPs
HEAD_IP=$(echo "$NODE_INFO" | grep "^HEAD:" | cut -d: -f2)
WORKER_IPS=()
while IFS= read -r line; do
    ip=$(echo "$line" | cut -d: -f2)
    if [[ -n "$ip" ]]; then
        WORKER_IPS+=("$ip")
    fi
done <<< "$(echo "$NODE_INFO" | grep "^WORKER:")"

if [[ -z "$HEAD_IP" ]]; then
    echo "Error: Could not determine head node IP"
    exit 1
fi

echo "Head node IP: $HEAD_IP (this machine)"

if [[ ${#WORKER_IPS[@]} -eq 0 ]]; then
    echo "No worker nodes found. Nothing to copy."
    exit 0
fi

echo "Worker node IPs: ${WORKER_IPS[*]}"
echo ""

# Re-enable exit on error for the copy operations
set -e

# Copy from this machine (head node) to each worker node
for worker_ip in "${WORKER_IPS[@]}"; do
    echo "==> Copying to worker ($worker_ip)"

    for src in "${FILES[@]}"; do
        dst="$src"  # Same path on destination
        echo "    Copying: $src"

        # Note: $dst expands locally (intentional - we pass the value to remote)
        # shellcheck disable=SC2029
        ssh "${SSH_OPTS[@]}" "$worker_ip" "mkdir -p '$(dirname "$dst")'"

        # Note: $dst expands locally, \$(date +%s) expands on remote (intentional)
        # shellcheck disable=SC2029
        ssh "${SSH_OPTS[@]}" "$worker_ip" "test -e '$dst' && cp -r '$dst' '${dst}.bak.\$(date +%s)' || true"

        # Copy from head (local) to worker
        scp -r "${SCP_OPTS[@]}" "$src" "$worker_ip:$dst"
    done

    echo "    Done copying to $worker_ip"
done

echo ""
echo "All copies complete."
