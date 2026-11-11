#!/bin/bash
# Bounded glibc trim thresholds for HAProxy's 8 MiB buffers: observer vs off per config.
PY=/mnt/local_storage/sm/sm_env/bin/python
B=/mnt/local_storage/sm/haproxy_build/dist_final/haproxy
O=/mnt/local_storage/sm/runs/haproxy_tuning2
mkdir -p "$O"
cd /home/ray/default/sm_work || exit 1
for cfg in "plain:" \
           "t1g:--haproxy-env GLIBC_TUNABLES=glibc.malloc.trim_threshold=1073741824:glibc.malloc.mmap_threshold=33554432" \
           "t256m:--haproxy-env GLIBC_TUNABLES=glibc.malloc.trim_threshold=268435456:glibc.malloc.mmap_threshold=33554432" \
           "t64m:--haproxy-env GLIBC_TUNABLES=glibc.malloc.trim_threshold=67108864:glibc.malloc.mmap_threshold=33554432"; do
  name=${cfg%%:*}
  read -ra extra <<< "${cfg#*:}"
  timeout 600 "$PY" haproxy_latency.py --binary "$B" --reps 3 --duration 20 --window 6:18 "${extra[@]}" \
    --out "$O/$name" > "$O/$name.log" 2>&1
  echo "$name exit=$?"
done
