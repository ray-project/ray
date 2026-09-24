# Real captures, not fixtures anyone wrote

Taken from a 4-node cluster of 1x A10G (driver 580.126.20, NCCL 2.28.9,
torch 2.11.0+cu128) with `release/train_tests/health/01_nccl_ras.py`.

- `ncclras_a10g_mismatch.json` — a 4-rank job where rank 1 stopped calling the
  collective. Rank 1 is at AllReduce 100, the others at 101, and every rank is
  still `RUNNING`. This is the exact shape `mismatched_comms` keys on.
- `ncclras_a10g_mismatch.txt` — the same moment, `-f text`. Names the culprit
  outright: *"Rank 1 has launched up to operation 100 -- GPU 0 managed by
  process 6247 on node 10.0.109.86"*.
- `nvidia-smi_a10g_healthy.txt` — a healthy A10G. Its value is the field names:
  `_parse_nvidia_smi` was originally written against invented text and got
  several of them wrong, silently reporting every GPU clean.

Re-capture on new hardware rather than editing these by hand.
