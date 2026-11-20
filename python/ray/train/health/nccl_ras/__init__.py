"""The NCCL hang detector, which Ray Train runs as an in-flight health check
with ``RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR=1``.

NCCL ships a Reliability/Availability/Serviceability (RAS) subsystem (NCCL
>= 2.24) that runs a monitoring thread inside every NCCL process. Those threads
form a peer mesh that tracks each communicator's ranks and how many collectives
each rank launched.

- ``NCCLRASProbe`` queries it with ``ncclras -f json`` on each worker node.
- ``NCCLHangEvaluator`` counts, for each communicator whose ranks launched
  different numbers of an op, the consecutive reports in which no rank
  advanced any op. It warns once a communicator is suspected, and confirms the
  hang after ``RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S``.
- A confirmed hang is diagnosed with the Flight Recorder, ``nvidia-smi``, the
  RAS text report and the stack traces. ``ConfirmedHangEvaluator`` uploads
  them, then reattempts the run (``RAY_TRAIN_NCCL_RAS_ACTION=fail``) or only
  logs the hang (``observe``).

Warning: RAS only knows about the ranks it can reach through its mesh, and the
mesh is built from the communicators each process has created. A query
therefore returns every rank reachable from the queried node, which is the
whole job only if some communicator spans all ranks, such as the default
process group ``torch.distributed`` creates.
"""
