"""Fault injection for the health loop, using the tools the vendors ship.

Three layers, matching where the fault actually lives:

- :mod:`~.nvrx` drives NVIDIA's Resiliency Extension fault menu inside the
  training process (hangs, wedged streams, frozen interpreters, crashes).
- :mod:`~.dcgm` injects synthetic device telemetry through ``dcgmi test
  --inject``, which is what NVSentinel's GPU health monitor reads -- the same
  call NVSentinel's own local demo makes.
- :mod:`~.symptoms` reports the numbers a fault *produces* straight from the
  training loop, for unit tests that exercise a detector without needing the
  fault to be real.

Nothing here is imported by the health loop itself. It is test machinery.
"""
