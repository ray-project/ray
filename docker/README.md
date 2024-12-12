Overview of how the ray images are built:

Images without a "-cpu" or "-gpu" tag are built on ``ubuntu:22.04``. They are just an alias for **-cpu** (e.g. ``ray:latest`` is the same as ``ray:latest-cpu``).

```
ubuntu:22.04
└── base-deps:cpu
    └── ray:cpu

nvidia/cuda
└── base-deps:gpu
    └── ray:gpu
```
