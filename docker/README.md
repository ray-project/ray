Overview of how the ray images are built:

Images without a "-cpu" or "-gpu" tag are built on ``ubuntu/focal``. They are just an alias for **-cpu** (e.g. ``ray:latest`` is the same as ``ray:latest-cpu``).

```
ubuntu/focal
└── base-deps:cpu
    └── ray-deps:cpu
        └── ray:cpu
            └── ray-ml:cpu

nvidia/cuda
└── base-deps:gpu
    └── ray-deps:gpu
        └── ray:gpu
            └── ray-ml:gpu
```
