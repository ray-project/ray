.. _compiled-graph-overlap:

Experimental: Overlapping communication and computation
=======================================================

Compiled Graph currently provides experimental support for GPU communication and computation overlap. When you turn this feature on, it automatically overlaps the GPU communication with computation operations, thereby hiding the communication overhead and improving performance.

To enable this feature, specify ``_overlap_gpu_communication=True`` when calling :func:`dag.experimental_compile() <ray.dag.DAGNode.experimental_compile>`.

The following code has GPU communication and computation operations that benefit
from overlapping.

.. literalinclude:: ../doc_code/cgraph_overlap.py
    :language: python
    :start-after: __cgraph_overlap_start__
    :end-before: __cgraph_overlap_end__

The output of the preceding code includes the following two lines:

.. testoutput::

    overlap_gpu_communication=False, duration=1.0670117866247892
    overlap_gpu_communication=True, duration=0.9211348341777921

The actual performance numbers may vary on different hardware, but enabling ``_overlap_gpu_communication`` improves latency by about 14% for this example.
