.. _ref-e2e-multimodal-ai-workloads:

End-to-end Multimodal AI Workloads
==================================

While it may seem like this is a simple image semantic-search and classification demo — it’s really a deep dive into why Ray is the ideal platform for performant, reliable, and scalable multimodal AI workloads. Ray Data helps build a heterogeneous CPU/GPU pipeline that ingests images and runs large-scale batch inference to produce embeddings. Next, Ray Train offers distributed, fault-tolerant, multi-node, multi-GPU training—complete with experiment orchestration and model artifact management. Finally, Ray Serve deploys an embedding-based search service, achieving low-latency, autoscaling inference at production scale. Along the way, you’ll see how Ray’s unified resource management, autoscaling, and observability capabilities let you not only implement — but continuously monitor and optimize — every stage of your multimodal AI workflow.

.. toctree::
   :maxdepth: 2

   ./README.ipynb
   ./notebooks/01-Batch-Inference.ipynb
   ./notebooks/02-Distributed-Training.ipynb
   ./notebooks/03-Online-Serving.ipynb
