Image Search & Classification Tutorial
======================================

This tutorial provides a gentle introduction to Ray by demonstrating how to build a multimodal AI application using the Ray libraries. In this tutorial, we will create a simple dog breed classification and semantic search application that showcases Ray's capabilities for distributed computing. The tutorial will cover data processing with Ray Data, model training with Ray Train, and serving with Ray Serve. For each of these workloads, we will first start developing locally on your machine, and then scale out to run on a remote Ray cluster.

By the end of this tutorial, you will have a working, production-ready image classification and search application that can classify dog breeds and find similar images. You will also have a broad understanding of how the Ray libraries work, how they are powered by Ray's core capabilities for distributed computing, and the best practices for developing and scaling Ray applications.

.. toctree::
   :maxdepth: 2
   
   introduction
   data-processing
   training
   model-serving
   scaling-out
   production

The tutorial is structured as follows:

1. **Introduction**: Overview of Ray and its components
2. **Data Processing**: Using Ray Data for efficient data processing
3. **Training**: Training models with Ray Train
4. **Model Serving**: Deploying models with Ray Serve
5. **Scaling Out**: Scaling your application across multiple machines
6. **Production**: Deploying to production with Ray Jobs and Services

Each section builds upon the previous ones, creating a complete end-to-end application that demonstrates Ray's capabilities for building scalable AI applications.

Getting Started
---------------

To get started, proceed to the :doc:`introduction` section where we'll set up our environment and learn about Ray's core components. 