Advanced AsyncIO on Ray
=======================

It is an in-depth tutorial about how to use AsyncIO on Ray. To learn basic APIs, please take a look at `Ray AsyncIO APIs <https://docs.ray.io/en/latest/async_api.html>`_

Motivation
----------

Ray excels in CPU bounded workloads such as ML training or data processing, but what if workloads are IO bounded? The best solution for Python is to use `AsyncIO API <https://docs.python.org/3/library/asyncio.html>`_.  
Ray is nicely integrated to Python's AsyncIO APIs to support IO bounded workload.

In this tutorial, we'll learn about how AsyncIO works in Ray. After that, we'll build distributed web crawler together using AsyncIO with Ray.

AsyncIO
-------
AsyncIO is Python's standard library to enable async programming in Python. This tutorial will assume you already know about the basic of AsyncIO. Please take a look at this tutorial before reading this `tutorial <https://realpython.com/async-io-python/>`_ if you are not familiar with AsyncIO APIs.

