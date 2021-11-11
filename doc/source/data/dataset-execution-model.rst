Dataset Execution Model
=======================

This page overviews the execution model of Datasets in detail, which may be useful for understanding and tuning performance.


Parallel Read
-------------
talk about datasource and read task

Lazy Reads
~~~~~~~~~~
read task not executed until needed for schema(), exponential rampup

Tuning Read Parallelism
~~~~~~~~~~~~~~~~~~~~~~~
fractional num cpus




Parallel Transformation
-----------------------
task vs actor compute provider

Locality
~~~~~~~~
locality scheduling for tasks




Distributed Shuffle
-------------------
talk about groupby, sort, random shuffle, fast repartition



Memory Management
-----------------
talk about blocks representation

Dynamic block splitting
~~~~~~~~~~~~~~~~~~~~~~~
clipped by max target block size

Data Balancing
~~~~~~~~~~~~~~
Ray tries to spread read tasks out to balance memory usage

Execution Memory
~~~~~~~~~~~~~~~~
talk about how to tune this

Spilling
~~~~~~~~
spill to object store on out of memory

Reference Counting
~~~~~~~~~~~~~~~~~~
mention how blocks are pinned until deleted
