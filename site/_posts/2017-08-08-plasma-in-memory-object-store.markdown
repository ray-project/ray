---
layout: post
title: "The Plasma In-Memory Object Store"
excerpt: "This post announces Plasma, an in-memory object store for communicating data between processes."
date: 2017-08-08 00:00:00
---

*This was originally posted on the [Apache Arrow blog][1]*.

This blog post presents Plasma, an in-memory object store that is being
developed as part of Apache Arrow. **Plasma holds immutable objects in shared
memory so that they can be accessed efficiently by many clients across process
boundaries.** In light of the trend toward larger and larger multicore machines,
Plasma enables critical performance optimizations in the big data regime.

Plasma was initially developed as part of [Ray][2], and has recently been moved
to Apache Arrow in the hopes that it will be broadly useful.

One of the goals of Apache Arrow is to serve as a common data layer enabling
zero-copy data exchange between multiple frameworks. A key component of this
vision is the use of off-heap memory management (via Plasma) for storing and
sharing Arrow-serialized objects between applications.

**Expensive serialization and deserialization as well as data copying are a
common performance bottleneck in distributed computing.** For example, a
Python-based execution framework that wishes to distribute computation across
multiple Python “worker” processes and then aggregate the results in a single
“driver” process may choose to serialize data using the built-in `pickle`
library. Assuming one Python process per core, each worker process would have to
copy and deserialize the data, resulting in excessive memory usage. The driver
process would then have to deserialize results from each of the workers,
resulting in a bottleneck.

Using Plasma plus Arrow, the data being operated on would be placed in the
Plasma store once, and all of the workers would read the data without copying or
deserializing it (the workers would map the relevant region of memory into their
own address spaces). The workers would then put the results of their computation
back into the Plasma store, which the driver could then read and aggregate
without copying or deserializing the data.

### The Plasma API:

Below we illustrate a subset of the API. The C++ API is documented more fully
[here][5], and the Python API is documented [here][6].

**Object IDs:** Each object is associated with a string of bytes.

**Creating an object:** Objects are stored in Plasma in two stages. First, the
object store *creates* the object by allocating a buffer for it. At this point,
the client can write to the buffer and construct the object within the allocated
buffer. When the client is done, the client *seals* the buffer making the object
immutable and making it available to other Plasma clients.

```python
# Create an object.
object_id = pyarrow.plasma.ObjectID(20 * b'a')
object_size = 1000
buffer = memoryview(client.create(object_id, object_size))

# Write to the buffer.
for i in range(1000):
    buffer[i] = 0

# Seal the object making it immutable and available to other clients.
client.seal(object_id)
```

**Getting an object:** After an object has been sealed, any client who knows the
object ID can get the object.

```python
# Get the object from the store. This blocks until the object has been sealed.
object_id = pyarrow.plasma.ObjectID(20 * b'a')
[buff] = client.get([object_id])
buffer = memoryview(buff)
```

If the object has not been sealed yet, then the call to `client.get` will block
until the object has been sealed.

### A sorting application

To illustrate the benefits of Plasma, we demonstrate an **11x speedup** (on a
machine with 20 physical cores) for sorting a large pandas DataFrame (one
billion entries). The baseline is the built-in pandas sort function, which sorts
the DataFrame in 477 seconds. To leverage multiple cores, we implement the
following standard distributed sorting scheme.

* We assume that the data is partitioned across K pandas DataFrames and that
  each one already lives in the Plasma store.
* We subsample the data, sort the subsampled data, and use the result to define
  L non-overlapping buckets.
* For each of the K data partitions and each of the L buckets, we find the
  subset of the data partition that falls in the bucket, and we sort that
  subset.
* For each of the L buckets, we gather all of the K sorted subsets that fall in
  that bucket.
* For each of the L buckets, we merge the corresponding K sorted subsets.
* We turn each bucket into a pandas DataFrame and place it in the Plasma store.

Using this scheme, we can sort the DataFrame (the data starts and ends in the
Plasma store), in 44 seconds, giving an 11x speedup over the baseline.

### Design

The Plasma store runs as a separate process. It is written in C++ and is
designed as a single-threaded event loop based on the [Redis][3] event loop library.
The plasma client library can be linked into applications. Clients communicate
with the Plasma store via messages serialized using [Google Flatbuffers][4].

### Call for contributions

Plasma is a work in progress, and the API is currently unstable. Today Plasma is
primarily used in [Ray][2] as an in-memory cache for Arrow serialized objects.
We are looking for a broader set of use cases to help refine Plasma's API. In
addition, we are looking for contributions in a variety of areas including
improving performance and building other language bindings. Please let us know
if you are interested in getting involved with the project.

[1]: http://arrow.apache.org/blog/2017/08/08/plasma-in-memory-object-store/
[2]: https://github.com/ray-project/ray
[3]: https://redis.io/
[4]: https://google.github.io/flatbuffers/
[5]: https://github.com/apache/arrow/blob/master/cpp/apidoc/tutorials/plasma.md
[6]: https://github.com/apache/arrow/blob/master/python/doc/source/plasma.rst
