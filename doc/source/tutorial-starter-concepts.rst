Tutorial Starter Concepts Guide
===============================

Understanding how Ray works requires some background knowledge on parallel 
programming and distributed systems in general. If you are new to these concepts, 
here is a brief glossary page with explanations for some of the different 
terminology you may encounter. Otherwise, you may skip onto the rest of the Ray 
documentation.

.. contents:: Concepts Explained
    :depth: 2

Parallel Computing
------------------

Parallel computing is the concept that multiple processors can share memory and 
data to work together. Since additional processors can independently complete other 
work left to do while one processor is busy working on a single part, this can 
speed up computation for a single program. 

For example, if you want to process each element in an array, you may be able to 
parallelize that work among four processors by first putting the array in 
shared memory accessible by all four processors, then have each processor 
work on 1/4th of the array at the same time. 

Now, processing the array will only take the time to process 1/4th of the array. 
While there is an additional time **overhead** in sharing the work amongst the 
four processors in the first place, in time-consuming computational tasks such 
as in machine learning, this overhead is minimal in comparison. 

In general, parallel computing gives a factor of speedup no more than the 
number of parallel processors.

Workers, Nodes, and Clusters
----------------------------

In a distributed system, a collection of computers may act, work, and maintain the 
appearance of a single large and powerful computer. This collection is called 
a **cluster.** 

Each individual computer in the cluster is called a **node.** Nodes that 
carry out the main computing tasks requested in a distributed program are called 
**workers** (as opposed to nodes that manage the cluster itself).

Multitasking-- Tasks, Processes, Schedulers
-------------------------------------------

**Multitasking** is the ability for a system to run multiple computing jobs at once.
These computing jobs are called **tasks.** Each task is handled by a **process.** 

True multitasking is possible due to many modern computers having multiple 
**processors,** or computing units. However, the number of processes that can 
exist to handle tasks are not limited to the number of processors your 
computing system has, since multiple processes can time-share on the same 
processor. 

This means that Ray can be given more tasks to parallelize than 
physical processors in your machine, although since they must time-share 
there still wouldn't be more speedup than the number of processors available.

When there are multiple tasks to handle, a **scheduler** is needed to decide 
which task is handled by which process and which processes run at any given 
time. 

In a distributed system, there may be **local schedulers,** which handle 
assigning tasks to processes within a node, and **global schedulers,** which 
handle assigning tasks over the whole cluster.

Asychronous Program Execution-- Blocking, Futures
-------------------------------------------------

Within a single execution thread, only one operation may be active at a time. 
Functions may **block,** which is when they wait for something necessary to happen 
before returning. 

However, Ray relies on **asynchronous** function calls, where a function 
returns before it is finished. Instead, the function causes work to happen in 
the background in preparation for the program's use sometime in the future.
Asynchronous execution allows for the control flow of a program to continue 
without blocking and having to wait for functions to return.

While there are many styles of asynchronous programming, such as callbacks, Ray does 
asynchronous functions in the style of **futures** or promises, where the function 
returns a placeholder value to be filled in later.

Key-Value Object Store
----------------------

In a key-value object store, objects may be accessed by a unique object ID, 
similar to using **keys** to access **values** inside dictionaries in Python, 
but on a larger database-level scale.

Serialization
-------------

You will not need knowledge of Python's specific serialization module, `pickle`_, 
unless you delve into the Ray Design `Object Store documentation`_. 
However, you should be aware that serialization is a mechanism for 
translating (Python) objects into other formats for storing and communicating.

.. _`pickle`: https://docs.python.org/2/library/pickle.html
.. _`Object Store documentation`: http://ray.readthedocs.io/en/latest/serialization.html
