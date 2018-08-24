The Plasma Object Store
=======================

Plasma is a high-performance shared memory object store originally developed in
Ray and now being developed in `Apache Arrow`_. See the `relevant
documentation`_.

Using Plasma with Huge Pages
----------------------------

On Linux, it is possible to increase the write throughput of the Plasma object
store by using huge pages. You first need to create a file system and activate
huge pages as follows.

.. code-block:: shell

  sudo mkdir -p /mnt/hugepages
  gid=`id -g`
  uid=`id -u`
  sudo mount -t hugetlbfs -o uid=$uid -o gid=$gid none /mnt/hugepages
  sudo bash -c "echo $gid > /proc/sys/vm/hugetlb_shm_group"
  # This typically corresponds to 20000 2MB pages (about 40GB), but this
  # depends on the platform.
  sudo bash -c "echo 20000 > /proc/sys/vm/nr_hugepages"

**Note:** Once you create the huge pages, they will take up memory which will
never be freed unless you remove the huge pages. If you run into memory issues,
that may be the issue.

You need root access to create the file system, but not for running the object
store.

You can then start Ray with huge pages on a single machine as follows.

.. code-block:: python

  ray.init(huge_pages=True, plasma_directory="/mnt/hugepages")

In the cluster case, you can do it by passing ``--huge-pages`` and
``--plasma-directory=/mnt/hugepages`` into ``ray start`` on any machines where
huge pages should be enabled.

See the relevant `Arrow documentation for huge pages`_.

.. _`Apache Arrow`: https://arrow.apache.org/
.. _`relevant documentation`: https://arrow.apache.org/docs/python/plasma.html#the-plasma-in-memory-object-store
.. _`Arrow documentation for huge pages`: https://arrow.apache.org/docs/python/plasma.html#using-plasma-with-huge-pages
