Key Concepts
============
..
    TODO(cade) Can we simplify this? From https://github.com/ray-project/ray/pull/26754#issuecomment-1192927645:
    * Worker Nodes
    * Head Node
    * Autoscaler
    * Clients and Jobs
    
    Need to add the following sections + break out existing content into them.
    See ray-core/user-guide.rst for a TOC example
    
    overview
    high-level-architecture
    jobs
    nodes-vs-workers
    scheduling-and-autoscaling
    configuration
    Things-to-know
    .
    Yeah I think this is implementation detail (making key-concepts work for both). The few places I can think of:
    .
    nodes vs pods. Can tackle this in bullet points under node concept.
    autoscaler location: head node vs kuberay pod. Can have a couple bullet points here too.
    Btw, the key concepts section needs to be trimmed way down. It's supposed to be a single, concise page of key concepts. Right now, it's introducing a lot of other stuff that isn't really key (there's like 10 things). I think you can keep it to:
    .
    Worker Nodes
    Head Node
    Autoscaler
    Clients and Jobs
    The other sub-sections can be moved under user guides in the K8s/VM specific sections.

.. include:: /_includes/clusters/we_are_hiring.rst

.. _cluster-key-concepts-under-construction:

..
  .. _cluster-key-concepts-under-construction:
  
  Cluster
  -------
  
  A Ray cluster is a set of one or more nodes that are running Ray and share the
  same :ref:`head node<cluster-node-types>`.
  
  .. _cluster-node-types-under-construction:
  
  Node types
  ----------
  
  A Ray cluster consists of a :ref:`head node<cluster-head-node>` and a set of
  :ref:`worker nodes<cluster-worker-node>`.
  
  .. image:: ray-cluster.jpg
      :align: center
      :width: 600px

.. _cluster-head-node-under-construction:

Head node
~~~~~~~~~

The head node is the first node started by the Ray cluster launcher when trying
to launch a Ray
cluster. Among other things, the head node holds the :ref:`Global Control Store
(GCS)<memory>` and runs the :ref:`autoscaler<cluster-autoscaler>`. Once the head
node is started, it will be responsible for launching any additional
:ref:`worker nodes<cluster-worker-node>`. The head node itself will also execute
tasks and actors to utilize its capacity.

.. _cluster-worker-node-under-construction:

Worker node
~~~~~~~~~~~

A worker node is any node in the Ray cluster that is not functioning as head node.
Therefore, worker nodes are simply responsible for executing tasks and actors.
When a worker node is launched, it will be given the address of the head node to
form a cluster.

.. _cluster-autoscaler-under-construction:

Autoscaler
----------

The autoscaler is a process that runs on the :ref:`head node<cluster-head-node>`
and is responsible for adding or removing :ref:`worker nodes<cluster-worker-node>`
to meet the needs of the Ray workload while matching the specification in the
:ref:`cluster config file<cluster-config>`. In particular, if the resource
demands of the Ray workload exceed the current capacity of the cluster, the
autoscaler will try to add nodes. Conversely, if a node is idle for long enough,
the autoscaler will remove it from the cluster. To learn more about autoscaling,
refer to the :ref:`Ray cluster deployment guide<deployment-guide-autoscaler>`.

Clients and Jobs
----------------

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras aliquet congue diam in ultricies. Duis feugiat non est sit amet tincidunt. Sed eget leo tempor, tempus mi quis, sollicitudin orci. Vivamus cursus et enim ac interdum. Ut magna ligula, suscipit id justo finibus, pharetra elementum lacus. Etiam tristique vulputate lacus, vel suscipit augue hendrerit nec. Praesent hendrerit scelerisque mi.

Vivamus id neque risus. Curabitur sed enim fringilla, lacinia erat nec, finibus purus. In ornare diam feugiat sapien elementum porttitor. Praesent sodales tristique nibh quis efficitur. Mauris maximus porta nisi ac pretium. Donec quis nulla nibh. Maecenas ac auctor arcu. Pellentesque id nulla at massa tempus condimentum id nec ligula. Suspendisse aliquet scelerisque libero quis rhoncus. Quisque tempus aliquam tortor ac vehicula. Aliquam erat volutpat. Donec lectus est, consectetur ut dolor non, volutpat posuere nisi.

..
    Ray Client
    ----------
    The Ray Client is an API that connects a Python script to a remote Ray cluster.
    To learn more about the Ray Client, you can refer to the :ref:`documentation<ray-client>`.
    
    Job submission
    --------------
    
    Ray Job submission is a mechanism to submit locally developed and tested applications
    to a remote Ray cluster. It simplifies the experience of packaging, deploying,
    and managing a Ray application. To learn more about Ray jobs, refer to the
    :ref:`documentation<ray-job-submission-api-ref>`.
