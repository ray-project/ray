:orphan:

.. include:: we_are_hiring.rst

.. _k8s-gpus:

GPU Usage with Kubernetes
=========================
This document provides some notes on GPU usage with Kubernetes.

To use GPUs on Kubernetes, you will need to configure both your Kubernetes setup and add additional values to your Ray cluster configuration.

For relevant documentation for GPU usage on different clouds, see instructions for `GKE`_, for `EKS`_, and for `AKS`_.

The `Ray Docker Hub <https://hub.docker.com/r/rayproject/>`_ hosts CUDA-based images packaged with Ray for use in Kubernetes pods.
For example, the image ``rayproject/ray-ml:nightly-gpu`` is ideal for running GPU-based ML workloads with the most recent nightly build of Ray.
Read :ref:`here<docker-images>` for further details on Ray images.

Using Nvidia GPUs requires specifying the relevant resource `limits` in the container fields of your Kubernetes configurations.
(Kubernetes `sets <https://kubernetes.io/docs/tasks/manage-gpus/scheduling-gpus/#using-device-plugins>`_
the GPU request equal to the limit.) The configuration for a pod running a Ray GPU image and
using one Nvidia GPU looks like this:

.. code-block:: yaml

  apiVersion: v1
  kind: Pod
  metadata:
   generateName: example-cluster-ray-worker
   spec:
    ...
    containers:
     - name: ray-node
       image: rayproject/ray:nightly-gpu
       ...
       resources:
        cpu: 1000m
        memory: 512Mi
       limits:
        memory: 512Mi
        nvidia.com/gpu: 1

GPU taints and tolerations
--------------------------
.. note::

  Users using a managed Kubernetes service probably don't need to worry about this section.

The `Nvidia gpu plugin`_ for Kubernetes applies `taints`_ to GPU nodes; these taints prevent non-GPU pods from being scheduled on GPU nodes.
Managed Kubernetes services like GKE, EKS, and AKS automatically apply matching `tolerations`_
to pods requesting GPU resources. Tolerations are applied by means of Kubernetes's `ExtendedResourceToleration`_ `admission controller`_.
If this admission controller is not enabled for your Kubernetes cluster, you may need to manually add a GPU toleration each of to your GPU pod configurations. For example,

.. code-block:: yaml

  apiVersion: v1
  kind: Pod
  metadata:
   generateName: example-cluster-ray-worker
   spec:
   ...
   tolerations:
   - effect: NoSchedule
     key: nvidia.com/gpu
     operator: Exists
   ...
   containers:
   - name: ray-node
     image: rayproject/ray:nightly-gpu
     ...

Further reference and discussion
--------------------------------
Read about Kubernetes device plugins `here <https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/>`__,
about Kubernetes GPU plugins `here <https://kubernetes.io/docs/tasks/manage-gpus/scheduling-gpus>`__,
and about Nvidia's GPU plugin for Kubernetes `here <https://github.com/NVIDIA/k8s-device-plugin>`__.

If you run into problems setting up GPUs for your Ray cluster on Kubernetes, please reach out to us at `<https://discuss.ray.io>`_.

Questions or Issues?
--------------------

.. include:: /_includes/_help.rst

.. _`GKE`: https://cloud.google.com/kubernetes-engine/docs/how-to/gpus
.. _`EKS`: https://docs.aws.amazon.com/eks/latest/userguide/eks-optimized-ami.html
.. _`AKS`: https://docs.microsoft.com/en-us/azure/aks/gpu-cluster

.. _`tolerations`: https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/
.. _`taints`: https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/
.. _`Nvidia gpu plugin`: https://github.com/NVIDIA/k8s-device-plugin
.. _`admission controller`: https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/
.. _`ExtendedResourceToleration`: https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/#extendedresourcetoleration
