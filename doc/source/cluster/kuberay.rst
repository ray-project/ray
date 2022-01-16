Using Kuberay
=============

Here we describe how you can deploy a Ray cluster on Kuberay. The following instructions are for
Minikube but the deployment works the same way on a real Kubernetes cluster. First we make sure
Minikube is initialized with

.. code-block:: shell

minikube start


Now you can deploy the Kuberay operator using

.. code-blocks:: shell

./ray/python/ray/autoscaler/kuberay/init-config.sh


.. code-blocks:: shell

kubectl apply -k "ray/python/ray/autoscaler/kuberay/config/default"


You can verify that the operator has been deployed using

.. code-blocks:: shell

kubectl -n ray-system get pods


Now let's deploy a new Ray cluster:

.. code-blocks:: shell

kubectl create -f ray/python/ray/autoscaler/kuberay/ray-cluster.complete.yaml