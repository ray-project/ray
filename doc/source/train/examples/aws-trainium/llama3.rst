Fine tune Llama3.1 with AWS Trainium
====================================

This example demonstrates how to fine-tune Llama 3.1 on `AWS
Trainium <https://aws.amazon.com/ai/machine-learning/trainium/>`__ using
Ray Train, PyTorch Lightning, and Neuron.

AWS Trainium is the machine learning (ML) chip that AWS built for deep
learning (DL) training of 100B+ parameter models. `AWS Neuron
SDK <https://aws.amazon.com/machine-learning/neuron/>`__ helps
developers train models on Trainium accelerators.

Prepare the environment
-----------------------

See the AWS guide for `setting up an EKS cluster with
KubeRay <https://awslabs.github.io/data-on-eks/docs/gen-ai/training/Neuron/RayTrain-Llama2#1-deploying-the-solution>`__,
and the guide to `configure
Neuron <https://awslabs.github.io/data-on-eks/docs/gen-ai/training/Neuron/RayTrain-Llama2#1-deploying-the-solution>`__
within the cluster.

Create docker image
-------------------

Use a pre-configured job to run in the cluster.

1. Clone the repo.

::

   git clone https://github.com/aws-neuron/aws-neuron-samples.git

2. Go to the XXX directory.

::

   cd XXX

3. Trigger the script.

::

   ./kuberay-trn1-llama3-pretrain-build-image.sh

4. Enter the zone your cluster is running in.
5. Verify in the AWS console that the Amazon ECR service has the newly
   created ``kuberay_trn1_llama3.1_pytorch2``
6. Update the image in the following manifest files:

   1. ``1-llama3-pretrain-trn1-rayjob-create-test-data.yaml``
   2. ``3-llama3-pretrain-trn1-rayjob.yaml``

Configuring Ray
---------------

The XXX directory in the AWS Neuron samples repository simplifies the
Ray configuration. KubeRay provides a manifest that you can easily apply
to the cluster to set up the head and worker pods. Run the following
command:

::

   kubectl apply -f llama3-pretrain-trn1-raycluster.yaml

Port forward from the cluster to see the state of the Ray dashboard and
then view it on ```http://localhost:8265`` <http://localhost:8265>`__.
Run it in the background with the following command:

::

   kubectl port-forward service/kuberay-trn1-head-svc 8265:8265 &

Running the model
-----------------

Now that Ray is running with a Docker image to run the workload, you can
prepare to run it in a cluster. Before beginning, make sure that you’re
using the correct image URIs to run the Ray jobs.

1. Download the dataset and generated pre-train data:

::

   kubectl apply -f 1-llama3-pretrain-trn1-rayjob-create-test-data.yaml

When the job is done, run the following training job:

::

   kubectl apply -f 3-llama3-pretrain-trn1-rayjob.yaml
