:orphan:

Distributed fine-tuning of Llama 3.1 8B on AWS Trainium with Ray and PyTorch Lightning
======================================================================================

.. raw:: html

    <a id="try-anyscale-quickstart-aws-trainium-llama3" target="_blank" href="https://www.anyscale.com/ray-on-anyscale?utm_source=ray_docs&utm_medium=docs&utm_campaign=aws-trainium-llama3">
      <img src="../../../_static/img/run-on-anyscale.svg" alt="Run on Anyscale" />
      <br/><br/>
    </a>

This example demonstrates how to fine-tune the `Llama 3.1 8B <https://huggingface.co/NousResearch/Meta-Llama-3.1-8B/>`__ model on `AWS
Trainium <https://aws.amazon.com/ai/machine-learning/trainium/>`__ instances using Ray Train, PyTorch Lightning, and AWS Neuron SDK.

AWS Trainium is the machine learning (ML) chip that AWS built for deep
learning (DL) training of 100B+ parameter models. `AWS Neuron
SDK <https://aws.amazon.com/machine-learning/neuron/>`__ helps
developers train models on Trainium accelerators.

Prepare the environment
-----------------------

See `Setup EKS cluster and tools <https://github.com/aws-neuron/aws-neuron-eks-samples/tree/master/llama3.1_8B_finetune_ray_ptl_neuron#setupeksclusterandtools>`__ for setting up an Amazon EKS cluster leveraging AWS Trainium instances.

Create a Docker image
---------------------
When the EKS cluster is ready, create an Amazon ECR repository for building and uploading the Docker image containing artifacts for fine-tuning a Llama3.1 8B model:

1. Clone the repo.

::

   git clone https://github.com/aws-neuron/aws-neuron-eks-samples.git

2. Go to the ``llama3.1_8B_finetune_ray_ptl_neuron`` directory.

::

   cd aws-neuron-eks-samples/llama3.1_8B_finetune_ray_ptl_neuron

3. Trigger the script.

::

   chmod +x 0-kuberay-trn1-llama3-finetune-build-image.sh
   ./0-kuberay-trn1-llama3-finetune-build-image.sh

4. Enter the zone your cluster is running in, for example: us-east-2.

5. Verify in the AWS console that the Amazon ECR service has the newly
   created ``kuberay_trn1_llama3.1_pytorch2`` repository.

6. Update the ECR image ARN in the manifest file used for creating the Ray cluster.

Replace the <AWS_ACCOUNT_ID> and <REGION> placeholders with actual values in the ``1-llama3-finetune-trn1-create-raycluster.yaml`` file using commands below to reflect the ECR image ARN created above:

::

   export AWS_ACCOUNT_ID=<enter_your_aws_account_id> # for ex: 111222333444
   export REGION=<enter_your_aws_region> # for ex: us-east-2
   sed -i "s/<AWS_ACCOUNT_ID>/$AWS_ACCOUNT_ID/g" 1-llama3-finetune-trn1-create-raycluster.yaml
   sed -i "s/<REGION>/$REGION/g" 1-llama3-finetune-trn1-create-raycluster.yaml

Configuring Ray Cluster
-----------------------

The ``llama3.1_8B_finetune_ray_ptl_neuron`` directory in the AWS Neuron samples repository simplifies the
Ray configuration. KubeRay provides a manifest that you can apply
to the cluster to set up the head and worker pods.

Run the following command to set up the Ray cluster:

::

   kubectl apply -f 1-llama3-finetune-trn1-create-raycluster.yaml


Accessing Ray Dashboard
-----------------------
Port forward from the cluster to see the state of the Ray dashboard and
then view it on `http://localhost:8265 <http://localhost:8265/>`__.
Run it in the background with the following command:

::

   kubectl port-forward service/kuberay-trn1-head-svc 8265:8265 &

Launching Ray Jobs
------------------

The Ray cluster now ready to handle workloads. Initiate the data preparation and fine-tuning Ray jobs:

1. Launch the Ray job for downloading the dolly-15k dataset and the Llama3.1 8B model artifacts:

::

   kubectl apply -f 2-llama3-finetune-trn1-rayjob-create-data.yaml

2. When the job has executed successfully, run the following fine-tuning job:

::

   kubectl apply -f 3-llama3-finetune-trn1-rayjob-submit-finetuning-job.yaml

3. Monitor the jobs via the Ray Dashboard


For detailed information on each of the steps above, see the `AWS documentation link <https://github.com/aws-neuron/aws-neuron-eks-samples/blob/master/llama3.1_8B_finetune_ray_ptl_neuron/README.md/>`__.
