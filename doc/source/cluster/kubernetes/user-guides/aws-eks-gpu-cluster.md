(kuberay-eks-gpu-cluster-setup)=

# Start Amazon EKS Cluster with GPUs for KubeRay

This guide walks you through the steps to create an Amazon EKS cluster with GPU nodes specifically for KubeRay.
The configuration outlined here can be applied to most KubeRay examples found in the documentation.

## Step 1: Create a Kubernetes cluster on Amazon EKS

Follow the first two steps in [this AWS documentation](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-console.html#) to: 
(1) create your Amazon EKS cluster and (2) configure your computer to communicate with your cluster.

## Step 2: Create node groups for the Amazon EKS cluster

Follow "Step 3: Create nodes" in [this AWS documentation](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-console.html#) to create node groups.
The following section provides more detailed information.

### Create a CPU node group

Typically, avoid running GPU workloads on the Ray head. Create a CPU node group for all Pods except Ray GPU 
workers, such as the KubeRay operator, Ray head, and CoreDNS Pods.

Here's a common configuration that works for most KubeRay examples in the docs:
  * Instance type: [**m5.xlarge**](https://aws.amazon.com/ec2/instance-types/m5/) (4 vCPU; 16 GB RAM)
  * Disk size: 256 GB
  * Desired size: 1, Min size: 0, Max size: 1

### Create a GPU node group

Create a GPU node group for Ray GPU workers.

1. Here's a common configuration that works for most KubeRay examples in the docs:
   * AMI type: Bottlerocket NVIDIA (BOTTLEROCKET_x86_64_NVIDIA)
   * Instance type: [**g5.xlarge**](https://aws.amazon.com/ec2/instance-types/g5/) (1 GPU; 24 GB GPU Memory; 4 vCPUs; 16 GB RAM)
   * Disk size: 1024 GB
   * Desired size: 1, Min size: 0, Max size: 1

2. Please install the NVIDIA device plugin. (Note: You can skip this step if you used the `BOTTLEROCKET_x86_64_NVIDIA` AMI in the step above.)
   * Install the DaemonSet for NVIDIA device plugin to run GPU enabled containers in your Amazon EKS cluster. You can refer to the [Amazon EKS optimized accelerated Amazon Linux AMIs](https://docs.aws.amazon.com/eks/latest/userguide/eks-optimized-ami.html#gpu-ami)
   or [NVIDIA/k8s-device-plugin](https://github.com/NVIDIA/k8s-device-plugin) repository for more details.
   * If the GPU nodes have taints, add `tolerations` to `nvidia-device-plugin.yml` to enable the DaemonSet to schedule Pods on the GPU nodes.

   > **Note:** If you encounter permission issues with `kubectl`, follow "Step 2: Configure your computer to communicate with your cluster"
   in the [AWS documentation](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-console.html#).

   ```sh
   # Install the DaemonSet
   kubectl apply -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.9.0/nvidia-device-plugin.yml
   
   # Verify that your nodes have allocatable GPUs. If the GPU node fails to detect GPUs,
   # please verify whether the DaemonSet schedules the Pod on the GPU node.
   kubectl get nodes "-o=custom-columns=NAME:.metadata.name,GPU:.status.allocatable.nvidia\.com/gpu"
   
   # Example output:
   # NAME                                GPU
   # ip-....us-west-2.compute.internal   4
   # ip-....us-west-2.compute.internal   <none>
   ```

3. Add a Kubernetes taint to prevent scheduling CPU Pods on this GPU node group. For KubeRay examples, add the following taint to the GPU nodes: `Key: ray.io/node-type, Value: worker, Effect: NoSchedule`, and include the corresponding `tolerations` for GPU Ray worker Pods.

   > Warning: GPU nodes are extremely expensive. Please remember to delete the cluster if you no longer need it.

## Step 3: Verify the node groups

> **Note:** If you encounter permission issues with `eksctl`, navigate to your AWS account's webpage and copy the
credential environment variables, including `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN`,
from the "Command line or programmatic access" page.

```sh
eksctl get nodegroup --cluster ${YOUR_EKS_NAME}

# CLUSTER         NODEGROUP       STATUS  CREATED                 MIN SIZE        MAX SIZE        DESIRED CAPACITY        INSTANCE TYPE   IMAGE ID                        ASG NAME                           TYPE
# ${YOUR_EKS_NAME}     cpu-node-group  ACTIVE  2023-06-05T21:31:49Z    0               1               1                       m5.xlarge       AL2_x86_64                      eks-cpu-node-group-...     managed
# ${YOUR_EKS_NAME}     gpu-node-group  ACTIVE  2023-06-05T22:01:44Z    0               1               1                       g5.12xlarge     BOTTLEROCKET_x86_64_NVIDIA      eks-gpu-node-group-...     managed
```
