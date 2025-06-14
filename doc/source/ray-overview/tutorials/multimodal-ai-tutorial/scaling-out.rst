Scaling Out with Ray
====================

Ray makes it easy to scale your applications from a single machine to a distributed cluster. To do this, you either need to have a distributed cluster that has been previously set up to connect to, or you need to integrate with a cloud provider. This section explains how to create a distributed Ray cluster that you can use to scale your applications and workloads.

Local Development
-----------------

When you start developing with Ray, the first call to Ray automatically creates a local cluster on your machine. This is equivalent to running:

.. code-block:: python

    import ray
    ray.init()

When the Ray cluster is created locally, it is ephemeral and is destroyed when the Python process exits. To run your application across multiple machines, you need to create a persistent Ray cluster and connect to the head node to run your application.

Multi-node Deployment Options
-----------------------------

Multi-node Ray clusters can be created in a few different ways, including:

- **Anyscale**: a managed service for building, running, and scaling Ray applications
- **Ray Cluster Launcher**: a tool for creating and managing Ray clusters with configuration files
- **KubeRay**: a Kubernetes operator for creating and managing Ray clusters in Kubernetes

Anyscale offers the fastest way to get started with multi-node Ray clusters. Ansycale supports both VM-based and Kubernetes-based deployments, and also offers additional features to help you manage your Ray applications and compue resources. Check out the `Anyscale console <https://console.anyscale.com/register/ha?render_flow=ray&utm_source=ray_docs&utm_medium=docs&utm_campaign=tutorial>`_ to get started.

If you want to create a remote development environment without using Anyscale, the Ray Cluster Launcher also offers a straightforward alternative, though lacking critical features around production-readiness, observability, node start performance, etc.

.. warning:: The Ray Cluster Launcher is a development tool and is not recommended for production workloads.

Deploying Ray Clusters with KubeRay requires more setup and configuration, and will not be covered in this tutorial. See :doc:`../../../cluster/kubernetes/getting-started` for more information.

Deploying Multi-node Ray Clusters
---------------------------------

.. tab-set::

   .. tab-item:: Using Anyscale

      Anyscale Workspaces make it easy to create and manage multi-node Ray clusters. 
      
      .. note:: This section walks you through the process of creating a custom workspace for the application. Alternatively, you can use the `Anyscale Workspace template for this example application <https://console.anyscale.com/template-preview/image-search-and-classification>`_ to skip this step and quickly get started.
      
      After signing up and creating an account, create a new Anyscale Workspace:

      .. image:: ./images/create-workspace.png
         :alt: Create an Anyscale Workspace
         :align: center
         :width: 75%

      Specify your Ray worker configuration in the Anyscale compute config. For this tutorial, we will use 1 `4xL4` worker node:

      .. image:: ./images/l4-compute-config.png
         :alt: Create an Anyscale Compute Configuration
         :align: center
         :width: 50%

      Once you have set the compute config, start the workspace. This will start a 2-node Ray cluster with 1 head node and 1 worker node. The worker node will have 4 L4 GPUs available to run your Ray application. 

      Once the Ray cluster is running, you are able to develop your Ray application in the Anyscale Workspace. For more information on Anyscale workspaces, see the `Anyscale documentation <https://docs.anyscale.com/platform/workspaces/>`_.

      Inside of the Anyscale Workspace, first pull the example code from the repository using the integrated terminal in the Anyscale Workspace IDE:

      .. code-block:: bash

         git clone https://github.com/anyscale/foundational-ray-app.git
         cd foundational-ray-app

      Next, install the dependencies from the integrated terminal in the Anyscale Workspace development environment:

      .. literalinclude:: ../../examples/e2e-multimodal-ai-workloads/ci/build.sh
         :language: bash
         :start-after: # Install Python dependencies

      This registers the dependencies across the Ray cluster, and ensure that dependencies are available to all of the Ray workers, which you can see from the Dependencies tab of the Anyscale Workspace:
      
      .. image:: ./images/anyscale-dependencies.png
         :alt: Dependencies tab
         :align: center
         :width: 30%      

   .. tab-item:: Using the Cluster Launcher

      The Ray Cluster Launcher is a development tool for quickly creating persistent Ray clusters through configuration files. It simplifies the process of launching a remote Ray cluster on common cloud providers. In this example, we will use AWS.

      .. note:: Make sure that you have quota and permissions to launch `g6.12xlarge` instances in the AWS account that you are using. Reach 

      To create a Ray cluster with the Cluster Launcher, you need to first configure credentials to authenticate to a cloud provider. For example, to create a Ray cluster on AWS, you need to install the AWS Python SDK (boto3) and `set up AWS credentials <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html>`_ using environment variables:

      .. code-block:: bash

         # install AWS Python SDK (boto3)
         pip install -U boto3

         # setup AWS credentials using environment variables
         export AWS_ACCESS_KEY_ID=foo
         export AWS_SECRET_ACCESS_KEY=bar
         export AWS_SESSION_TOKEN=baz

      Next, create a YAML configuration file `doggos_cluster.yaml` to specify the Ray cluster configuration. Here is an example for AWS to create a 2-node Ray cluster with 1 head node and 1 worker node with 4 L4 GPUs with an EFS volume accessible from all of the nodes:

      .. code-block:: yaml
         
         cluster_name: multimodal_ai_cluster
         docker:
            image: "rayproject/ray:2.44.1-py312-cu123"
            container_name: "ray_container"
            pull_before_run: True
            run_options:
               - --ulimit nofile=65536:65536
         idle_timeout_minutes: 30
         provider:
            type: aws
            region: us-west-2
            availability_zone: us-west-2a,us-west-2b
            cache_stopped_nodes: True
         available_node_types:
            ray.head.default:
               node_config:
                     InstanceType: m5.large
            ray.worker.default:
               min_workers: 1
               max_workers: 1
               node_config:
                     InstanceType: g6.12xlarge
         head_node_type: ray.head.default
         setup_commands:
            - sudo kill -9 `sudo lsof /var/lib/dpkg/lock-frontend | awk '{print $2}' | tail -n 1`;
               sudo pkill -9 apt-get;
               sudo pkill -9 dpkg;
               sudo dpkg --configure -a;
               sudo apt-get -y install binutils;
               cd $HOME;
               git clone https://github.com/aws/efs-utils;
               cd $HOME/efs-utils;
               ./build-deb.sh;
               sudo apt-get -y install ./build/amazon-efs-utils*deb;
               cd $HOME;
               sudo mkdir -p mnt/user_storage;
               sudo mount -t efs {{FileSystemId}}:/ mnt/user_storage;
               sudo chmod 777 mnt/user_storage;
            - pip3 install --no-cache-dir \
               "matplotlib==3.10.0" \
               "torch==2.7.0" \
               "transformers==4.52.3" \
               "scikit-learn==1.6.0" \
               "mlflow==2.19.0" \
               "ipywidgets==8.1.3"
         head_start_ray_commands:
            - ray stop
            - ray start --head --port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml --dashboard-host=0.0.0.0
         worker_start_ray_commands:
            - ray stop
            - ray start --address=$RAY_HEAD_IP:6379 --object-manager-port=8076

      Deploy the Ray cluster with the cluster launcher CLI:

      .. code-block:: bash

         ray up doggos_cluster.yaml

      This starts the deployment of a cluster in your AWS account, assuming that the permissions have been properly configured. Follow the interactive prompts from the cluster launcher CLI. It takes a while to pull the container images and properly set everything.

      .. note:: The first time you launch a Ray cluster with the Cluster Launcher, it takes a while to pull the container images and properly set everything. Subsequent launches are much faster
      
      Once the cluster has been launched, enable port-forwarding to the head node so that the Ray dashboard is accessible from your local machine:
      
      .. code-block:: bash

         ray dashboard doggos_cluster.yaml
        
      Now, you should be able to access the Ray dashboard at `http://localhost:8265` and see that the Ray cluster is running.

Scaling Out the Embedding Pipeline
----------------------------------

Now that we have a distributed Ray cluster with L4 GPUs, we can scale out the embedding pipeline to run much more efficiently.

.. tab-set::

   .. tab-item:: Using Anyscale

      First, let's open the `doggos/embed.py` file and inspect the code:

      .. literalinclude:: ../../examples/e2e-multimodal-ai-workloads/doggos/embed.py
         :language: python

      This code runs the same embedding pipeline as the local run, but with a few changes to scale out the embeddings pipeline across the 4 L4 GPUs.

      Run the embedding pipeline by running the following command in the integrated terminal:

      .. code-block:: bash
         
         python -m doggos.embed

      You should see that the embedding pipeline is running and the embeddings are being generated per the logs:

      .. image:: ./images/doggos-embedder-dataset-logs.png
         :alt: Embedding dataset execution logs
         :align: center
         :width: 75%

      Compared to the local run, the embeddings are generated much faster because the pipeline is running in parallel across the Ray workers, using 4 L4 GPUs:

      .. image:: ./images/doggos-gpu-usage.png
         :alt: GPU usage
         :align: center
         :width: 25%

      After the embeddings are generated, you can see the browse the embeddings from the file viewer provided in the Anyscale Workspace, connecting to the `User storage` volume wher ethe embeddings were written:

      .. image:: ./images/doggos-embeddings-files.png
         :alt: Embeddings file viewer
         :align: center

      With Anyscale, the Ray dashboard is avaiale right from the Anyscale Workspace, and you can also monitor the progress of the data processing workload from the Ray Workloads dashboard:

      .. image:: ./images/doggos-data-dashboard.png
         :alt: Anyscale data workload dashboard
         :align: center

   .. tab-item:: Using the Cluster Launcher
      
      TODO: add cluster launcher instructions

Scaling Out the Training Pipeline
---------------------------------

Next, we will scale out the training pipeline to run on the remote Ray cluster.

.. tab-set::

   .. tab-item:: Using Anyscale

      Assuming that you have already cloned the repository and installed the dependencies, let's look at the `doggos/train.py` file:

      .. literalinclude:: ../../examples/e2e-multimodal-ai-workloads/doggos/train.py
         :language: python

      This code runs the same training pipeline as the local run, but with a few changes to scale out the training pipeline across the Ray workers.

      Run the training pipeline by running the following command in the integrated terminal in the Anyscale Workspace:

      .. code-block:: bash
         
         python -m doggos.train

      You should see that the training pipeline is running and the model is being trained per the logs:

      .. image:: ./images/doggos-train-logs.png
         :alt: Training logs
         :align: center
         :width: 75%

      Compared to the local run, the training happens much more efficiently because the pipeline is running in parallel across the Ray workers, using the 4 L4 GPUs:

      .. image:: ./images/doggos-gpu-usage.png
         :alt: GPU usage
         :align: center
         :width: 25%

      After the training is complete, you can see the model checkpoints from the file viewer provided in the Anyscale Workspace, connecting to the `User storage` volume where the checkpoints were written:

      .. image:: ./images/doggos-model-checkpoints.png
         :alt: Model checkpoints
         :align: center

      You can also monitor the progress of the training workload from the Ray Workloads dashboard:

      .. image:: ./images/doggos-train-dashboard.png
         :alt: Anyscale data workload dashboard
         :align: center

      After the training is complete, let's do a quick evaluation of the model on the test set:

      .. code-block:: bash

         python -m doggos.eval

      The evaluation also runs in parallel across the Ray workers, using the 4 L4 GPUs, and completes much faster than the local run. From the output, we can see that the model achieves good precision, recall, and F1 score, and also has high accuracy.

   .. tab-item:: Using the Cluster Launcher

      TODO: add instructions

Scaling out the Search Application
----------------------------------

Finally, we will deploy the trained dog breed classifier as a production-ready API using Ray Serve.

.. tab-set::

   .. tab-item:: Using Anyscale

      Assuming that you have already cloned the repository and installed the dependencies, let's look at the `doggos/serve.py` file:

      .. literalinclude:: ../../examples/e2e-multimodal-ai-workloads/doggos/serve.py
         :language: python

      This code runs the same serve pipeline as the local run, but with a few changes to scale out the service across the 4 L4 GPUs.

      Run the serve pipeline by running the following command in the integrated terminal in the Anyscale Workspace:

      .. code-block:: bash
         
         python -m doggos.serve

      You can see from the logs that the service is being deployed in the Anyscale workspace:

      .. image:: ./images/doggos-serve-logs.png
         :alt: Serve logs
         :align: center
         :width: 75%

      You can also check the status of the service by looking at the Ray dashboard:

      .. image:: ./images/doggos-serve-dashboard.png
         :alt: Ray dashboard
         :align: center
         :width: 75%

      Once the service has been deployed and is running,you can test it by running the following command in a new terminal:

      .. code-block:: bash
         
         curl -X POST http://127.0.0.1:8000/predict/ -H "Content-Type: application/json" -d '{"url": "https://doggos-dataset.s3.us-west-2.amazonaws.com/samara.png", "k": 4}'

      You should see the response from the service, and the service is running in parallel across the 4 L4 GPUs.

      We can also run a script with a higher concurrency to see the service scale out to use all of the GPUs:
      
      .. code-block:: bash

         python -m doggos.sim_concurrency

      You can see from the metrics that the service is scaling out to use 2 of the GPUs, as defined in the Ray Serve deployment configuration:
      
      .. image:: ./images/doggos-serve-metrics.png
         :alt: Serve metrics
         :align: center
         :width: 75%

   .. tab-item:: Using the Cluster Launcher

      TODO: add instructions

In this section, we scaled out the multimodal AI application to run on a remote Ray cluster, and deployed the trained dog breed classifier as a production-ready API using Ray Serve. In the next section, we will discuss some key considerations for scaling out Ray applications to production. 