{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Distributing your PyTorch Training Code with Ray Train and Ray Data\n",
    "\n",
    "**Time to complete**: 10 min\n",
    "\n",
    "This template shows you how to distribute your PyTorch training code with Ray Train and Ray Data, getting performance and usability improvements along the way.\n",
    "\n",
    "In this tutorial, you:\n",
    "1. Start with a basic single machine PyTorch example.\n",
    "2. Distribute it to multiple GPUs on multiple machines with [Ray Train](https://docs.ray.io/en/latest/train/train.html) and, if you are using an Anyscale Workspace, inspect results with the Ray Train dashboard.\n",
    "3. Scale data ingest separately from training with [Ray Data](https://docs.ray.io/en/latest/data/data.html) and, if you are using an Anyscale Workspace, inspect results with the Ray Data dashboard. "
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Step 1: Start with a basic single machine PyTorch example\n",
    "\n",
    "In this step you train a PyTorch VisionTransformer model to recognize objects using the open CIFAR-10 dataset. It's a minimal example that trains on a single machine. Note that the code has multiple functions to highlight the changes needed to run things with Ray.\n",
    "\n",
    "First, install and import the required Python modules."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "vscode": {
     "languageId": "shellscript"
    }
   },
   "outputs": [],
   "source": [
    "%%bash\n",
    "pip install torch torchvision"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "import os\n",
    "from typing import Dict\n",
    "\n",
    "import torch\n",
    "from filelock import FileLock\n",
    "from torch import nn\n",
    "from torch.utils.data import DataLoader\n",
    "from torchvision import datasets, transforms\n",
    "from torchvision.transforms import Normalize, ToTensor\n",
    "from torchvision.models import VisionTransformer\n",
    "from tqdm import tqdm"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Next, define a function that returns PyTorch DataLoaders for train and test data. Note how the code also preprocesses the data. "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "def get_dataloaders(batch_size):\n",
    "    # Transform to normalize the input images.\n",
    "    transform = transforms.Compose([ToTensor(), Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))])\n",
    "\n",
    "    with FileLock(os.path.expanduser(\"~/data.lock\")):\n",
    "        # Download training data from open datasets.\n",
    "        training_data = datasets.CIFAR10(\n",
    "            root=\"~/data\",\n",
    "            train=True,\n",
    "            download=True,\n",
    "            transform=transform,\n",
    "        )\n",
    "\n",
    "        # Download test data from open datasets.\n",
    "        testing_data = datasets.CIFAR10(\n",
    "            root=\"~/data\",\n",
    "            train=False,\n",
    "            download=True,\n",
    "            transform=transform,\n",
    "        )\n",
    "\n",
    "    # Create data loaders.\n",
    "    train_dataloader = DataLoader(training_data, batch_size=batch_size, shuffle=True)\n",
    "    test_dataloader = DataLoader(testing_data, batch_size=batch_size)\n",
    "\n",
    "    return train_dataloader, test_dataloader"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Now, define a function that runs the core training loop. Note how the code synchronously alternates between training the model for one epoch and then evaluating its performance."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "def train_func():\n",
    "    lr = 1e-3\n",
    "    epochs = 1\n",
    "    batch_size = 512\n",
    "\n",
    "    # Get data loaders inside the worker training function.\n",
    "    train_dataloader, valid_dataloader = get_dataloaders(batch_size=batch_size)\n",
    "\n",
    "    model = VisionTransformer(\n",
    "        image_size=32,   # CIFAR-10 image size is 32x32\n",
    "        patch_size=4,    # Patch size is 4x4\n",
    "        num_layers=12,   # Number of transformer layers\n",
    "        num_heads=8,     # Number of attention heads\n",
    "        hidden_dim=384,  # Hidden size (can be adjusted)\n",
    "        mlp_dim=768,     # MLP dimension (can be adjusted)\n",
    "        num_classes=10   # CIFAR-10 has 10 classes\n",
    "    )\n",
    "    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')\n",
    "    model.to(device)\n",
    "\n",
    "    loss_fn = nn.CrossEntropyLoss()\n",
    "    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-2)\n",
    "\n",
    "    # Model training loop.\n",
    "    for epoch in range(epochs):\n",
    "        model.train()\n",
    "        for X, y in tqdm(train_dataloader, desc=f\"Train Epoch {epoch}\"):\n",
    "            X, y = X.to(device), y.to(device)\n",
    "            pred = model(X)\n",
    "            loss = loss_fn(pred, y)\n",
    "\n",
    "            optimizer.zero_grad()\n",
    "            loss.backward()\n",
    "            optimizer.step()\n",
    "\n",
    "        model.eval()\n",
    "        valid_loss, num_correct, num_total = 0, 0, 0\n",
    "        with torch.no_grad():\n",
    "            for X, y in tqdm(valid_dataloader, desc=f\"Valid Epoch {epoch}\"):\n",
    "                X, y = X.to(device), y.to(device)\n",
    "                pred = model(X)\n",
    "                loss = loss_fn(pred, y)\n",
    "\n",
    "                valid_loss += loss.item()\n",
    "                num_total += y.shape[0]\n",
    "                num_correct += (pred.argmax(1) == y).sum().item()\n",
    "\n",
    "        valid_loss /= len(train_dataloader)\n",
    "        accuracy = num_correct / num_total\n",
    "\n",
    "        print({\"epoch_num\": epoch, \"loss\": valid_loss, \"accuracy\": accuracy})"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Finally, run training."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "train_func()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "The training should take about 2 minutes and 10 seconds with an accuracy of about 0.35.  "
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Step 2: Distribute training to multiple machines with Ray Train\n",
    "\n",
    "Next, modify this example to run with Ray Train on multiple machines with distributed data parallel (DDP) training. In DDP training, each process trains a copy of the model on a subset of the data and synchronizes gradients across all processes after each backward pass to keep models consistent. Essentially, Ray Train allows you to wrap PyTorch training code in a function and run the function on each worker in your Ray Cluster. With a few modifications, you get the fault tolerance and auto-scaling of a [Ray Cluster](https://docs.ray.io/en/latest/cluster/getting-started.html), as well as the observability and ease-of-use of [Ray Train](https://docs.ray.io/en/latest/train/train.html).\n",
    "\n",
    "First, set some environment variables and import some modules."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "vscode": {
     "languageId": "shellscript"
    }
   },
   "outputs": [],
   "source": [
    "%%bash\n",
    "# Remove when Ray Train v2 is the default in an upcoming release.\n",
    "echo \"RAY_TRAIN_V2_ENABLED=1\" > /home/ray/default/.env"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "vscode": {
     "languageId": "shellscript"
    }
   },
   "outputs": [],
   "source": [
    "# Load env vars in notebooks.\n",
    "from dotenv import load_dotenv\n",
    "load_dotenv()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "import ray.train\n",
    "from ray.train import RunConfig, ScalingConfig\n",
    "from ray.train.torch import TorchTrainer\n",
    "\n",
    "import tempfile\n",
    "import uuid"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Next, modify the training function you wrote earlier. Every difference from the previous script is highlighted and explained with a numbered comment; for example, “[1].”"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "def train_func_per_worker(config: Dict):\n",
    "    lr = config[\"lr\"]\n",
    "    epochs = config[\"epochs\"]\n",
    "    batch_size = config[\"batch_size_per_worker\"]\n",
    "\n",
    "    # Get data loaders inside the worker training function.\n",
    "    train_dataloader, valid_dataloader = get_dataloaders(batch_size=batch_size)\n",
    "\n",
    "    # [1] Prepare data loader for distributed training.\n",
    "    # The prepare_data_loader method assigns unique rows of data to each worker so that\n",
    "    # the model sees each row once per epoch.\n",
    "    # NOTE: This approach only works for map-style datasets. For a general distributed\n",
    "    # preprocessing and sharding solution, see the next part using Ray Data for data \n",
    "    # ingestion.\n",
    "    # =================================================================================\n",
    "    train_dataloader = ray.train.torch.prepare_data_loader(train_dataloader)\n",
    "    valid_dataloader = ray.train.torch.prepare_data_loader(valid_dataloader)\n",
    "\n",
    "    model = VisionTransformer(\n",
    "        image_size=32,   # CIFAR-10 image size is 32x32\n",
    "        patch_size=4,    # Patch size is 4x4\n",
    "        num_layers=12,   # Number of transformer layers\n",
    "        num_heads=8,     # Number of attention heads\n",
    "        hidden_dim=384,  # Hidden size (can be adjusted)\n",
    "        mlp_dim=768,     # MLP dimension (can be adjusted)\n",
    "        num_classes=10   # CIFAR-10 has 10 classes\n",
    "    )\n",
    "\n",
    "    # [2] Prepare and wrap your model with DistributedDataParallel.\n",
    "    # The prepare_model method moves the model to the correct GPU/CPU device.\n",
    "    # =======================================================================\n",
    "    model = ray.train.torch.prepare_model(model)\n",
    "\n",
    "    loss_fn = nn.CrossEntropyLoss()\n",
    "    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-2)\n",
    "\n",
    "    # Model training loop.\n",
    "    for epoch in range(epochs):\n",
    "        if ray.train.get_context().get_world_size() > 1:\n",
    "            # Required for the distributed sampler to shuffle properly across epochs.\n",
    "            train_dataloader.sampler.set_epoch(epoch)\n",
    "\n",
    "        model.train()\n",
    "        for X, y in tqdm(train_dataloader, desc=f\"Train Epoch {epoch}\"):\n",
    "            pred = model(X)\n",
    "            loss = loss_fn(pred, y)\n",
    "\n",
    "            optimizer.zero_grad()\n",
    "            loss.backward()\n",
    "            optimizer.step()\n",
    "\n",
    "        model.eval()\n",
    "        valid_loss, num_correct, num_total = 0, 0, 0\n",
    "        with torch.no_grad():\n",
    "            for X, y in tqdm(valid_dataloader, desc=f\"Valid Epoch {epoch}\"):\n",
    "                pred = model(X)\n",
    "                loss = loss_fn(pred, y)\n",
    "\n",
    "                valid_loss += loss.item()\n",
    "                num_total += y.shape[0]\n",
    "                num_correct += (pred.argmax(1) == y).sum().item()\n",
    "\n",
    "        valid_loss /= len(train_dataloader)\n",
    "        accuracy = num_correct / num_total\n",
    "\n",
    "        # [3] (Optional) Report checkpoints and attached metrics to Ray Train.\n",
    "        # ====================================================================\n",
    "        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:\n",
    "            torch.save(\n",
    "                model.module.state_dict(),\n",
    "                os.path.join(temp_checkpoint_dir, \"model.pt\")\n",
    "            )\n",
    "            ray.train.report(\n",
    "                metrics={\"loss\": valid_loss, \"accuracy\": accuracy},\n",
    "                checkpoint=ray.train.Checkpoint.from_directory(temp_checkpoint_dir),\n",
    "            )\n",
    "            if ray.train.get_context().get_world_rank() == 0:\n",
    "                print({\"epoch_num\": epoch, \"loss\": valid_loss, \"accuracy\": accuracy})"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Finally, run the training function on the Ray Cluster with a TorchTrainer using GPU workers.\n",
    "\n",
    "The `TorchTrainer` takes the following arguments:\n",
    "* `train_loop_per_worker`: the training function you defined earlier. Each Ray Train worker runs this function. Note that you can call special Ray Train functions from within this function.\n",
    "* `train_loop_config`: a hyperparameter dictionary. Each Ray Train worker calls its `train_loop_per_worker` function with this dictionary.\n",
    "* `scaling_config`: a configuration object that specifies the number of workers and compute resources, for example, CPUs or GPUs, that your training run needs.\n",
    "* `run_config`: a configuration object that specifies various fields used at runtime, including the path to save checkpoints.\n",
    "\n",
    "`trainer.fit` spawns a controller process to orchestrate the training run and worker processes to actually execute the PyTorch training code."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "def train_cifar_10(num_workers, use_gpu):\n",
    "    global_batch_size = 512\n",
    "\n",
    "    train_config = {\n",
    "        \"lr\": 1e-3,\n",
    "        \"epochs\": 1,\n",
    "        \"batch_size_per_worker\": global_batch_size // num_workers,\n",
    "    }\n",
    "\n",
    "    # [1] Start distributed training.\n",
    "    # Define computation resources for workers.\n",
    "    # Run `train_func_per_worker` on those workers.\n",
    "    # =============================================\n",
    "    scaling_config = ScalingConfig(num_workers=num_workers, use_gpu=use_gpu)\n",
    "    run_config = RunConfig(\n",
    "        # /mnt/cluster_storage is an Anyscale-specific storage path.\n",
    "        # OSS users should set up this path themselves.\n",
    "        storage_path=\"/mnt/cluster_storage\", \n",
    "        name=f\"train_run-{uuid.uuid4().hex}\",\n",
    "    )\n",
    "    trainer = TorchTrainer(\n",
    "        train_loop_per_worker=train_func_per_worker,\n",
    "        train_loop_config=train_config,\n",
    "        scaling_config=scaling_config,\n",
    "        run_config=run_config,\n",
    "    )\n",
    "    result = trainer.fit()\n",
    "    print(f\"Training result: {result}\")\n",
    "\n",
    "if __name__ == \"__main__\":\n",
    "    train_cifar_10(num_workers=8, use_gpu=True)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Because you're running training in a data parallel fashion this time, it should take under 1 minute while maintaining similar accuracy."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "\n",
    "If you are using an Anyscale Workspace, go to the Ray Train dashboard to analyze your distributed training job. Click **Ray Workloads** and then **Ray Train**, which shows a list of all the training runs you have kicked off.\n",
    "\n",
    "![Train Runs](https://raw.githubusercontent.com/ray-project/ray/master/doc/source/train/examples/pytorch/distributing-pytorch/images/train_runs.png)\n",
    "\n",
    "Clicking the run displays an overview page that shows logs from the controller, which is the process that coordinates your entire Ray Train job, as well as information about the 8 training workers.\n",
    "\n",
    "![Train Run](https://raw.githubusercontent.com/ray-project/ray/master/doc/source/train/examples/pytorch/distributing-pytorch/images/train_run.png)\n",
    "\n",
    "Click an individual worker for a more detailed worker page.\n",
    "\n",
    "![Train Worker](https://raw.githubusercontent.com/ray-project/ray/master/doc/source/train/examples/pytorch/distributing-pytorch/images/train_worker.png)\n",
    "\n",
    "If your worker is still alive, you can click **CPU Flame Graph**, **Stack Trace**, or **Memory Profiling** links in the overall run page or the individual worker page. Clicking **CPU Flame Graph** profiles your run with py-spy for 5 seconds and shows a CPU flame graph. Clicking **Stack Trace** shows the current stack trace of your job, which is useful for debugging hanging jobs. Finally, clicking **Memory Profiling** profiles your run with memray for 5 seconds and shows a memory flame graph.\n",
    "\n",
    "You can also click the **Metrics** tab on the navigation bar to view useful stats about the cluster, such as GPU utilization and metrics about Ray [actors](https://docs.ray.io/en/latest/ray-core/actors.html) and [tasks](https://docs.ray.io/en/latest/ray-core/tasks.html).\n",
    "\n",
    "![Metrics Dashboard](https://raw.githubusercontent.com/ray-project/ray/master/doc/source/train/examples/pytorch/distributing-pytorch/images/metrics_dashboard.png)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Step 3: Scale data ingest separately from training with Ray Data\n",
    "\n",
    "\n",
    "Modify this example to load data with Ray Data instead of the native PyTorch DataLoader. With a few modifications, you can scale data preprocessing and training separately. For example, you can do the former with a pool of CPU workers and the latter with a pool of GPU workers. See [How does Ray Data compare to other solutions for offline inference?](https://docs.ray.io/en/latest/data/comparisons.html#how-does-ray-data-compare-to-other-solutions-for-ml-training-ingest) for a comparison between Ray Data and PyTorch data loading.\n",
    "\n",
    "First, create [Ray Data Datasets](https://docs.ray.io/en/latest/data/key-concepts.html#datasets-and-blocks) from S3 data and inspect their schemas."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "import ray.data\n",
    "\n",
    "import numpy as np\n",
    "\n",
    "STORAGE_PATH = \"s3://ray-example-data/cifar10-parquet\"\n",
    "train_dataset = ray.data.read_parquet(f'{STORAGE_PATH}/train')\n",
    "test_dataset = ray.data.read_parquet(f'{STORAGE_PATH}/test')\n",
    "train_dataset.schema()\n",
    "test_dataset.schema()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Next, use Ray Data to transform the data. Note that both loading and transformation happen lazily, which means that only the training workers materialize the data."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "def transform_cifar(row: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:\n",
    "    # Define the torchvision transform.\n",
    "    transform = transforms.Compose([ToTensor(), Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))])\n",
    "    row[\"image\"] = transform(row[\"image\"])\n",
    "    return row\n",
    "\n",
    "train_dataset = train_dataset.map(transform_cifar)\n",
    "test_dataset = test_dataset.map(transform_cifar)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Next, modify the training function you wrote earlier. Every difference from the previous script is highlighted and explained with a numbered comment; for example, “[1].”"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "def train_func_per_worker(config: Dict):\n",
    "    lr = config[\"lr\"]\n",
    "    epochs = config[\"epochs\"]\n",
    "    batch_size = config[\"batch_size_per_worker\"]\n",
    "\n",
    "\n",
    "    # [1] Prepare `Dataloader` for distributed training.\n",
    "    # The get_dataset_shard method gets the associated dataset shard to pass to the \n",
    "    # TorchTrainer constructor in the next code block.\n",
    "    # The iter_torch_batches method lazily shards the dataset among workers.\n",
    "    # =============================================================================\n",
    "    train_data_shard = ray.train.get_dataset_shard(\"train\")\n",
    "    valid_data_shard = ray.train.get_dataset_shard(\"valid\")\n",
    "    train_dataloader = train_data_shard.iter_torch_batches(batch_size=batch_size)\n",
    "    valid_dataloader = valid_data_shard.iter_torch_batches(batch_size=batch_size)\n",
    "\n",
    "    model = VisionTransformer(\n",
    "        image_size=32,   # CIFAR-10 image size is 32x32\n",
    "        patch_size=4,    # Patch size is 4x4\n",
    "        num_layers=12,   # Number of transformer layers\n",
    "        num_heads=8,     # Number of attention heads\n",
    "        hidden_dim=384,  # Hidden size (can be adjusted)\n",
    "        mlp_dim=768,     # MLP dimension (can be adjusted)\n",
    "        num_classes=10   # CIFAR-10 has 10 classes\n",
    "    )\n",
    "\n",
    "    model = ray.train.torch.prepare_model(model)\n",
    "\n",
    "    loss_fn = nn.CrossEntropyLoss()\n",
    "    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-2)\n",
    "\n",
    "    # Model training loop.\n",
    "    for epoch in range(epochs):\n",
    "        model.train()\n",
    "        for batch in tqdm(train_dataloader, desc=f\"Train Epoch {epoch}\"):\n",
    "            X, y = batch['image'], batch['label']\n",
    "            pred = model(X)\n",
    "            loss = loss_fn(pred, y)\n",
    "\n",
    "            optimizer.zero_grad()\n",
    "            loss.backward()\n",
    "            optimizer.step()\n",
    "\n",
    "        model.eval()\n",
    "        valid_loss, num_correct, num_total, num_batches = 0, 0, 0, 0\n",
    "        with torch.no_grad():\n",
    "            for batch in tqdm(valid_dataloader, desc=f\"Valid Epoch {epoch}\"):\n",
    "                # [2] Each Ray Data batch is a dict so you must access the\n",
    "                # underlying data using the appropriate keys.\n",
    "                # =======================================================\n",
    "                X, y = batch['image'], batch['label']\n",
    "                pred = model(X)\n",
    "                loss = loss_fn(pred, y)\n",
    "\n",
    "                valid_loss += loss.item()\n",
    "                num_total += y.shape[0]\n",
    "                num_batches += 1\n",
    "                num_correct += (pred.argmax(1) == y).sum().item()\n",
    "\n",
    "        valid_loss /= num_batches\n",
    "        accuracy = num_correct / num_total\n",
    "\n",
    "        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:\n",
    "            torch.save(\n",
    "                model.module.state_dict(),\n",
    "                os.path.join(temp_checkpoint_dir, \"model.pt\")\n",
    "            )\n",
    "            ray.train.report(\n",
    "                metrics={\"loss\": valid_loss, \"accuracy\": accuracy},\n",
    "                checkpoint=ray.train.Checkpoint.from_directory(temp_checkpoint_dir),\n",
    "            )\n",
    "            if ray.train.get_context().get_world_rank() == 0:\n",
    "                print({\"epoch_num\": epoch, \"loss\": valid_loss, \"accuracy\": accuracy})"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Finally, run the training function with the Ray Data Dataset on the Ray Cluster with 8 GPU workers."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "def train_cifar_10(num_workers, use_gpu):\n",
    "    global_batch_size = 512\n",
    "\n",
    "    train_config = {\n",
    "        \"lr\": 1e-3,\n",
    "        \"epochs\": 1,\n",
    "        \"batch_size_per_worker\": global_batch_size // num_workers,\n",
    "    }\n",
    "\n",
    "    # Configure computation resources.\n",
    "    scaling_config = ScalingConfig(num_workers=num_workers, use_gpu=use_gpu)\n",
    "    run_config = RunConfig(\n",
    "        storage_path=\"/mnt/cluster_storage\", \n",
    "        name=f\"train_data_run-{uuid.uuid4().hex}\",\n",
    "    )\n",
    "\n",
    "    # Initialize a Ray TorchTrainer.\n",
    "    trainer = TorchTrainer(\n",
    "        train_loop_per_worker=train_func_per_worker,\n",
    "        # [1] With Ray Data you pass the Dataset directly to the Trainer.\n",
    "        # ==============================================================\n",
    "        datasets={\"train\": train_dataset, \"valid\": test_dataset},\n",
    "        train_loop_config=train_config,\n",
    "        scaling_config=scaling_config,\n",
    "        run_config=run_config,\n",
    "    )\n",
    "\n",
    "    result = trainer.fit()\n",
    "    print(f\"Training result: {result}\")\n",
    "\n",
    "if __name__ == \"__main__\":\n",
    "    train_cifar_10(num_workers=8, use_gpu=True)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Once again your training run should take around 1 minute with similar accuracy. There aren't big performance wins with Ray Data on this example due to the small size of the dataset; for more interesting benchmarking information see [this blog post](https://www.anyscale.com/blog/fast-flexible-scalable-data-loading-for-ml-training-with-ray-data). The main advantage of Ray Data is that it allows you to stream data across heterogeneous compute, maximizing GPU utilization while minimizing RAM usage."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "\n",
    "If you are using an Anyscale Workspace, in addition to the Ray Train and Metrics dashboards you saw in the previous section, you can also view the Ray Data dashboard by clicking **Ray Workloads** and then **Data** where you can view the throughput and status of each [Ray Data operator](https://docs.ray.io/en/latest/data/key-concepts.html#operators-and-plans).\n",
    "\n",
    "![Data Dashboard](https://raw.githubusercontent.com/ray-project/ray/master/doc/source/train/examples/pytorch/distributing-pytorch/images/data_dashboard.png)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Summary\n",
    "\n",
    "In this notebook, you:\n",
    "* Trained a PyTorch VisionTransformer model on a Ray Cluster with multiple GPU workers with Ray Train and Ray Data\n",
    "* Verified that speed improved without affecting accuracy\n",
    "* Gained insight into your distributed training and data preprocessing workloads with various dashboards"
   ]
  }
 ],
 "metadata": {
  "language_info": {
   "name": "python"
  },
  "orphan": true
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
