(kuberay-distributed-checkpointing-gcsefuse)=

# Distributed checkpointing with KubeRay and GCSFuse

This example orchestrates distributed checkpointing with KubeRay, using the GCSFuse CSI driver and Google Cloud Storage as the remote storage system.
To illustrate the concepts, this guide uses the [Finetuning a Pytorch Image Classifier with Ray Train](https://docs.ray.io/en/latest/train/examples/pytorch/pytorch_resnet_finetune.html) example.

## Why distributed checkpointing with GCSFuse?

In large-scale, high-performance machine learning, distributed checkpointing is crucial for fault tolerance, ensuring that if a node fails during training, Ray can resume the process from the latest saved checkpoint instead of starting from scratch.
While it's possible to directly reference remote storage paths (e.g., `gs://my-checkpoint-bucket`), using Google Cloud Storage FUSE (GCSFuse) has distinct advantages for distributed applications. GCSFuse allows you to mount Cloud Storage buckets like local file systems, making checkpoint management more intuitive for distributed applications that rely on these semantics. Furthermore, GCSFuse is designed for high-performance workloads, delivering the performance and scalability you need for distributed checkpointing of large models.

[Distributed checkpointing](https://docs.ray.io/en/latest/train/user-guides/checkpoints.html), in combination with [GCSFuse](https://cloud.google.com/storage/docs/gcs-fuse), allows for larger-scale model training with increased availability and efficiency.

## Create a Kubernetes cluster on GKE

Create a GKE cluster with the [GCSFuse CSI driver](https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/cloud-storage-fuse-csi-driver) and [Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity) enabled, as well as a GPU node pool with 4 L4 GPUs:
```
export PROJECT_ID=<your project id>
gcloud container clusters create kuberay-with-gcsfuse \
    --addons GcsFuseCsiDriver \
    --cluster-version=1.29.4 \
    --location=us-east4-c \
    --machine-type=g2-standard-8 \
    --release-channel=rapid \
    --num-nodes=4 \
    --accelerator type=nvidia-l4,count=1,gpu-driver-version=latest \
    --workload-pool=${PROJECT_ID}.svc.id.goog
```

Verify the successful creation of your cluster with 4 GPUs:
```
$ kubectl get nodes "-o=custom-columns=NAME:.metadata.name,GPU:.status.allocatable.nvidia\.com/gpu"
NAME                                                  GPU
gke-kuberay-with-gcsfuse-default-pool-xxxx-0000       1
gke-kuberay-with-gcsfuse-default-pool-xxxx-1111       1
gke-kuberay-with-gcsfuse-default-pool-xxxx-2222       1
gke-kuberay-with-gcsfuse-default-pool-xxxx-3333       1
```

## Install the KubeRay operator

Follow [Deploy a KubeRay operator](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.
The KubeRay operator Pod must be on the CPU node if you set up the taint for the GPU node pool correctly.

## Configuring the GCS Bucket

Create a GCS bucket that Ray uses as the remote filesystem.
```
BUCKET=<your GCS bucket>
gcloud storage buckets create gs://$BUCKET --uniform-bucket-level-access
```

Create a Kubernetes ServiceAccount that grants the RayCluster access to mount the GCS bucket:
```
kubectl create serviceaccount pytorch-distributed-training
```

Bind the `roles/storage.objectUser` role to the Kubernetes service account and bucket IAM policy.
See [Identifying projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects#identifying_projects) to find your project ID and project number:
```
PROJECT_ID=<your project ID>
PROJECT_NUMBER=<your project number>
gcloud storage buckets add-iam-policy-binding gs://${BUCKET} --member "principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/subject/ns/default/sa/pytorch-distributed-training"  --role "roles/storage.objectUser"
```

See [Access Cloud Storage buckets with the Cloud Storage FUSE CSI driver](https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/cloud-storage-fuse-csi-driver) for more details.

## Deploy the RayJob

Download the RayJob that executes all the steps documented in [Finetuning a Pytorch Image Classifier with Ray Train](https://docs.ray.io/en/latest/train/examples/pytorch/pytorch_resnet_finetune.html). The [source code](https://github.com/ray-project/kuberay/tree/master/ray-operator/config/samples/pytorch-resnet-image-classifier) is also in the KubeRay repository.

```
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/pytorch-resnet-image-classifier/ray-job.pytorch-image-classifier.yaml
```

Modify the RayJob by replacing all instances of the `GCS_BUCKET` placeholder with the Google Cloud Storage bucket you created earlier. Alternatively you can use `sed`:
```
sed -i "s/GCS_BUCKET/$BUCKET/g" ray-job.pytorch-image-classifier.yaml
```

Deploy the RayJob:
```
kubectl create -f ray-job.pytorch-image-classifier.yaml
```

The deployed RayJob includes the following configuration to enable distributed checkpointing to a shared filesystem:
* 4 Ray workers, each with a single GPU.
* All Ray nodes use the `pytorch-distributed-training` ServiceAccount, which we created earlier.
* Includes volumes that are managed by the `gcsfuse.csi.storage.gke.io` CSI driver.
* Mounts a shared storage path `/mnt/cluster_storage`, backed by the GCS bucket you created earlier.

You can configure the Pod with annotations, which allows for finer grain control of the GCSFuse sidecar container. See [Specify Pod annotations](https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/cloud-storage-fuse-csi-driver#pod-annotations) for more details.
```
annotations:
  gke-gcsfuse/volumes: "true"
  gke-gcsfuse/cpu-limit: "0"
  gke-gcsfuse/memory-limit: 5Gi
  gke-gcsfuse/ephemeral-storage-limit: 10Gi
```

You can also specify mount options when defining the GCSFuse container volume:
```
csi:
  driver: gcsfuse.csi.storage.gke.io
  volumeAttributes:
    bucketName: GCS_BUCKET
    mountOptions: "implicit-dirs,uid=1000,gid=100"
```

See [Mount options](https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/cloud-storage-fuse-csi-driver#mount-options) to learn more about mount options.

Logs from the Ray job should indicate the use of the shared remote filesystem in `/mnt/cluster_storage` and the checkpointing directory. For example:
```
Training finished iteration 10 at 2024-04-29 10:22:08. Total running time: 1min 30s
╭─────────────────────────────────────────╮
│ Training result                         │
├─────────────────────────────────────────┤
│ checkpoint_dir_name   checkpoint_000009 │
│ time_this_iter_s                6.47154 │
│ time_total_s                    74.5547 │
│ training_iteration                   10 │
│ acc                             0.24183 │
│ loss                            0.06882 │
╰─────────────────────────────────────────╯
Training saved a checkpoint for iteration 10 at: (local)/mnt/cluster_storage/finetune-resnet/TorchTrainer_cbb82_00000_0_2024-04-29_10-20-37/checkpoint_000009
```

## Inspect checkpointing data

Once the RayJob completes, you can inspect the contents of your bucket using a tool like [gsutil](https://cloud.google.com/storage/docs/gsutil).
```
gsutil ls gs://my-ray-bucket/**
gs://my-ray-bucket/finetune-resnet/
gs://my-ray-bucket/finetune-resnet/.validate_storage_marker
gs://my-ray-bucket/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/
gs://my-ray-bucket/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/checkpoint_000007/
gs://my-ray-bucket/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/checkpoint_000007/checkpoint.pt
gs://my-ray-bucket/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/checkpoint_000008/
gs://my-ray-bucket/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/checkpoint_000008/checkpoint.pt
gs://my-ray-bucket/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/checkpoint_000009/
gs://my-ray-bucket/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/checkpoint_000009/checkpoint.pt
gs://my-ray-bucket/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/error.pkl
gs://my-ray-bucket/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/error.txt
gs://my-ray-bucket/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/events.out.tfevents.1714436502.orch-image-classifier-nc2sq-raycluster-tdrfx-head-xzcl8
gs://my-ray-bucket/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/events.out.tfevents.1714436809.orch-image-classifier-zz4sj-raycluster-vn7kz-head-lwx8k
gs://my-ray-bucket/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/params.json
gs://my-ray-bucket/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/params.pkl
gs://my-ray-bucket/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/progress.csv
gs://my-ray-bucket/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/result.json
gs://my-ray-bucket/finetune-resnet/basic-variant-state-2024-04-29_17-21-29.json
gs://my-ray-bucket/finetune-resnet/basic-variant-state-2024-04-29_17-26-35.json
gs://my-ray-bucket/finetune-resnet/experiment_state-2024-04-29_17-21-29.json
gs://my-ray-bucket/finetune-resnet/experiment_state-2024-04-29_17-26-35.json
gs://my-ray-bucket/finetune-resnet/trainer.pkl
gs://my-ray-bucket/finetune-resnet/tuner.pkl

```

## Resuming from checkpoint

In the event of a failed job, you can use the latest checkpoint to resume training of the model. This example configures `TorchTrainer` to automatically resume
from the latest checkpoint:
```python
experiment_path = os.path.expanduser("/mnt/cluster_storage/finetune-resnet")
if TorchTrainer.can_restore(experiment_path):
    trainer = TorchTrainer.restore(experiment_path,
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config=train_loop_config,
        scaling_config=scaling_config,
        run_config=run_config,
    )
else:
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config=train_loop_config,
        scaling_config=scaling_config,
        run_config=run_config,
    )
```

You can verify automatic checkpoint recovery by redeploying the same RayJob:
```
kubectl create -f ray-job.pytorch-image-classifier.yaml
```

If the previous job succeeded, the training job should restore the checkpoint state from the `checkpoint_000009` directory
and then immediately complete training with 0 iterations:
```
2024-04-29 15:51:32,528 INFO experiment_state.py:366 -- Trying to find and download experiment checkpoint at /mnt/cluster_storage/finetune-resnet
2024-04-29 15:51:32,651 INFO experiment_state.py:396 -- A remote experiment checkpoint was found and will be used to restore the previous experiment state.
2024-04-29 15:51:32,652 INFO tune_controller.py:404 -- Using the newest experiment state file found within the experiment directory: experiment_state-2024-04-29_15-43-40.json

View detailed results here: /mnt/cluster_storage/finetune-resnet
To visualize your results with TensorBoard, run: `tensorboard --logdir /home/ray/ray_results/finetune-resnet`

Result(
  metrics={'loss': 0.070047477101968, 'acc': 0.23529411764705882},
  path='/mnt/cluster_storage/finetune-resnet/TorchTrainer_ecc04_00000_0_2024-04-29_15-43-40',
  filesystem='local',
  checkpoint=Checkpoint(filesystem=local, path=/mnt/cluster_storage/finetune-resnet/TorchTrainer_ecc04_00000_0_2024-04-29_15-43-40/checkpoint_000009)
)
```

If the previous job failed at an earlier checkpoint, the job should resume from the last saved checkpoint and run until `max_epochs=10`. For example, if the last run
failed at epoch 7, the training automatically resumes using `checkpoint_000006` and run 3 more iterations until epoch 10:
```
(TorchTrainer pid=611, ip=10.108.2.65) Restored on 10.108.2.65 from checkpoint: Checkpoint(filesystem=local, path=/mnt/cluster_storage/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/checkpoint_000006)
(RayTrainWorker pid=671, ip=10.108.2.65) Setting up process group for: env:// [rank=0, world_size=4]
(TorchTrainer pid=611, ip=10.108.2.65) Started distributed worker processes:
(TorchTrainer pid=611, ip=10.108.2.65) - (ip=10.108.2.65, pid=671) world_rank=0, local_rank=0, node_rank=0
(TorchTrainer pid=611, ip=10.108.2.65) - (ip=10.108.1.83, pid=589) world_rank=1, local_rank=0, node_rank=1
(TorchTrainer pid=611, ip=10.108.2.65) - (ip=10.108.0.72, pid=590) world_rank=2, local_rank=0, node_rank=2
(TorchTrainer pid=611, ip=10.108.2.65) - (ip=10.108.3.76, pid=590) world_rank=3, local_rank=0, node_rank=3
(RayTrainWorker pid=589, ip=10.108.1.83) Downloading: "https://download.pytorch.org/models/resnet50-0676ba61.pth" to /home/ray/.cache/torch/hub/checkpoints/resnet50-0676ba61.pth
(RayTrainWorker pid=671, ip=10.108.2.65)
  0%|          | 0.00/97.8M [00:00<?, ?B/s]
(RayTrainWorker pid=671, ip=10.108.2.65)
 22%|██▏       | 21.8M/97.8M [00:00<00:00, 229MB/s]
(RayTrainWorker pid=671, ip=10.108.2.65)
 92%|█████████▏| 89.7M/97.8M [00:00<00:00, 327MB/s]
(RayTrainWorker pid=671, ip=10.108.2.65)
100%|██████████| 97.8M/97.8M [00:00<00:00, 316MB/s]
(RayTrainWorker pid=671, ip=10.108.2.65) Moving model to device: cuda:0
(RayTrainWorker pid=671, ip=10.108.2.65) Wrapping provided model in DistributedDataParallel.
(RayTrainWorker pid=671, ip=10.108.2.65) Downloading: "https://download.pytorch.org/models/resnet50-0676ba61.pth" to /home/ray/.cache/torch/hub/checkpoints/resnet50-0676ba61.pth [repeated 3x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication, or see https://docs.ray.io/en/master/ray-observability/ray-logging.html#log-deduplication for more options.)
(RayTrainWorker pid=590, ip=10.108.3.76)
  0%|          | 0.00/97.8M [00:00<?, ?B/s] [repeated 3x across cluster]
(RayTrainWorker pid=590, ip=10.108.0.72)
 85%|████████▍ | 82.8M/97.8M [00:00<00:00, 256MB/s]
100%|██████████| 97.8M/97.8M [00:00<00:00, 231MB/s] [repeated 11x across cluster]
(RayTrainWorker pid=590, ip=10.108.3.76)
100%|██████████| 97.8M/97.8M [00:00<00:00, 238MB/s]
(RayTrainWorker pid=671, ip=10.108.2.65) Epoch 7-train Loss: 0.0903 Acc: 0.2418
(RayTrainWorker pid=671, ip=10.108.2.65) Epoch 7-val Loss: 0.0881 Acc: 0.2353
(RayTrainWorker pid=590, ip=10.108.0.72) Checkpoint successfully created at: Checkpoint(filesystem=local, path=/mnt/cluster_storage/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/checkpoint_000007)
(RayTrainWorker pid=590, ip=10.108.0.72) Moving model to device: cuda:0 [repeated 3x across cluster]
(RayTrainWorker pid=590, ip=10.108.0.72) Wrapping provided model in DistributedDataParallel. [repeated 3x across cluster]

Training finished iteration 8 at 2024-04-29 17:27:29. Total running time: 54s
╭─────────────────────────────────────────╮
│ Training result                         │
├─────────────────────────────────────────┤
│ checkpoint_dir_name   checkpoint_000007 │
│ time_this_iter_s               40.46113 │
│ time_total_s                   95.00043 │
│ training_iteration                    8 │
│ acc                             0.23529 │
│ loss                            0.08811 │
╰─────────────────────────────────────────╯
Training saved a checkpoint for iteration 8 at: (local)/mnt/cluster_storage/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/checkpoint_000007
(RayTrainWorker pid=671, ip=10.108.2.65) Epoch 8-train Loss: 0.0893 Acc: 0.2459
(RayTrainWorker pid=671, ip=10.108.2.65) Epoch 8-val Loss: 0.0859 Acc: 0.2353
(RayTrainWorker pid=589, ip=10.108.1.83) Checkpoint successfully created at: Checkpoint(filesystem=local, path=/mnt/cluster_storage/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/checkpoint_000008) [repeated 4x across cluster]

Training finished iteration 9 at 2024-04-29 17:27:36. Total running time: 1min 1s
╭─────────────────────────────────────────╮
│ Training result                         │
├─────────────────────────────────────────┤
│ checkpoint_dir_name   checkpoint_000008 │
│ time_this_iter_s                5.99923 │
│ time_total_s                  100.99965 │
│ training_iteration                    9 │
│ acc                             0.23529 │
│ loss                            0.08592 │
╰─────────────────────────────────────────╯
Training saved a checkpoint for iteration 9 at: (local)/mnt/cluster_storage/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/checkpoint_000008
2024-04-29 17:27:37,170 WARNING util.py:202 -- The `process_trial_save` operation took 0.540 s, which may be a performance bottleneck.
(RayTrainWorker pid=671, ip=10.108.2.65) Epoch 9-train Loss: 0.0866 Acc: 0.2377
(RayTrainWorker pid=671, ip=10.108.2.65) Epoch 9-val Loss: 0.0833 Acc: 0.2353
(RayTrainWorker pid=589, ip=10.108.1.83) Checkpoint successfully created at: Checkpoint(filesystem=local, path=/mnt/cluster_storage/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/checkpoint_000009) [repeated 4x across cluster]

Training finished iteration 10 at 2024-04-29 17:27:43. Total running time: 1min 8s
╭─────────────────────────────────────────╮
│ Training result                         │
├─────────────────────────────────────────┤
│ checkpoint_dir_name   checkpoint_000009 │
│ time_this_iter_s                6.71457 │
│ time_total_s                  107.71422 │
│ training_iteration                   10 │
│ acc                             0.23529 │
│ loss                            0.08333 │
╰─────────────────────────────────────────╯
Training saved a checkpoint for iteration 10 at: (local)/mnt/cluster_storage/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/checkpoint_000009

Training completed after 10 iterations at 2024-04-29 17:27:45. Total running time: 1min 9s
2024-04-29 17:27:46,236 WARNING experiment_state.py:323 -- Experiment checkpoint syncing has been triggered multiple times in the last 30.0 seconds. A sync will be triggered whenever a trial has checkpointed more than `num_to_keep` times since last sync or if 300 seconds have passed since last sync. If you have set `num_to_keep` in your `CheckpointConfig`, consider increasing the checkpoint frequency or keeping more checkpoints. You can suppress this warning by changing the `TUNE_WARN_EXCESSIVE_EXPERIMENT_CHECKPOINT_SYNC_THRESHOLD_S` environment variable.

Result(
  metrics={'loss': 0.08333033206416111, 'acc': 0.23529411764705882},
  path='/mnt/cluster_storage/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29',
  filesystem='local',
  checkpoint=Checkpoint(filesystem=local, path=/mnt/cluster_storage/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/checkpoint_000009)
)
(RayTrainWorker pid=590, ip=10.108.3.76) Checkpoint successfully created at: Checkpoint(filesystem=local, path=/mnt/cluster_storage/finetune-resnet/TorchTrainer_96923_00000_0_2024-04-29_17-21-29/checkpoint_000009) [repeated 3x across cluster]
```
