# DreamBooth fine-tuning of Stable Diffusion with Ray Train

| Template Specification | Description |
| ---------------------- | ----------- |
| Summary | This example shows how to do [DreamBooth fine-tuning](https://dreambooth.github.io/) of a Stable Diffusion model using Ray Train for data-parallel training with many workers and Ray Data for data ingestion. Use one of the provided datasets, or supply your own photos. By the end of this example, you'll be able to generate images of your subject in a variety of situations, just by feeding in a text prompt! |
| Time to Run | ~10-15 minutes to generate a regularization dataset and fine-tune the model on photos of your subject. |
| Minimum Compute Requirements | At least 1 GPUs, where each GPU has >= 24GB GRAM. The default is 1 node with 4 GPUS: A10G GPU (AWS) or L4 GPU (GCE). |
| Cluster Environment | This template uses a docker image built on top of the latest Anyscale-provided Ray image using Python 3.9: [`anyscale/ray:latest-py39-cu118`](https://docs.anyscale.com/reference/base-images/overview). See the appendix below for more details. |

![Dreambooth fine-tuning sample results](https://raw.githubusercontent.com/ray-project/ray/workspace_templates_2.6.1/doc/source/templates/05_dreambooth_finetuning/dreambooth/images/dreambooth_example.png)

## Run the example

This README will only contain minimal instructions on running this example on Anyscale.
See [the guide on the Ray documentation](https://docs.ray.io/en/latest/ray-air/examples/dreambooth_finetuning.html)
for a step-by-step walkthrough of the training code.

You can get started fine-tuning on a sample dog dataset with default settings with the following commands:

```bash
chmod +x ./dreambooth_run.sh
./dreambooth_run.sh
```

## Customizing the example

Here are a few modifications to the `dreambooth_run.sh` script that you may want to make:

1. The image dataset of your subject. This example provides two sample datasets, but you can also supply your own directory of 4-5 images, as well as the general class your subject falls under. For example, the dog dataset contains images of one particular puppy, and the general class this subject falls under is `dog`.
    - Modify the `$CLASS_NAME` and `$INSTANCE_DIR` environment variables.
2. The `$DATA_PREFIX` that the pre-trained model is downloaded to. This directory is also where the training dataset and the fine-tuned model checkpoint are written at the end of training.
    - If you add more worker nodes to the cluster, you should `$DATA_PREFIX` this to a shared NFS filesystem such as `/mnt/cluster_storage`. See [this page of the docs](https://docs.anyscale.com/develop/workspaces/storage#storage-shared-across-nodes) for all the options.
    - Note that each run of the script will overwrite the fine-tuned model checkpoint from the previous run, so consider changing the `$DATA_PREFIX` environment variable on each run if you don't want to lose the models/data of previous runs.
3. The `$NUM_WORKERS` variable sets the number of data-parallel workers used during fine-tuning. The default is 2 workers (2 workers, each using 1 GPU), and you should increase this number if you add more GPU worker nodes to the cluster.
4. Setting `--num_epochs` and `--max_train_steps` determines the number of fine-tuning steps to take.
    - Depending on the batch size and number of data-parallel workers, one epoch will run for a certain number of steps. The run will terminate when one of these values (epoch vs. total number of steps) is reached.
5. `generate.py` is used to generate stable diffusion images after loading the model from a checkpoint. You should modify the prompt at the end to be something more interesting, rather than just a photo of your subject.
6. If you want to launch another fine-tuning run, you may want to run *only* the `python train.py ...` command. Running the bash script will start from the beginning (generating another regularization dataset).
7. Use the following command for LoRA fine-tuning. 
```bash
python train.py \
  --model_dir=$ORIG_MODEL_PATH \
  --output_dir=$TUNED_MODEL_DIR \
  --instance_images_dir=$IMAGES_OWN_DIR \
  --instance_prompt="photo of $UNIQUE_TOKEN $CLASS_NAME" \
  --class_images_dir=$IMAGES_REG_DIR \
  --class_prompt="photo of a $CLASS_NAME" \
  --train_batch_size=2 \
  --lr=1e-4 \  # Note a much higher learning rate here!
  --num_epochs=10 \
  --max_train_steps=400 \
  --num_workers $NUM_WORKERS
  --use_lora
```

## Interact with the fine-tuned model

### Generate images with a script

Use the `generate.py` script to generate images with a prompt.
Replace the variables with the values that you used in the fine-tuning script.
See `run_model_flags` in `flags.py` for a full list of available command line arguments to pass to the script.

```bash
python generate.py \
  --model_dir=$TUNED_MODEL_DIR \
  --output_dir=$IMAGES_NEW_DIR \
  --prompts="photo of a $UNIQUE_TOKEN $CLASS_NAME" \
  --num_samples_per_prompt=5
```

To generate images using LoRA fine-tuned model:

```bash
python generate.py \
  --model_dir=$ORIG_MODEL_PATH \
  --lora_weights_dir=$TUNED_MODEL_DIR \
  --output_dir=$IMAGES_NEW_DIR \
  --prompts="photo of a $UNIQUE_TOKEN $CLASS_NAME" \
  --num_samples_per_prompt=5
```

### Generate images interactively in a notebook

See the `playground.ipynb` notebook for a more interactive way to generate images with the fine-tuned model.
Click on the Jupyter icon on the workspace page and open the notebook. *Note: The widgets in this notebook don't work in VSCode, so please use Jupyter!*

## Appendix

### Advanced: Build off of this template's cluster environment

#### Option 1: Build a new cluster environment on Anyscale

The requirements are listed in `dreambooth/requirements.txt`. Feel free to modify this to include more requirements, then follow [this guide](https://docs.anyscale.com/configure/dependency-management/cluster-environments#creating-a-cluster-environment) to use the `anyscale` CLI to create a new cluster environment. The requirements should be pasted into the cluster environment yaml.

Finally, update your workspace's cluster environment to this new one after it's done building.

#### Option 2: Build a new docker image with your own infrastructure

Use the following `docker pull` command if you want to manually build a new Docker image based off of this one.

```bash
docker pull us-docker.pkg.dev/anyscale-workspace-templates/workspace-templates/dreambooth-finetuning:latest
```
