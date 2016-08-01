# Training AlexNet

WARNING: Running this application is fairly involved. In particular, it requires
you to download the ImageNet dataset and put it on S3.

This document walks through how to load the ImageNet dataset from S3 and train
AlexNet using data parallel stochastic gradient descent.

## Running the Application

The instructions in this section must be done before you can run the
application.

### Install the Dependencies

Install the following dependencies.

- [TensorFlow](https://www.tensorflow.org/)

In addition, install the following dependencies.

**On Ubuntu**

```
sudo apt-get install libjpeg8-dev awscli
sudo pip install boto3 botocore pillow
```

**On Mac OSX**

```
brew install libjpeg awscli
sudo pip install boto3 botocore pillow
```

### Put ImageNet on S3

1. To run this application, first put the ImageNet tar files on S3 (e.g., in the
directory `ILSVRC2012_img_train`). Also, put the file `train.txt` on S3. We will
use `$S3_BUCKET` to refer to the name of your S3 bucket.

2. Use `aws configure` to enable Boto to connect to S3. If you are using
multiple machines, this must be done on all machines.

### Run the Application

From the directory `ray/examples/alexnet/` run the following

```
source ../../setup-env.sh
python driver.py --s3-bucket $S3_BUCKET
```

## Parallel Data Loading

To speed up data loading, we will pull data from S3 in parallel with a number of
workers. At the core of our loading code is the remote function
`load_tarfile_from_s3`. When executed, this function connects to S3 and
retrieves the appropriate object.

```python
@ray.remote([str, str, List], [np.ndarray, List])
def load_tarfile_from_s3(bucket, s3_key, size=[]):
  # Pull the object with the given key and bucket from S3, untar the contents,
  # and return it.
  return images, labels
```

To load data in parallel, we simply call this function multiple times with the
keys of all the objects that we want to retrieve. This returns a list of pairs
of object IDs, where the first object ID in each pair refers to a
batch of images and the second refers to the corresponding batch of labels.

```python
batches = [load_tarfile_from_s3.remote(bucket, s3_key, size) for s3_key in s3_keys]
```

By default, this will only fetch objects whose keys have prefix
`ILSVRC2012_img_train/n015` (this is 13 tar files). To fetch all of the data,
pass in `--key-prefix ILSVRC2012_img_train/n`.

## Data Parallel Training

The other parallel component of this application is the training procedure. This
is built on top of the remote function `compute_grad`.

```python
@ray.remote([np.ndarray, np.ndarray, np.ndarray, List], [List])
def compute_grad(X, Y, mean, weights):
  # Load the weights into the network.
  # Subtract the mean and crop the images.
  # Compute the gradients.
  return gradients
```

This function takes training inputs and outputs, the mean image (to subtract off
of the input images), and the current network weights.

We can parallelize the computation of the gradient over multiple batches by
calling `compute_grad` multiple times in parallel.

```python
gradient_ids = []
for i in range(num_workers):
  # Choose a random batch and use it to compute the gradient of the loss.
  x_id, y_id = batches[np.random.randint(len(batches))]
  gradient_ids.append(compute_grad.remote(x_id, y_id, mean_id, weights_id))
```
