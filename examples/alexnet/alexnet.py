# The code for AlexNet is copied and adapted from the TensorFlow repository
# https://github.com/tensorflow/tensorflow/blob/master/tensorflow/models/image/alexnet/alexnet_benchmark.py.

import ray
import numpy as np
import tarfile, io
import boto3
import PIL.Image as Image
import tensorflow as tf

import ray.array.remote as ra

STDDEV = 0.001 # The standard deviation of the network weight initialization.

def load_chunk(tarfile, size=None):
  """Load a number of images from a single imagenet .tar file.

  This function also converts the image from grayscale to RGB if necessary.

  Args:
    tarfile (tarfile.TarFile): The archive from which the files get loaded.
    size (Optional[Tuple[int, int]]): Resize the image to this size if provided.

  Returns:
    numpy.ndarray: Contains the image data in format [batch, w, h, c]
  """
  result = []
  filenames = []
  for member in tarfile.getmembers():
    filename = member.path
    content = tarfile.extractfile(member)
    img = Image.open(content)
    rgbimg = Image.new("RGB", img.size)
    rgbimg.paste(img)
    if size != None:
      rgbimg = rgbimg.resize(size, Image.ANTIALIAS)
    result.append(np.array(rgbimg).reshape(1, rgbimg.size[0], rgbimg.size[1], 3))
    filenames.append(filename)
  return np.concatenate(result), filenames

@ray.remote(num_return_vals=2)
def load_tarfile_from_s3(bucket, s3_key, size=[]):
  """Load an imagenet .tar file.

  Args:
    bucket (str): Bucket holding the imagenet .tar.
    s3_key (str): s3 key from which the .tar file is loaded.
    size (List[int]): Resize the image to this size if size != []; len(size) == 2 required.

  Returns:
    np.ndarray: The image data (see load_chunk).
  """
  s3 = boto3.client("s3")
  response = s3.get_object(Bucket=bucket, Key=s3_key)
  output = io.BytesIO()
  chunk = response["Body"].read(1024 * 8)
  while chunk:
    output.write(chunk)
    chunk = response["Body"].read(1024 * 8)
  output.seek(0) # go to the beginning of the .tar file
  tar = tarfile.open(mode="r", fileobj=output)
  return load_chunk(tar, size=size if size != [] else None)

def load_tarfiles_from_s3(bucket, s3_keys, size=[]):
  """Load a number of imagenet .tar files.

  Args:
    bucket (str): Bucket holding the imagenet .tars.
    s3_keys (List[str]): List of s3 keys from which the .tar files are being
      loaded.
    size (List[int]): Resize the image to this size if size does not equal [].
      The length of size must be 2.

  Returns:
    np.ndarray: Contains object IDs to the chunks of the images (see load_chunk).
  """

  return [load_tarfile_from_s3.remote(bucket, s3_key, size) for s3_key in s3_keys]

def setup_variables(params, placeholders, assigns, kernelshape, biasshape):
  """Creates the variables for each layer and adds the variables and the components needed to feed them to various lists

  Args:
    params (List): Network parameters used for creating feed_dicts
    placeholders (List): Placeholders used for feeding weights into
    assigns (List): Assignments used for actually setting variables
    kernelshape (List): Shape of the kernel used for the conv layer
    biasshape (List): Shape of the bias used

  Returns:
    None
  """
  kernel = tf.Variable(tf.truncated_normal(kernelshape, stddev=STDDEV))
  biases = tf.Variable(tf.constant(0.0, shape=biasshape, dtype=tf.float32),
                       trainable=True, name='biases')
  kernel_new = tf.placeholder(tf.float32, shape=kernel.get_shape())
  biases_new = tf.placeholder(tf.float32, shape=biases.get_shape())
  update_kernel = kernel.assign(kernel_new)
  update_biases = biases.assign(biases_new)
  params += [kernel, biases]
  placeholders += [kernel_new, biases_new]
  assigns += [update_kernel, update_biases]

def conv_layer(parameters, prev_layer, shape, scope):
  """Constructs a convolutional layer for the network.

  Args:
    parameters (List): Parameters used in constructing layer.
    prevlayer (Tensor): The previous layer to connect the network together.
    shape (List): The strides used for convolution
    scope (Scope): Current scope of tensorflow

  Returns:
    Tensor: Activation of layer
  """
  kernel = parameters[-2]
  bias = parameters[-1]
  conv = tf.nn.conv2d(prev_layer, kernel, shape, padding='SAME')
  add_bias = tf.nn.bias_add(conv, bias)
  return tf.nn.relu(add_bias, name=scope)

def net_initialization():
  images = tf.placeholder(tf.float32, shape=[None, 224, 224, 3])
  y_true = tf.placeholder(tf.float32, shape=[None, 1000])
  parameters = []
  assignment = []
  placeholders = []
  # conv1
  with tf.name_scope('conv1') as scope:
    setup_variables(parameters, placeholders, assignment, [11, 11, 3, 96], [96])
    conv1 = conv_layer(parameters, images, [1, 4, 4, 1], scope)

  # pool1
  pool1 = tf.nn.max_pool(conv1,
                         ksize=[1, 3, 3, 1],
                         strides=[1, 2, 2, 1],
                         padding='VALID',
                         name='pool1')

  # lrn1
  pool1_lrn = tf.nn.lrn(pool1, depth_radius=5, bias=1.0,
                               alpha=0.0001, beta=0.75,
                               name="LocalResponseNormalization")

  # conv2
  with tf.name_scope('conv2') as scope:
    setup_variables(parameters, placeholders, assignment, [5, 5, 96, 256], [256])
    conv2 = conv_layer(parameters, pool1_lrn, [1, 1, 1, 1], scope)

  pool2 = tf.nn.max_pool(conv2,
                         ksize=[1, 3, 3, 1],
                         strides=[1, 2, 2, 1],
                         padding='VALID',
                         name='pool2')

  # lrn2
  pool2_lrn = tf.nn.lrn(pool2, depth_radius=5, bias=1.0,
                               alpha=0.0001, beta=0.75,
                               name="LocalResponseNormalization")

  # conv3
  with tf.name_scope('conv3') as scope:
    setup_variables(parameters, placeholders, assignment, [3, 3, 256, 384], [384])
    conv3 = conv_layer(parameters, pool2_lrn, [1, 1, 1, 1], scope)

  # conv4
  with tf.name_scope('conv4') as scope:
    setup_variables(parameters, placeholders, assignment, [3, 3, 384, 384], [384])
    conv4 = conv_layer(parameters, conv3, [1, 1, 1, 1], scope)

  # conv5
  with tf.name_scope('conv5') as scope:
    setup_variables(parameters, placeholders, assignment, [3, 3, 384, 256], [256])
    conv5 = conv_layer(parameters, conv4, [1, 1, 1, 1], scope)

  # pool5
  pool5 = tf.nn.max_pool(conv5,
                         ksize=[1, 3, 3, 1],
                         strides=[1, 2, 2, 1],
                         padding='VALID',
                         name='pool5')

  # lrn5
  pool5_lrn = tf.nn.lrn(pool5, depth_radius=5, bias=1.0,
                               alpha=0.0001, beta=0.75,
                               name="LocalResponseNormalization")

  dropout = tf.placeholder(tf.float32)

  with tf.name_scope('fc1') as scope:
    n_input = int(np.prod(pool5_lrn.get_shape().as_list()[1:]))
    setup_variables(parameters, placeholders, assignment, [n_input, 4096], [4096])
    fc_in = tf.reshape(pool5_lrn, [-1, n_input])
    fc_layer1 = tf.nn.tanh(tf.nn.bias_add(tf.matmul(fc_in, parameters[-2]), parameters[-1]))
    fc_out1 = tf.nn.dropout(fc_layer1, dropout)

  with tf.name_scope('fc2') as scope:
    n_input = int(np.prod(fc_out1.get_shape().as_list()[1:]))
    setup_variables(parameters, placeholders, assignment, [n_input, 4096], [4096])
    fc_in = tf.reshape(fc_out1, [-1, n_input])
    fc_layer2 = tf.nn.tanh(tf.nn.bias_add(tf.matmul(fc_in, parameters[-2]), parameters[-1]))
    fc_out2 = tf.nn.dropout(fc_layer2, dropout)

  with tf.name_scope('fc3') as scope:
    n_input = int(np.prod(fc_out2.get_shape().as_list()[1:]))
    setup_variables(parameters, placeholders, assignment, [n_input, 1000], [1000])
    fc_in = tf.reshape(fc_out2, [-1, n_input])
    fc_layer3 = tf.nn.softmax(tf.nn.bias_add(tf.matmul(fc_in, parameters[-2]), parameters[-1]))

  y_pred = fc_layer3 / tf.reduce_sum(fc_layer3,
                          reduction_indices=len(fc_layer3.get_shape()) - 1,
                          keep_dims=True)
  # manual computation of crossentropy
  y_pred = tf.clip_by_value(y_pred, tf.cast(1e-10, dtype=tf.float32),
                            tf.cast(1. - 1e-10, dtype=tf.float32))
  cross_entropy = tf.reduce_mean(-tf.reduce_sum(y_true * tf.log(y_pred),
                                  reduction_indices=len(y_pred.get_shape()) - 1))
  opt = tf.train.MomentumOptimizer(learning_rate=0.01, momentum=0.9) # Any other optimizier can be placed here
  correct_pred = tf.equal(tf.argmax(y_pred, 1), tf.argmax(y_true, 1))
  accuracy = tf.reduce_mean(tf.cast(correct_pred, tf.float32))

  comp_grads = opt.compute_gradients(cross_entropy, parameters)

  application = opt.apply_gradients(zip(placeholders,parameters))
  sess = tf.Session()
  init_all_variables = tf.initialize_all_variables()
  return comp_grads, sess, application, accuracy, images, y_true, dropout, placeholders, parameters, assignment, init_all_variables


def net_reinitialization(net_vars):
  return net_vars

@ray.remote
def num_images(batches):
  """Counts number of images in batches.

  Args:
    batches (List): Collection of batches of images and labels.

  Returns:
    int: The number of images
  """
  shape_ids = [ra.shape.remote(batch) for batch in batches]
  return sum([shape[0] for shape in ray.get(shape_ids)])

@ray.remote
def compute_mean_image(batches):
  """Computes the mean image given a list of batches of images.

  Args:
    batches (List[ObjectID]): A list of batches of images.

  Returns:
    ndarray: The mean image
  """
  if len(batches) == 0:
    raise Exception("No images were passed into `compute_mean_image`.")
  sum_image_ids = [ra.sum.remote(batch, axis=0) for batch in batches]
  n_images = num_images.remote(batches)
  return np.sum(ray.get(sum_image_ids), axis=0).astype("float64") / ray.get(n_images)

@ray.remote(num_return_vals=4)
def shuffle_arrays(first_images, first_labels, second_images, second_labels):
  """Shuffles the images and labels from two batches.

  Args:
    first_images (ndarray): First batch of images.
    first_labels (ndarray): First batch of labels.
    second_images (ndarray): Second batch of images.
    second_labels (ndarray): Second batch of labels.

  Returns:
    ndarray: First batch of shuffled images.
    ndarray: First batch of shuffled labels.
    ndarray: Second bach of shuffled images.
    ndarray: Second batch of shuffled labels.
  """
  images = np.concatenate((first_images, second_images))
  labels = np.concatenate((first_labels, second_labels))
  total_length = len(images)
  first_len = len(first_images)
  random_indices = np.random.permutation(total_length)
  new_first_images = images[random_indices[0:first_len]]
  new_first_labels = labels[random_indices[0:first_len]]
  new_second_images = images[random_indices[first_len:total_length]]
  new_second_labels = labels[random_indices[first_len:total_length]]
  return new_first_images, new_first_labels, new_second_images, new_second_labels

def shuffle_pair(first_batch, second_batch):
  """Shuffle two batches of data.

  Args:
    first_batch (Tuple[ObjectID. ObjectID]): The first batch to be shuffled. The
      first component is the object ID of a batch of images, and the second
      component is the object ID of the corresponding batch of labels.
    second_batch (Tuple[ObjectID, ObjectID]): The second batch to be shuffled.
      The first component is the object ID of a batch of images, and the second
      component is the object ID of the corresponding batch of labels.

  Returns:
    Tuple[ObjectID, ObjectID]: The first batch of shuffled data.
    Tuple[ObjectID, ObjectID]: Two second bach of shuffled data.
  """
  images1, labels1, images2, labels2 = shuffle_arrays.remote(first_batch[0], first_batch[1], second_batch[0], second_batch[1])
  return (images1, labels1), (images2, labels2)

@ray.remote
def filenames_to_labels(filenames, filename_label_dict):
  """Converts filename strings to integer labels.

  Args:
    filenames (List[str]): The filenames of the images.
    filename_label_dict (Dict[str, int]): A dictionary mapping filenames to
      integer labels.

  Returns:
    ndarray: Integer labels
  """
  return np.asarray([int(filename_label_dict[filename]) for filename in filenames])

def one_hot(x):
  """Converts integer labels to one hot vectors.

  Args:
    x (int): Index to be set to one

  Returns:
    ndarray: One hot vector.
  """
  zero = np.zeros([1000])
  zero[x] = 1.0
  return zero

def crop_images(images):
  """Randomly crop a batch of images.

  This is used to generate many slightly different images from each training
  example.

  Args:
    images (ndarray): A batch of images to crop. The shape of images should be
      batch_size x height x width x channels.

  Returns:
    ndarray: A batch of cropped images.
  """
  original_height = 256
  original_width = 256
  cropped_height = 224
  cropped_width = 224
  height_offset = np.random.randint(original_height - cropped_height + 1)
  width_offset = np.random.randint(original_width - cropped_width + 1)
  return images[:, height_offset:(height_offset + cropped_height), width_offset:(width_offset + cropped_width), :]

def shuffle(batches):
  """Shuffle the data.

  This method groups the batches together in pairs and within each pair shuffles
  the data between the two members.

  Args:
    batches (List[Tuple[ObjectID, ObjectID]]): This is a list of tuples, where
      each tuple consists of two object IDs. The first component is an object ID
      for a batch of images, and the second component is an object ID for the
      corresponding batch of labels.

  Returns:
    List[Tuple[ObjectID, ObjectID]]: The shuffled data.
  """
  # Randomly permute the order of the batches.
  permuted_batches = np.random.permutation(batches)
  new_batches = []
  for i in range(len(batches) / 2):
    # Swap data between consecutive batches.
    shuffled_batch1, shuffled_batch2 = shuffle_pair(permuted_batches[2 * i], permuted_batches[2 * i + 1])
    new_batches += [shuffled_batch1, shuffled_batch2]
  if len(batches) % 2 == 1:
    # If there is an odd number of batches, don't forget the last one.
    new_batches.append(permuted_batches[-1])
  return new_batches

@ray.remote
def compute_grad(X, Y, mean, weights):
  """Computes the gradient of the network.

  Args:
    X (ndarray): Numpy array of images in the form of [224, 224,3]
    Y (ndarray): Labels corresponding to each image
    mean (ndarray): Mean image to subtract from images
    weights (List[ndarray]): The network weights.

  Returns:
    List of gradients for each variable
  """
  comp_grads, sess, _, _, images, y_true, dropout, placeholders, _, assignment, _ = ray.reusables.net_vars
  # Set the network weights.
  feed_dict = dict(zip(placeholders, weights))
  sess.run(assignment, feed_dict=feed_dict)
  # Choose a subset of the batch to compute on and crop the images.
  random_indices = np.random.randint(0, len(X), size=128)
  subset_X = crop_images(X[random_indices] - mean)
  subset_Y = np.asarray([one_hot(label) for label in Y[random_indices]])

  # Compute the gradients.
  return sess.run([g for (g, v) in comp_grads], feed_dict={images: subset_X, y_true: subset_Y, dropout: 0.5})

@ray.remote
def compute_accuracy(X, Y, weights):
  """Returns the accuracy of the network

  Args:
    X (ndarray): A batch of images.
    Y (ndarray): A batch of labels.
    weights (List[ndarray]): The network weights.

  Returns:
    The accuracy of the network on the given batch.
  """
  _, sess, _, accuracy, images, y_true, dropout, placeholders, _, assignment, _ = ray.reusables.net_vars
  # Set the network weights.
  feed_dict = dict(zip(placeholders, weights))
  sess.run(assignment, feed_dict=feed_dict)

  one_hot_Y = np.asarray([one_hot(label) for label in Y])
  cropped_X = crop_images(X)
  return sess.run(accuracy, feed_dict={images: cropped_X, y_true: one_hot_Y, dropout: 1.0})
