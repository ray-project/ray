"""
Utils for converting a TFRecord -> decoded image buffer for training pipeline.
Adapted from MLPerf reference implementation.
https://github.com/mlcommons/training/blob/master/image_classification/tensorflow2/imagenet_preprocessing.py
"""
import tensorflow as tf
import functools
import logging

DEFAULT_IMAGE_SIZE = 224
NUM_CHANNELS = 3

_R_MEAN = 123.68
_G_MEAN = 116.78
_B_MEAN = 103.94
CHANNEL_MEANS = [_R_MEAN, _G_MEAN, _B_MEAN]

# The lower bound for the smallest side of the image for aspect-preserving
# resizing. For example, if an image is 500 x 1000, it will be resized to
# _RESIZE_MIN x (_RESIZE_MIN * 2).
_RESIZE_MIN = 256

# TODO(swang): Set num_classes from main script?
NUM_CLASSES = 1000


def process_record_dataset(
    dataset,
    is_training,
    batch_size,
    num_epochs,
    online_processing,
    shuffle_buffer=None,
    dtype=tf.float32,
    datasets_num_private_threads=None,
    drop_remainder=False,
    tf_data_experimental_slack=False,
    prefetch_batchs=tf.data.experimental.AUTOTUNE,
):
    """Given a Dataset with raw records, return an iterator over the records.

    Args:
      dataset: A Dataset representing raw records
      is_training: A boolean denoting whether the input is for training.
      batch_size: The number of samples per batch.
      shuffle_buffer: The buffer size to use when shuffling records. A larger
        value results in better randomness, but smaller values reduce startup
        time and use less memory.
      dtype: Data type to use for images/features.
      datasets_num_private_threads: Number of threads for a private
        threadpool created for all datasets computation.
      drop_remainder: A boolean indicates whether to drop the remainder of the
        batches. If True, the batch dimension will be static.
      tf_data_experimental_slack: Whether to enable tf.data's
        `experimental_slack` option.
      prefetch_batchs: The number of batchs to prefetch.

    Returns:
      Dataset of (image, label) pairs ready for iteration.
    """
    # Defines a specific size thread pool for tf.data operations.
    if datasets_num_private_threads:
        options = tf.data.Options()
        options.experimental_threading.private_threadpool_size = (
            datasets_num_private_threads
        )
        dataset = dataset.with_options(options)
        logging.info("datasets_num_private_threads: %s", datasets_num_private_threads)

    if is_training:
        # Shuffles records before repeating to respect epoch boundaries.
        if shuffle_buffer is not None:
            dataset = dataset.shuffle(buffer_size=shuffle_buffer)
        # Repeats the dataset for the number of epochs to train.
        dataset = dataset.repeat(num_epochs)

    one_hot = False
    # TODO(swang): Support one-hot encoding?
    # num_classes = FLAGS.num_classes
    # if FLAGS.label_smoothing and FLAGS.label_smoothing > 0:
    #  one_hot = True

    num_classes = NUM_CLASSES

    if online_processing:
        map_fn = functools.partial(
            preprocess_parsed_example,
            is_training=is_training,
            dtype=dtype,
            num_classes=num_classes,
            one_hot=one_hot,
        )

        # Parses the raw records into images and labels.
        dataset = dataset.map(map_fn, num_parallel_calls=tf.data.experimental.AUTOTUNE)
    dataset = dataset.batch(batch_size, drop_remainder=drop_remainder)

    # Operations between the final prefetch and the get_next call to the iterator
    # will happen synchronously during run time. We prefetch here again to
    # background all of the above processing work and keep it out of the
    # critical training path. Setting buffer_size to tf.data.experimental.AUTOTUNE
    # allows DistributionStrategies to adjust how many batches to fetch based
    # on how many devices are present.
    dataset = dataset.prefetch(buffer_size=prefetch_batchs)

    options = tf.data.Options()
    options.experimental_slack = tf_data_experimental_slack
    dataset = dataset.with_options(options)

    return dataset


def _parse_example_proto(example_serialized):
    """Parses an Example proto containing a training example of an image.

    The output of the build_image_data.py image preprocessing script is a dataset
    containing serialized Example protocol buffers. Each Example proto contains
    the following fields (values are included as examples):

      image/height: 462
      image/width: 581
      image/colorspace: 'RGB'
      image/channels: 3
      image/class/label: 615
      image/class/synset: 'n03623198'
      image/class/text: 'knee pad'
      image/object/bbox/xmin: 0.1
      image/object/bbox/xmax: 0.9
      image/object/bbox/ymin: 0.2
      image/object/bbox/ymax: 0.6
      image/object/bbox/label: 615
      image/format: 'JPEG'
      image/filename: 'ILSVRC2012_val_00041207.JPEG'
      image/encoded: <JPEG encoded string>

    Args:
      example_serialized: scalar Tensor tf.string containing a serialized
        Example protocol buffer.

    Returns:
      image_buffer: Tensor tf.string containing the contents of a JPEG file.
      label: Tensor tf.int32 containing the label.
      bbox: 3-D float Tensor of bounding boxes arranged [1, num_boxes, coords]
        where each coordinate is [0, 1) and the coordinates are arranged as
        [ymin, xmin, ymax, xmax].
    """
    # Dense features in Example proto.
    feature_map = {
        "image/encoded": tf.io.FixedLenFeature([], dtype=tf.string, default_value=""),
        "image/class/label": tf.io.FixedLenFeature(
            [], dtype=tf.int64, default_value=-1
        ),
        "image/class/text": tf.io.FixedLenFeature(
            [], dtype=tf.string, default_value=""
        ),
    }
    # NOTE(swang): bbox from dataset is not actually used by the reference
    # implementation.
    # https://github.com/mlcommons/training/pull/170
    # sparse_float32 = tf.VarLenFeature(dtype=tf.float32)
    # # Sparse features in Example proto.
    # feature_map.update(
    #     {k: sparse_float32 for k in ['image/object/bbox/xmin',
    #                                  'image/object/bbox/ymin',
    #                                  'image/object/bbox/xmax',
    #                                  'image/object/bbox/ymax']})

    features = tf.io.parse_single_example(example_serialized, feature_map)
    label = tf.cast(features["image/class/label"], dtype=tf.int32)

    return features["image/encoded"], label


def parse_example_proto_and_decode(example_serialized):
    """Parses an example and decodes the image to prepare for caching."""
    image_buffer, label = _parse_example_proto(example_serialized)
    image_buffer = tf.reshape(image_buffer, shape=[])
    image_buffer = tf.io.decode_jpeg(image_buffer, channels=NUM_CHANNELS)
    return image_buffer, label


def preprocess_parsed_example(
    image_buffer, label, is_training, dtype, num_classes, one_hot=False
):
    """Applies preprocessing steps to the input parsed example."""
    image = preprocess_image(
        image_buffer=image_buffer,
        output_height=DEFAULT_IMAGE_SIZE,
        output_width=DEFAULT_IMAGE_SIZE,
        num_channels=NUM_CHANNELS,
        is_training=is_training,
    )
    image = tf.cast(image, dtype)

    # Subtract one so that labels are in [0, 1000), and cast to float32 for
    # Keras model.
    label = tf.reshape(label, shape=[1])
    label = tf.cast(label, tf.int32)
    label -= 1

    if one_hot:
        label = tf.one_hot(label, num_classes)
        label = tf.reshape(label, [num_classes])
    else:
        label = tf.cast(label, tf.float32)

    return image, label


def preprocess_example(
    example_serialized, is_training, dtype, num_classes, one_hot=False
):
    """Applies preprocessing steps to the input parsed example."""
    image, label = parse_example_proto_and_decode(example_serialized)
    image = preprocess_image(
        image_buffer=image,
        output_height=DEFAULT_IMAGE_SIZE,
        output_width=DEFAULT_IMAGE_SIZE,
        num_channels=NUM_CHANNELS,
        is_training=is_training,
    )
    image = tf.cast(image, dtype)

    # Subtract one so that labels are in [0, 1000), and cast to float32 for
    # Keras model.
    label = tf.reshape(label, shape=[1])
    label = tf.cast(label, tf.int32)
    label -= 1

    if one_hot:
        label = tf.one_hot(label, num_classes)
        label = tf.reshape(label, [num_classes])
    else:
        label = tf.cast(label, tf.float32)

    return image, label


def build_tf_dataset(
    filenames,
    batch_size,
    num_images_per_epoch,
    num_epochs,
    online_processing,
    shuffle_buffer=None,
    is_training=True,
    dtype=tf.float32,
    datasets_num_private_threads=None,
    input_context=None,
    drop_remainder=False,
    tf_data_experimental_slack=False,
    # NOTE(swang): MLPerf sets this to False by default, but we should
    # cache decoded images for parity with AIR bulk ingest.
    dataset_cache=True,
    prefetch_batchs=tf.data.experimental.AUTOTUNE,
):
    """Input function which provides batches for train or eval.

    Args:
      is_training: A boolean denoting whether the input is for training.
      data_dir: The directory containing the input data.
      batch_size: The number of samples per batch.
      dtype: Data type to use for images/features
      datasets_num_private_threads: Number of private threads for tf.data.
      input_context: A `tf.distribute.InputContext` object passed in by
        `tf.distribute.Strategy`.
      drop_remainder: A boolean indicates whether to drop the remainder of the
        batches. If True, the batch dimension will be static.
      tf_data_experimental_slack: Whether to enable tf.data's
        `experimental_slack` option.
      dataset_cache: Whether to cache the dataset on workers.
         Typically used to improve training performance when training data is in
         remote storage and can fit into worker memory.
      filenames: Optional field for providing the file names of the TFRecords.
      prefetch_batchs: The number of batchs to prefetch.

    Returns:
      A dataset that can be used for iteration.
    """
    dataset = tf.data.Dataset.from_tensor_slices(filenames)

    if input_context:
        # TODO(swang): Shard and set shard index based on TF session.
        logging.info(
            "Sharding the dataset: input_pipeline_id=%d num_input_pipelines=%d",
            input_context.input_pipeline_id,
            input_context.num_input_pipelines,
        )
        dataset = dataset.shard(
            input_context.num_input_pipelines, input_context.input_pipeline_id
        )

    if is_training:
        # Shuffle the input files
        dataset = dataset.shuffle(buffer_size=len(filenames))

    # Convert to individual records.
    # cycle_length = 10 means that up to 10 files will be read and deserialized in
    # parallel. You may want to increase this number if you have a large number of
    # CPU cores.
    dataset = dataset.interleave(
        tf.data.TFRecordDataset,
        cycle_length=10,
        num_parallel_calls=tf.data.experimental.AUTOTUNE,
    )

    if is_training:
        if online_processing:
            dataset = dataset.map(
                parse_example_proto_and_decode,
                num_parallel_calls=tf.data.experimental.AUTOTUNE,
            )
        else:
            # We're just applying random transforms a single time and loading these
            # all into memory. NOTE(swang): this doesn't follow the reference
            # implementation and should only be used for debugging purposes.
            map_fn = functools.partial(
                preprocess_example,
                is_training=is_training,
                dtype=dtype,
                num_classes=NUM_CLASSES,
            )

            dataset = dataset.map(
                map_fn, num_parallel_calls=tf.data.experimental.AUTOTUNE
            )
    dataset = dataset.take(num_images_per_epoch)
    if dataset_cache:
        # Improve training / eval performance when data is in remote storage and
        # can fit into worker memory.
        dataset = dataset.cache()

    return process_record_dataset(
        dataset=dataset,
        is_training=is_training,
        batch_size=batch_size,
        num_epochs=num_epochs,
        online_processing=online_processing,
        shuffle_buffer=shuffle_buffer,
        dtype=dtype,
        datasets_num_private_threads=datasets_num_private_threads,
        drop_remainder=drop_remainder,
        tf_data_experimental_slack=tf_data_experimental_slack,
        prefetch_batchs=prefetch_batchs,
    )


def _decode_crop_and_flip(image_buffer, num_channels):
    """Crops the given image to a random part of the image, and randomly flips.

    We use the fused decode_and_crop op, which performs better than the two ops
    used separately in series, but note that this requires that the image be
    passed in as an un-decoded string Tensor.

    Args:
        image_buffer: scalar string Tensor representing the raw JPEG image buffer.
        bbox: 3-D float Tensor of bounding boxes arranged [1, num_boxes, coords]
            where each coordinate is [0, 1) and the coordinates are arranged as
            [ymin, xmin, ymax, xmax].
        num_channels: Integer depth of the image buffer for decoding.

    Returns:
        3-D tensor with cropped image.

    """
    # A large fraction of image datasets contain a human-annotated bounding box
    # delineating the region of the image containing the object of interest.    We
    # choose to create a new bounding box for the object which is a randomly
    # distorted version of the human-annotated bounding box that obeys an
    # allowed range of aspect ratios, sizes and overlap with the human-annotated
    # bounding box. If no box is supplied, then we assume the bounding box is
    # the entire image.
    decoded = not isinstance(image_buffer, bytes)
    shape = (
        tf.shape(image_buffer) if decoded else tf.image.extract_jpeg_shape(image_buffer)
    )
    bbox = tf.constant(
        [0.0, 0.0, 1.0, 1.0], dtype=tf.float32, shape=[1, 1, 4]
    )  # From the entire image
    sample_distorted_bounding_box = tf.image.sample_distorted_bounding_box(
        shape,
        bounding_boxes=bbox,
        min_object_covered=0.1,
        aspect_ratio_range=[0.75, 1.33],
        area_range=[0.05, 1.0],
        max_attempts=100,
        use_image_if_no_bounding_boxes=True,
    )
    bbox_begin, bbox_size, _ = sample_distorted_bounding_box

    # Reassemble the bounding box in the format the crop op requires.
    offset_y, offset_x, _ = tf.unstack(bbox_begin)
    target_height, target_width, _ = tf.unstack(bbox_size)
    crop_window = tf.stack([offset_y, offset_x, target_height, target_width])

    if decoded:
        image_buffer = tf.image.crop_to_bounding_box(
            image_buffer,
            offset_height=offset_y,
            offset_width=offset_x,
            target_height=target_height,
            target_width=target_width,
        )
    else:
        # Use the fused decode and crop op here, which is faster than sequential.
        image_buffer = tf.image.decode_and_crop_jpeg(
            image_buffer, crop_window, channels=num_channels
        )
    # Flip to add a little more random distortion in.
    image_buffer = tf.image.random_flip_left_right(image_buffer)
    return image_buffer


def _central_crop(image, crop_height, crop_width):
    """Performs central crops of the given image list.

    Args:
        image: a 3-D image tensor
        crop_height: the height of the image following the crop.
        crop_width: the width of the image following the crop.

    Returns:
        3-D tensor with cropped image.
    """
    shape = tf.shape(input=image)
    height, width = shape[0], shape[1]

    amount_to_be_cropped_h = height - crop_height
    crop_top = amount_to_be_cropped_h // 2
    amount_to_be_cropped_w = width - crop_width
    crop_left = amount_to_be_cropped_w // 2
    return tf.slice(image, [crop_top, crop_left, 0], [crop_height, crop_width, -1])


def _mean_image_subtraction(image, means, num_channels):
    """Subtracts the given means from each image channel.

    For example:
        means = [123.68, 116.779, 103.939]
        image = _mean_image_subtraction(image, means)

    Note that the rank of `image` must be known.

    Args:
        image: a tensor of size [height, width, C].
        means: a C-vector of values to subtract from each channel.
        num_channels: number of color channels in the image that will be distorted.

    Returns:
        the centered image.

    Raises:
        ValueError: If the rank of `image` is unknown, if `image` has a rank other
            than three or if the number of channels in `image` doesn't match the
            number of values in `means`.
    """
    if image.get_shape().ndims != 3:
        raise ValueError("Input must be of size [height, width, C>0]")

    if len(means) != num_channels:
        raise ValueError("len(means) must match the number of channels")

    # We have a 1-D tensor of means; convert to 3-D.
    # Note(b/130245863): we explicitly call `broadcast` instead of simply
    # expanding dimensions for better performance.
    means = tf.broadcast_to(means, tf.shape(image))

    return image - means


def _smallest_size_at_least(height, width, resize_min):
    """Computes new shape with the smallest side equal to `smallest_side`.

    Computes new shape with the smallest side equal to `smallest_side` while
    preserving the original aspect ratio.

    Args:
        height: an int32 scalar tensor indicating the current height.
        width: an int32 scalar tensor indicating the current width.
        resize_min: A python integer or scalar `Tensor` indicating the size of
            the smallest side after resize.

    Returns:
        new_height: an int32 scalar tensor indicating the new height.
        new_width: an int32 scalar tensor indicating the new width.
    """
    resize_min = tf.cast(resize_min, tf.float32)

    # Convert to floats to make subsequent calculations go smoothly.
    height, width = tf.cast(height, tf.float32), tf.cast(width, tf.float32)

    smaller_dim = tf.minimum(height, width)
    scale_ratio = resize_min / smaller_dim

    # Convert back to ints to make heights and widths that TF ops will accept.
    new_height = tf.cast(height * scale_ratio, tf.int32)
    new_width = tf.cast(width * scale_ratio, tf.int32)

    return new_height, new_width


def _aspect_preserving_resize(image, resize_min):
    """Resize images preserving the original aspect ratio.

    Args:
        image: A 3-D image `Tensor`.
        resize_min: A python integer or scalar `Tensor` indicating the size of
            the smallest side after resize.

    Returns:
        resized_image: A 3-D tensor containing the resized image.
    """
    shape = tf.shape(input=image)
    height, width = shape[0], shape[1]

    new_height, new_width = _smallest_size_at_least(height, width, resize_min)

    return _resize_image(image, new_height, new_width)


def _resize_image(image, height, width):
    """Simple wrapper around tf.resize_images.

    This is primarily to make sure we use the same `ResizeMethod` and other
    details each time.

    Args:
        image: A 3-D image `Tensor`.
        height: The target height for the resized image.
        width: The target width for the resized image.

    Returns:
        resized_image: A 3-D tensor containing the resized image. The first two
            dimensions have the shape [height, width].
    """
    return tf.compat.v1.image.resize(
        image,
        [height, width],
        method=tf.image.ResizeMethod.BILINEAR,
        align_corners=False,
    )


def preprocess_image(
    image_buffer, output_height, output_width, num_channels, is_training=False
):
    """Preprocesses the given image.

    Preprocessing includes decoding, cropping, and resizing for both training
    and eval images. Training preprocessing, however, introduces some random
    distortion of the image to improve accuracy.

    Args:
        image_buffer: scalar string Tensor representing the raw JPEG image buffer.
        bbox: 3-D float Tensor of bounding boxes arranged [1, num_boxes, coords]
            where each coordinate is [0, 1) and the coordinates are arranged as
            [ymin, xmin, ymax, xmax].
        output_height: The height of the image after preprocessing.
        output_width: The width of the image after preprocessing.
        num_channels: Integer depth of the image buffer for decoding.
        is_training: `True` if we're preprocessing the image for training and
            `False` otherwise.

    Returns:
        A preprocessed image.
    """
    if is_training:
        # For training, we want to randomize some of the distortions.
        image = _decode_crop_and_flip(image_buffer, num_channels)
        image = _resize_image(image, output_height, output_width)
    else:
        # For validation, we want to decode, resize, then just crop the middle.
        if isinstance(image_buffer, bytes):
            image = tf.image.decode_jpeg(image_buffer, channels=num_channels)
        else:
            image = image_buffer
        image = _aspect_preserving_resize(image, _RESIZE_MIN)
        image = _central_crop(image, output_height, output_width)

    image.set_shape([output_height, output_width, num_channels])

    return _mean_image_subtraction(image, CHANNEL_MEANS, num_channels)
