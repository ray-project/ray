"""ResNet training script, with some code from
https://github.com/tensorflow/models/tree/master/resnet.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import numpy as np
import ray
import tensorflow as tf

import cifar_input
import resnet_model

FLAGS = tf.app.flags.FLAGS
tf.app.flags.DEFINE_string('dataset', 'cifar10', 'cifar10 or cifar100.')
tf.app.flags.DEFINE_string('train_data_path', '',
                           'Filepattern for training data.')
tf.app.flags.DEFINE_string('eval_data_path', '',
                           'Filepattern for eval data')
tf.app.flags.DEFINE_string('eval_dir', '',
                           'Directory to keep eval outputs.')
tf.app.flags.DEFINE_integer('eval_batch_count', 50,
                            'Number of batches to eval.')
tf.app.flags.DEFINE_integer('num_gpus', 0,
                            'Number of gpus used for training.')
use_gpu = 1 if int(FLAGS.num_gpus) > 0 else 0

@ray.remote(num_return_vals=4)
def get_data(path, size, dataset):
 os.environ['CUDA_VISIBLE_DEVICES'] = ''
 with tf.device('/cpu:0'):
  queue = cifar_input.build_data(path, size, dataset)
  sess = tf.Session()
  coord = tf.train.Coordinator()
  tf.train.start_queue_runners(sess, coord=coord)
  images, labels = sess.run(queue)
  coord.request_stop()
  sess.close()
  return (images[:int(size / 3), :],
          images[int(size / 3):int(2 * size / 3), :],
          images[int(2 * size / 3):, :],
          labels)

@ray.remote(num_gpus=use_gpu)
class ResNetTrainActor(object):
  def __init__(self, data, dataset, num_gpus):
    if num_gpus > 0:
      os.environ['CUDA_VISIBLE_DEVICES'] = ','.join([str(i) for i in ray.get_gpu_ids()])
    hps = resnet_model.HParams(batch_size=128,
                               num_classes=10 if dataset == 'cifar10' else 100,
                               min_lrn_rate=0.0001,
                               lrn_rate=0.1,
                               num_residual_units=5,
                               use_bottleneck=False,
                               weight_decay_rate=0.0002,
                               relu_leakiness=0.1,
                               optimizer='mom',
                               num_gpus=num_gpus)
    data = ray.get(data)
    total_images = np.concatenate([data[0], data[1], data[2]])
    if num_gpus > 0:
      tf.set_random_seed(ray.get_gpu_ids()[0] + 1)
    else:
      tf.set_random_seed(1)
    with tf.device('/gpu:0' if num_gpus > 0 else '/cpu:0'):
      images, labels = cifar_input.build_input([total_images, data[3]], hps.batch_size, dataset, True)
      self.model = resnet_model.ResNet(hps, images, labels, 'train')
      self.model.build_graph()
      config = tf.ConfigProto(allow_soft_placement=True)
      sess = tf.Session(config=config)
      self.model.variables.set_session(sess)
      self.coord = tf.train.Coordinator()
      tf.train.start_queue_runners(sess, coord=self.coord)
      init = tf.global_variables_initializer()
      sess.run(init)

  def compute_steps(self, weights):
    # This method sets the weights in the network, runs some training steps,
    # and returns the new weights.
    steps = 10
    self.model.variables.set_weights(weights)
    for i in range(steps):
      self.model.variables.sess.run(self.model.train_op)
    return self.model.variables.get_weights()

  def get_weights(self):
    return self.model.variables.get_weights()

@ray.remote
class ResNetTestActor(object):
  def __init__(self, data, dataset, eval_batch_count, eval_dir):
    hps = resnet_model.HParams(batch_size=100,
                               num_classes=10 if dataset == 'cifar10' else 100,
                               min_lrn_rate=0.0001,
                               lrn_rate=0.1,
                               num_residual_units=5,
                               use_bottleneck=False,
                               weight_decay_rate=0.0002,
                               relu_leakiness=0.1,
                               optimizer='mom',
                               num_gpus=0)
    data = ray.get(data)
    total_images = np.concatenate([data[0], data[1], data[2]])
    with tf.device('/cpu:0'):
      images, labels = cifar_input.build_input([total_images, data[3]], hps.batch_size, dataset, False)
      self.model = resnet_model.ResNet(hps, images, labels, 'eval')
      self.model.build_graph()
      config = tf.ConfigProto(allow_soft_placement=True)
      sess = tf.Session(config=config)
      self.model.variables.set_session(sess)
      self.coord = tf.train.Coordinator()
      tf.train.start_queue_runners(sess, coord=self.coord)
      init = tf.global_variables_initializer()
      sess.run(init)
      self.best_precision = 0.0
      self.eval_batch_count = eval_batch_count
      self.summary_writer = tf.summary.FileWriter(eval_dir, sess.graph)
      self.summary_writer
    self.ip_addr = ray.services.get_node_ip_address()

  def accuracy(self, weights, train_step):
    self.model.variables.set_weights(weights)
    total_prediction, correct_prediction = 0, 0
    model = self.model
    sess = self.model.variables.sess
    for _ in range(self.eval_batch_count):
      summaries, loss, predictions, truth = sess.run(
          [model.summaries, model.cost, model.predictions,
           model.labels])

      truth = np.argmax(truth, axis=1)
      predictions = np.argmax(predictions, axis=1)
      correct_prediction += np.sum(truth == predictions)
      total_prediction += predictions.shape[0]

    precision = 1.0 * correct_prediction / total_prediction
    self.best_precision = max(precision, self.best_precision)
    precision_summ = tf.Summary()
    precision_summ.value.add(
        tag='Precision', simple_value=precision)
    self.summary_writer.add_summary(precision_summ, train_step)
    best_precision_summ = tf.Summary()
    best_precision_summ.value.add(
        tag='Best Precision', simple_value=self.best_precision)
    self.summary_writer.add_summary(best_precision_summ, train_step)
    self.summary_writer.add_summary(summaries, train_step)
    tf.logging.info('loss: %.3f, precision: %.3f, best precision: %.3f' %
                    (loss, precision, self.best_precision))
    self.summary_writer.flush()
    return precision

  def get_ip_addr(self):
    return self.ip_addr

def train():
  """Training loop."""
  num_gpus = int(FLAGS.num_gpus)
  ray.init(num_gpus=num_gpus, redirect_output=True)
  train_data = get_data.remote(FLAGS.train_data_path, 50000, FLAGS.dataset)
  test_data = get_data.remote(FLAGS.eval_data_path, 10000, FLAGS.dataset)
  if num_gpus > 0:
    train_actors = [ResNetTrainActor.remote(train_data, FLAGS.dataset, num_gpus) for _ in range(num_gpus)]
  else:
    train_actors = [ResNetTrainActor.remote(train_data, num_gpus, 0)]
  test_actor = ResNetTestActor.remote(test_data, FLAGS.dataset, FLAGS.eval_batch_count, FLAGS.eval_dir)
  print('The log files for tensorboard are stored at ip {}.'.format(ray.get(test_actor.get_ip_addr.remote())))
  step = 0
  weight_id = train_actors[0].get_weights.remote()
  acc_id = test_actor.accuracy.remote(weight_id, step)
  if num_gpus == 0:
    num_gpus = 1
  print("Starting computation.")
  while True:
    all_weights = ray.get([actor.compute_steps.remote(weight_id) for actor in train_actors])
    mean_weights = {k: sum([weights[k] for weights in all_weights]) / num_gpus for k in all_weights[0]}
    weight_id = ray.put(mean_weights)
    step += 10
    if step % 200 == 0:
      acc = ray.get(acc_id)
      acc_id = test_actor.accuracy.remote(weight_id, step)
      print('Step {0}: {1:.6f}'.format(step - 200, acc))

def main(_):
  train()

if __name__ == '__main__':
  tf.app.run()
