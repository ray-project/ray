# Copyright 2016 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

"""ResNet Train/Eval module.
"""
import os
import cifar_input
import numpy as np
import resnet_model
import ray
import tensorflow as tf

FLAGS = tf.app.flags.FLAGS
tf.app.flags.DEFINE_string('train_data_path', '',
                           'Filepattern for training data.')
tf.app.flags.DEFINE_string('eval_data_path', '',
                           'Filepattern for eval data')
tf.app.flags.DEFINE_string('num_gpus', 1, 'Number of gpus to run with')

@ray.remote
def get_test(path, size):
 os.environ["CUDA_VISIBLE_DEVICES"] = ''
 with tf.device("/cpu:0"):
  images, labels = cifar_input.build_input(path, size)
  sess = tf.Session()
  coord = tf.train.Coordinator()
  tf.train.start_queue_runners(sess, coord=coord)
  batches = [sess.run([images, labels]) for _ in range(5)]
  coord.request_stop()
  sess.close()
  return (np.concatenate([batches[i][0] for i in range(5)]), np.concatenate([batches[i][1] for i in range(5)]))

@ray.remote(num_return_vals=25)
def get_batches(path, size):
 os.environ["CUDA_VISIBLE_DEVICES"] = ''
 with tf.device("/cpu:0"):
  images, labels = cifar_input.build_input(path, size)
  sess = tf.Session()
  coord = tf.train.Coordinator()
  tf.train.start_queue_runners(sess, coord=coord)
  batches = [sess.run([images, labels]) for _ in range(25)]
  coord.request_stop()
  sess.close()
  print("get_batches")
  return batches

@ray.actor(num_gpus=1)
class ResNetActor(object):
  def __init__(self):
    os.environ["CUDA_VISIBLE_DEVICES"] = ",".join([str(i) for i in ray.get_gpu_ids()])
    hps = resnet_model.HParams(batch_size=128,
                               num_classes=10,
                               min_lrn_rate=0.0001,
                               lrn_rate=0.1,
                               num_residual_units=5,
                               use_bottleneck=False,
                               weight_decay_rate=0.0002,
                               relu_leakiness=0.1,
                               optimizer='mom')
    with tf.Graph().as_default():
      with tf.device("/gpu:0"):
        self.model = resnet_model.ResNet(hps, 'train')
        self.model.build_graph()
        config = tf.ConfigProto(allow_soft_placement=True)
        sess = tf.Session(config=config)
        self.model.variables.set_session(sess)
        init = tf.global_variables_initializer()
        self.placeholders = [self.model.x, self.model.labels]
        sess.run(init)


  def accuracy(self, weights, batch):
    self.model.variables.set_weights(weights)
    batches = [(batch[0][128*i:128*(i+1)], batch[1][128*i:128*(i+1)]) for i in range(78)]
    return sum([self.model.variables.sess.run(self.model.precision, feed_dict=dict(zip(self.placeholders, batches[i]))) for i in range(78)]) / 78


  def compute_rollout(self, weights, batch):
    rollouts = 10
    self.model.variables.set_weights(weights)
    for i in range(rollouts):
      randlist = np.random.randint(0, batch[0].shape[0], 128)
      subset = (batch[0][randlist, :], batch[1][randlist, :])
      self.model.variables.sess.run(self.model.train_op, feed_dict=dict(zip(self.placeholders, subset))) 
    return self.model.variables.get_weights()  

  def get_weights(self):
    return self.model.variables.get_weights()

def train(hps):
  """Training loop."""
  gpus = int(FLAGS.num_gpus)
  ray.init(num_workers=2, num_gpus=gpus)
  batches = get_batches.remote(FLAGS.train_data_path, hps.batch_size)
  test_batch = get_test.remote(FLAGS.eval_data_path, hps.batch_size)
  actor_list = [ResNetActor() for i in range(gpus)]
  step = 0
  weight_id = actor_list[0].get_weights()
  while True:
    with open("results.txt", "a") as results:
      print("Start of loop")
      rand_list = np.random.choice(25, gpus)
      print("Computing rollouts")
      all_weights = ray.get([actor.compute_rollout(weight_id, batches[i])  for i, actor in zip(rand_list, actor_list)])
      mean_weights = {k: sum([weights[k] for weights in all_weights]) / gpus for k in all_weights[0]}
      weight_id = ray.put(mean_weights)
      if step % 200 == 0:
        acc = ray.get(actor_list[0].accuracy(weight_id, test_batch))
        print(acc)
        results.write(str(step) + " " + str(acc) + "\n")
      step += 10


def main(_):
  batch_size = 128
  num_classes = 10
  hps = resnet_model.HParams(batch_size=batch_size,
                             num_classes=num_classes,
                             min_lrn_rate=0.0001,
                             lrn_rate=0.1,
                             num_residual_units=5,
                             use_bottleneck=False,
                             weight_decay_rate=0.0002,
                             relu_leakiness=0.1,
                             optimizer='mom')
  train(hps)

if __name__ == '__main__':
  tf.app.run()
