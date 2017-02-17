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
import importlib

FLAGS = tf.app.flags.FLAGS
tf.app.flags.DEFINE_string('train_data_path', '',
                           'Filepattern for training data.')
tf.app.flags.DEFINE_string('eval_data_path', '',
                           'Filepattern for eval data')

parentpid = os.getpid()
print(parentpid)

@ray.remote
def get_test(path, size):
 with tf.device("/cpu:0"):
  images, labels = cifar_input.build_input(path, size)
  sess = tf.Session()
  coord = tf.train.Coordinator()
  tf.train.start_queue_runners(sess, coord=coord)
  batches = [sess.run([images, labels]) for _ in range(5)]
  coord.request_stop()
  return (np.concatenate([batches[i][0] for i in range(5)]), np.concatenate([batches[i][1] for i in range(5)]))

@ray.remote(num_return_vals=25)
def get_batches(path, size):
 with tf.device("/cpu:0"):
  images, labels = cifar_input.build_input(path, size)
  sess = tf.Session()
  coord = tf.train.Coordinator()
  tf.train.start_queue_runners(sess, coord=coord)
  batches = [sess.run([images, labels]) for _ in range(25)]
  coord.request_stop()
  print("get_btaches")
  return batches

@ray.remote
def compute_rollout(weights, batch):
  model, _ = ray.env.model
  rollouts = 10
  model.variables.set_weights(weights)
  placeholders = [model.x, model.labels]

  for i in range(rollouts):
    randlist = np.random.randint(0,batch[0].shape[0], 128)
    subset = (batch[0][randlist, :], batch[1][randlist, :])
    model.variables.sess.run(model.train_op, feed_dict=dict(zip(placeholders, subset))) 
  return model.variables.get_weights()  

@ray.remote
def accuracy(weights, batch):
  model, _ = ray.env.model
  model.variables.set_weights(weights)
  placeholders = [model.x, model.labels]
  batches = [(batch[0][128*i:128*(i+1)], batch[1][128*i:128*(i+1)]) for i in range(78)]
  return sum([model.variables.sess.run(model.precision, feed_dict=dict(zip(placeholders, batches[i]))) for i in range(78)]) / 78

def model_initialization():
  pid = os.getpid()
  os.environ["CUDA_VISIBLE_DEVICES"] = str(pid & 8) if pid != parentpid else ""
  print(pid % 8 if pid != parentpid else "No")
  with tf.Graph().as_default():
    with tf.device("/gpu:0" if pid != parentpid else "/cpu:0"):
      model = resnet_model.ResNet(hps, 'train')
      model.build_graph()
      config = tf.ConfigProto(allow_soft_placement=True)
      sess = tf.Session(config=config)
      model.variables.set_session(sess)
      init = tf.global_variables_initializer()
      sess.run(init)
      return model, init

def model_reinitialization(model):
  return model

def train(hps):
  """Training loop."""
  ray.init(num_workers=2)
  batches = get_batches.remote(FLAGS.train_data_path, hps.batch_size)
  test_batch = get_test.remote(FLAGS.eval_data_path, hps.batch_size)
  print(ray.get(batches))
  ray.env.model = ray.EnvironmentVariable(model_initialization, model_reinitialization)
  model, init = ray.env.model
  model.variables.sess.run(init)
  step = 0
  while True:
    with open("results.txt", "w") as results:
      print("Start of loop")
      weights = model.variables.get_weights()
      weight_id = ray.put(weights)
      rand_list = np.random.choice(25, 2, replace=False)
      print("Computing rollouts")
      all_weights = ray.get([compute_rollout.remote(weight_id, batches[i])  for i in rand_list])
      mean_weights = {k: sum([weights[k] for weights in all_weights]) / 2 for k in all_weights[0]}
      model.variables.set_weights(mean_weights)
      new_weights = ray.put(mean_weights)
      if step % 200 == 0:
        acc = ray.get(accuracy.remote(new_weights, test_batch))
        print(acc)
        results.write(str(step) + " " + str(acc) + "\n")
      step += 10


def main(_):
  batch_size = 128
  num_classes = 10
  global hps
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
