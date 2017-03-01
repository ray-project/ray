"""ResNet Train module, with some code from tensorflow/models/resnet.
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
def get_test(path):
 os.environ["CUDA_VISIBLE_DEVICES"] = ''
 with tf.device("/cpu:0"):
  images, labels = cifar_input.build_input(path, 5000, False)
  sess = tf.Session()
  coord = tf.train.Coordinator()
  tf.train.start_queue_runners(sess, coord=coord)
  batches = [sess.run([images, labels]) for _ in range(2)]
  coord.request_stop()
  sess.close()
  return (np.concatenate([batches[i][0] for i in range(2)]), np.concatenate([batches[i][1] for i in range(2)]))

@ray.actor(num_gpus=1)
class ResNetActor(object):
  def __init__(self, path, gpus):
    os.environ["CUDA_VISIBLE_DEVICES"] = ",".join([str(i) for i in ray.get_gpu_ids()])
    hps = resnet_model.HParams(batch_size=128,
                               num_classes=10,
                               min_lrn_rate=0.0001,
                               lrn_rate=0.1,
                               num_residual_units=5,
                               use_bottleneck=False,
                               weight_decay_rate=0.0002,
                               relu_leakiness=0.1,
                               optimizer='mom',
                               gpus=gpus)
    with tf.Graph().as_default():
      tf.set_random_seed(ray.get_gpu_ids()[0] + 1)
      with tf.device("/gpu:0"):
        images, labels = cifar_input.build_input(path, hps.batch_size, True)       
        self.model = resnet_model.ResNet(images, labels, hps)
        self.model.build_graph()
        config = tf.ConfigProto(allow_soft_placement=True)
        sess = tf.Session(config=config)
        self.model.variables.set_session(sess)
        self.coord = tf.train.Coordinator()
        tf.train.start_queue_runners(sess, coord=self.coord)
        init = tf.global_variables_initializer()
        self.placeholders = [images, labels]
        sess.run(init)

  def global_step(self):
    return self.model.variables.sess.run(self.model.global_step)

  def lrn_rate(self):
    return self.model.variables.sess.run(self.model.lrn_rate)

  def accuracy(self, weights, batch):
    self.model.variables.set_weights(weights)
    batches = [(batch[0][128*i:128*(i+1)], batch[1][128*i:128*(i+1)]) for i in range(78)]
    return sum([self.model.variables.sess.run(self.model.precision, feed_dict=dict(zip(self.placeholders, batches[i]))) for i in range(78)]) / 78


  def compute_rollout(self, weights):
    rollouts = 10
    self.model.variables.set_weights(weights)
    for i in range(rollouts):
      self.model.variables.sess.run(self.model.train_op) 
    return self.model.variables.get_weights()  

  def get_weights(self):
    return self.model.variables.get_weights()

def train():
  """Training loop."""
  gpus = int(FLAGS.num_gpus)
  ray.init(num_workers=2, num_gpus=gpus)
  test_batch = get_test.remote(FLAGS.eval_data_path)
  actor_list = [ResNetActor(FLAGS.train_data_path, gpus) for _ in range(gpus)]
  step = 0
  weight_id = actor_list[0].get_weights()
  while True:
    with open("results.txt", "a") as results:
      print("Computing rollouts")
      all_weights = ray.get([actor.compute_rollout(weight_id) for actor in actor_list])
      mean_weights = {k: sum([weights[k] for weights in all_weights]) / gpus for k in all_weights[0]}
      weight_id = ray.put(mean_weights)
      if step % 200 == 0:
        acc = ray.get(actor_list[0].accuracy(weight_id, test_batch))
        print(acc)
        results.write(str(step) + " " + str(acc) + "\n")
      step += 10

def main(_):
  train()

if __name__ == '__main__':
  tf.app.run()
