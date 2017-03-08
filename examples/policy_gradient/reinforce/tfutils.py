from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import threading

class DataQueue(object):

  def __init__(self, placeholder_dict):
    """Here, placeholder_dict is an OrderedDict."""
    placeholders = placeholder_dict.values()
    shapes = [placeholder.get_shape()[1:].as_list() for placeholder in placeholders]
    types = [placeholder.dtype for placeholder in placeholders]
    self.queue = tf.RandomShuffleQueue(shapes=shapes, dtypes=dtypes, capacity=2000, min_after_dequeue=1000)
    self.enqueue_op = self.queue.enqueue_many(placeholders)

  def thread_main(self, sess, data_iterator):
    for data in data_iterator:
      feed_dict = {placeholder: data[name] for (name, placeholder) in placeholder_dict}
      sess.run(self.enqueue_op, feed_dict=feed_dict)

  def start_thread(self, sess, data_iterator, num_threads=1):
    threads = []
    for n in range(num_thread):
      t = threading.Thread(target=self.train_main, args=(sess, data_iterator))
      t.daemon = True # Thread will close when parent quits
      t.start()
      threads.append(t)
    return threads
