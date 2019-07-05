# Copyright 2017 The TensorFlow Authors. All Rights Reserved.
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
"""Communication channel used by the TensorBoard Debugger Plugin."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import threading

from six.moves import queue


class CommChannel(object):
  """A class that handles the queueing of outgoing and incoming messages.

  CommChannel is a multi-consumer interface that serves the following purposes:

  1) Keeps track of all the messages that it has received from the caller of
     put_outgoing(). In the case of TDP, these are messages about the start of
     Session.runs() and the pausing events at tensor breakpoints. These messages
     are kept in the order they are received. These messages are organized in
     memory by a serial index starting from 1. Since the messages are maintained
     in the memory indefinitely, they ought to be small in size.
     # TODO(cais): If the need arises, persist the messages.
  2) Allows the callers of get_outgoing() to retrieve any message by a serial
     index (also referred to as "position") at anytime. Notice that we want to
     support multiple callers because more than once browser sessions may need
     to connect to the backend simultaneously. If a caller of get_outgoing()
     requests a serial that has not been received from put_going() yet, the
     get_ougoing() call will block until a message is received at that position.
  """

  def __init__(self):
    self._outgoing = []
    self._outgoing_counter = 0
    self._outgoing_lock = threading.Lock()
    self._outgoing_pending_queues = dict()

  def put(self, message):
    """Put a message into the outgoing message stack.

    Outgoing message will be stored indefinitely to support multi-users.
    """
    with self._outgoing_lock:
      self._outgoing.append(message)
      self._outgoing_counter += 1

      # Check to see if there are pending queues waiting for the item.
      if self._outgoing_counter in self._outgoing_pending_queues:
        for q in self._outgoing_pending_queues[self._outgoing_counter]:
          q.put(message)
        del self._outgoing_pending_queues[self._outgoing_counter]

  def get(self, pos):
    """Get message(s) from the outgoing message stack.

    Blocks until an item at stack position pos becomes available.
    This method is thread safe.

    Args:
       pos: An int specifying the top position of the message stack to access.
         For example, if the stack counter is at 3 and pos == 2, then the 2nd
         item on the stack will be returned, together with an int that indicates
         the current stack heigh (3 in this case).

    Returns:
      1. The item at stack position pos.
      2. The height of the stack when the retun values are generated.

    Raises:
      ValueError: If input `pos` is zero or negative.
    """
    if pos <= 0:
      raise ValueError('Invalid pos %d: pos must be > 0' % pos)
    with self._outgoing_lock:
      if self._outgoing_counter >= pos:
        # If the stack already has the requested position, return the value
        # immediately.
        return self._outgoing[pos - 1], self._outgoing_counter
      else:
        # If the stack has not reached the requested position yet, create a
        # queue and block on get().
        if pos not in self._outgoing_pending_queues:
          self._outgoing_pending_queues[pos] = []
        q = queue.Queue(maxsize=1)
        self._outgoing_pending_queues[pos].append(q)

    value = q.get()
    with self._outgoing_lock:
      return value, self._outgoing_counter
