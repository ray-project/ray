# ----------------------------------------------------------------------------
# pyglet
# Copyright (c) 2006-2008 Alex Holkner
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions 
# are met:
#
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above copyright 
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#  * Neither the name of pyglet nor the names of its
#    contributors may be used to endorse or promote products
#    derived from this software without specific prior written
#    permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
# ----------------------------------------------------------------------------
from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
from builtins import object

import atexit
import threading
import time

import pyglet
from pyglet.debug import debug_print

_debug = debug_print('debug_media')


class MediaThread(object):
    """A thread that cleanly exits on interpreter shutdown, and provides
    a sleep method that can be interrupted and a termination method.

    :Ivariables:
        `condition` : threading.Condition
            Lock condition on all instance variables. 
        `stopped` : bool
            True if `stop` has been called.

    """
    _threads = set()
    _threads_lock = threading.Lock()

    def __init__(self, target=None):
        self._thread = threading.Thread(target=self._thread_run)
        self._thread.setDaemon(True)

        if target is not None:
            self.run = target

        self.condition = threading.Condition()
        self.stopped = False

    @classmethod
    def _atexit(cls):
        with cls._threads_lock:
            threads = list(cls._threads)
        for thread in threads:
            thread.stop()

    def run(self):
        pass

    def _thread_run(self):
        if pyglet.options['debug_trace']:
            pyglet._install_trace()

        with self._threads_lock:
            self._threads.add(self)
        self.run()
        with self._threads_lock:
            self._threads.remove(self)

    def start(self):
        self._thread.start()

    def stop(self):
        """Stop the thread and wait for it to terminate.

        The `stop` instance variable is set to ``True`` and the condition is
        notified.  It is the responsibility of the `run` method to check
        the value of `stop` after each sleep or wait and to return if set.
        """
        assert _debug('MediaThread.stop()')
        with self.condition:
            self.stopped = True
            self.condition.notify()
        self._thread.join()

    def sleep(self, timeout):
        """Wait for some amount of time, or until notified.

        :Parameters:
            `timeout` : float
                Time to wait, in seconds.

        """
        assert _debug('MediaThread.sleep(%r)' % timeout)
        with self.condition:
            if not self.stopped:
                self.condition.wait(timeout)

    def notify(self):
        """Interrupt the current sleep operation.

        If the thread is currently sleeping, it will be woken immediately,
        instead of waiting the full duration of the timeout.
        """
        assert _debug('MediaThread.notify()')
        with self.condition:
            self.condition.notify()

atexit.register(MediaThread._atexit)


class WorkerThread(MediaThread):
    def __init__(self, target=None):
        super(WorkerThread, self).__init__(target)
        self._jobs = []

    def run(self):
        while True:
            job = self.get_job()
            if not job:
                break
            job()

    def get_job(self):
        with self.condition:
            while self._empty() and not self.stopped:
                self.condition.wait()
            if self.stopped:
                result = None
            else:
                result = self._get()
        return result

    def put_job(self, job):
        with self.condition:
            self._put(job)
            self.condition.notify()

    def clear_jobs(self):
        with self.condition:
            self._clear()
            self.condition.notify()

    def _empty(self):
        return not self._jobs

    def _get(self):
        return self._jobs.pop(0)

    def _put(self, job):
        self._jobs.append(job)

    def _clear(self):
        del self._jobs[:]


class PlayerWorker(MediaThread):
    """
    Worker thread for refilling players.
    """
    # Time to wait if there are players, but they're all full.
    _nap_time = 0.05

    # Time to wait if there are no players.
    _sleep_time = None

    def __init__(self):
        super(PlayerWorker, self).__init__()
        self.players = set()

    def run(self):
        while True:
            # This is a big lock, but ensures a player is not deleted while
            # we're processing it -- this saves on extra checks in the
            # player's methods that would otherwise have to check that it's
            # still alive.
            with self.condition:
                assert _debug('PlayerWorker: woke up@{}'.format(time.time()))
                if self.stopped:
                    break
                sleep_time = -1

                if self.players:
                    filled = False
                    for player in self.players:
                        write_size = player.get_write_size()
                        if write_size > player.min_buffer_size:
                            player.refill(write_size)
                            filled = True
                    if not filled:
                        sleep_time = self._nap_time
                else:
                    assert _debug('PlayerWorker: No active players')
                    sleep_time = self._sleep_time

                if sleep_time != -1:
                    self.sleep(sleep_time)
                else:
                    # We MUST sleep, or we will starve pyglet's main loop.  It
                    # also looks like if we don't sleep enough, we'll starve out
                    # various updates that stop us from properly removing players
                    # that should be removed.
                    self.sleep(self._nap_time)

    def add(self, player):
        assert player is not None
        assert _debug('PlayerWorker: player added')
        with self.condition:
            self.players.add(player)
            self.condition.notify()

    def remove(self, player):
        assert _debug('PlayerWorker: player removed')
        with self.condition:
            if player in self.players:
                self.players.remove(player)
            self.condition.notify()


