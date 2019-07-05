from __future__ import print_function
from __future__ import division
from builtins import object
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
import time

from pyglet.app import WeakSet
from pyglet.media.events import MediaEvent
from pyglet.media.drivers.base import AbstractAudioDriver, AbstractAudioPlayer
from pyglet.media.threads import MediaThread

import pyglet
_debug = pyglet.options['debug_media']


class SilentAudioPacket(object):
    def __init__(self, timestamp, duration):
        self.timestamp = timestamp
        self.duration = duration

    def consume(self, dt):
        """Try to consume `dt` seconds of audio data. Return number of seconds consumed."""
        if dt > self.duration:
            duration = self.duration
            self.timestamp += self.duration
            self.duration = 0.
            return duration
        else:
            self.timestamp += dt
            self.duration -= dt
            return dt

    def is_empty(self):
        return self.duration == 0


class SilentAudioBuffer(object):
    """Buffer for silent audio packets"""
    def __init__(self):
        self.clear()

    def clear(self):
        # Buffered audio packets
        self._packets = []

        # Duration of the buffered contents
        self.duration = 0.

    def add_audio_data(self, audio_data):
        assert audio_data is not None
        packet = SilentAudioPacket(audio_data.timestamp, audio_data.duration)
        self._add_packet(packet)

    def consume_audio_data(self, dt):
        assert dt >= 0.
        while dt > 0. and not self.is_empty():
            consumed = self._packets[0].consume(dt)
            self.duration -= consumed
            if self._packets[0].is_empty():
                self._next_packet()
            dt -= consumed

    def is_empty(self):
        return self.duration <= 0

    def get_current_timestamp(self):
        if self._packets:
            return self._packets[0].timestamp
        else:
            return 0.

    def get_time_to_next_update(self):
        if self._packets and self.duration > 0.:
            return self._packets[0].duration
        else:
            return None

    def _add_packet(self, packet):
        self.duration += packet.duration
        self._packets.append(packet)

    def _next_packet(self):
        if len(self._packets) > 1:
            self.duration -= self._packets[0].duration
            del self._packets[0]


class EventBuffer(object):
    """Buffer for events from audio data"""
    def __init__(self):
        self.clear()

    def clear(self):
        # Events in the order they are retrieved from audio_data
        self._events = []

    def add_events(self, audio_data):
        assert audio_data is not None
        for event in audio_data.events:
            assert event.timestamp <= audio_data.duration
            event.timestamp += audio_data.timestamp
            self._events.append(event)

    def get_next_event_timestamp(self):
        if self._events:
            return self._events[0].timestamp
        else:
            return None

    def get_time_to_next_event(self, timestamp):
        if self._events:
            dt = self._events[0].timestamp - timestamp
            if dt < 0.:
                return 0.
            else:
                return dt
        else:
            return None

    def get_expired_events(self, timestamp):
        expired_events = []
        while self._events and self._events[0].timestamp <= timestamp:
            expired_events.append(self._events[0])
            del self._events[0]
        return expired_events


class SilentAudioPlayerPacketConsumer(AbstractAudioPlayer):
    # When playing video, length of audio (in secs) to buffer ahead.
    _buffer_time = 0.4

    # Minimum number of bytes to request from source
    _min_update_bytes = 1024

    # Minimum sleep time to prevent asymptotic behaviour
    _min_sleep_time = 0.01

    # Maximum sleep time
    _max_sleep_time = 0.2

    def __init__(self, source_group, player):
        super(SilentAudioPlayerPacketConsumer, self).__init__(source_group, player)

        # System time of first timestamp
        self._last_update_system_time = None

        # Buffered audio data and events
        self._audio_buffer = SilentAudioBuffer()
        self._event_buffer = EventBuffer()

        # Actual play state.
        self._playing = False
        self._eos = False

        # TODO Be nice to avoid creating this thread if user doesn't care
        #      about EOS events and there's no video format.
        # NOTE Use thread.condition as lock for all instance vars used by worker
        self._thread = MediaThread(target=self._worker_func)
        if source_group.audio_format:
            self._thread.start()

    def delete(self):
        if _debug:
            print('SilentAudioPlayer.delete')
        self._thread.stop()

    def play(self):
        if _debug:
            print('SilentAudioPlayer.play')

        with self._thread.condition:
            self._eos = False
            if not self._playing:
                self._playing = True
                self._update_time()
            self._thread.notify()

    def stop(self):
        if _debug:
            print('SilentAudioPlayer.stop')

        with self._thread.condition:
            if self._playing:
                self._consume_data()
                self._playing = False

    def clear(self):
        if _debug:
            print('SilentAudioPlayer.clear')

        with self._thread.condition:
            self._event_buffer.clear()
            self._audio_buffer.clear()
            self._eos = False
            self._thread.notify()

    def get_time(self):
        with self._thread.condition:
            result = self._audio_buffer.get_current_timestamp() + self._calculate_offset()

        if _debug:
            print('SilentAudioPlayer.get_time() -> ', result)
        return result

    def _update_time(self):
        self._last_update_system_time = time.time()

    def _consume_data(self):
        """Consume content of packets that should have been played back up to now."""
        with self._thread.condition:
            offset = self._calculate_offset()
            self._audio_buffer.consume_audio_data(offset)
            self._update_time()

            if self._audio_buffer.is_empty():
                if _debug:
                    print('Out of packets')
                timestamp = self.get_time()
                MediaEvent(timestamp, 'on_eos')._sync_dispatch_to_player(self.player)
                MediaEvent(timestamp, 'on_source_group_eos')._sync_dispatch_to_player(self.player)

    def _calculate_offset(self):
        """Calculate the current offset into the cached packages."""
        if self._last_update_system_time is None:
            return 0.
        if not self._playing:
            return 0.

        offset = time.time() - self._last_update_system_time
        if offset > self._audio_buffer.duration:
            return self._audio_buffer.duration

        return offset

    def _buffer_data(self):
        """Read data from the audio source into the internal buffer."""
        with self._thread.condition:
            # Calculate how much data to request from source
            secs = self._buffer_time - self._audio_buffer.duration
            bytes_to_read = int(secs * self.source_group.audio_format.bytes_per_second)

            while bytes_to_read > self._min_update_bytes and not self._eos:
                # Pull audio data from source
                if _debug:
                    print('Trying to buffer %d bytes (%r secs)' % (bytes_to_read, secs))
                audio_data = self.source_group.get_audio_data(bytes_to_read)
                if not audio_data:
                    self._eos = True
                    break
                else:
                    self._add_audio_data(audio_data)

                bytes_to_read -= audio_data.length

    def _add_audio_data(self, audio_data):
        """Add a package of audio data to the internal buffer. Update timestamps to reflect."""
        with self._thread.condition:
            self._audio_buffer.add_audio_data(audio_data)
            self._event_buffer.add_events(audio_data)

    def _get_sleep_time(self):
        """Determine how long to sleep until next event or next batch of data needs to be read."""
        if not self._playing:
            # Not playing, so no need to wake up
            return None

        if self._audio_buffer.duration < self._buffer_time/2 and not self._eos:
            # More buffering required
            return 0.

        time_to_next_event = self._event_buffer.get_time_to_next_event(self.get_time())
        time_to_next_buffer_update = self._audio_buffer.get_time_to_next_update()

        if time_to_next_event is None and time_to_next_buffer_update is None and self._eos:
            # Nothing to read and no events to handle:
            return None

        # Wait for first action to take (event or buffer) up to a maximum
        if _debug:
            print('Next event in {}, next buffer in {}'.format(time_to_next_event, time_to_next_buffer_update))
        return max(min(time_to_next_buffer_update or self._max_sleep_time,
                       time_to_next_event or self._max_sleep_time,
                       self._max_sleep_time),
                   self._min_sleep_time)

    def _dispatch_events(self):
        """Dispatch any events for the current timestamp."""
        timestamp = self.get_time()

        # Dispatch events
        for event in self._event_buffer.get_expired_events(timestamp):
            event._sync_dispatch_to_player(self.player)
            if _debug:
                print('Dispatched event {}'.format(event))

    # Worker func that consumes audio data and dispatches events
    def _worker_func(self):
        while True:
            with self._thread.condition:
                if self._thread.stopped:
                    break

                self._consume_data()
                self._dispatch_events()
                self._buffer_data()

                sleep_time = self._get_sleep_time()
                if _debug:
                    print('SilentAudioPlayer(Worker).sleep', sleep_time)
                self._thread.sleep(sleep_time)
                if _debug:
                    print('SilentAudioPlayer(Worker).wakeup')
        if _debug:
            print('SilentAudioPlayer(Worker) ended')


class SilentTimeAudioPlayer(AbstractAudioPlayer):
    # Note that when using this player (automatic if playing back video with
    # unsupported audio codec) no events are dispatched (because they are
    # normally encoded in the audio packet -- so no EOS events are delivered.
    # This is a design flaw.
    #
    # Also, seeking is broken because the timestamps aren't synchronized with
    # the source group.

    _time = 0.0
    _systime = None

    def play(self):
        self._systime = time.time()

    def stop(self):
        self._time = self.get_time()
        self._systime = None

    def delete(self):
        pass

    def clear(self):
        pass

    def get_time(self):
        if self._systime is None:
            return self._time
        else:
            return time.time() - self._systime + self._time


class SilentAudioDriver(AbstractAudioDriver):
    def __init__(self):
        self._players = WeakSet()

    def create_audio_player(self, source_group, player):
        if source_group.audio_format:
            p = SilentAudioPlayerPacketConsumer(source_group, player)
            self._players.add(p)
            return p
        else:
            return SilentTimeAudioPlayer(source_group, player)

    def get_listener(self):
        raise NotImplementedError('Silent audio driver does not support positional audio')

    def delete(self):
        while len(self._players) > 0:
            self._players.pop().delete()


def create_audio_driver():
    return SilentAudioDriver()

