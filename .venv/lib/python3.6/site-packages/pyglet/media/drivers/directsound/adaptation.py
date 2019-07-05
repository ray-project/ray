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
from __future__ import absolute_import, print_function

import ctypes
import math
import threading

from . import interface
from pyglet.debug import debug_print
from pyglet.media.events import MediaEvent
from pyglet.media.drivers.base import AbstractAudioDriver, AbstractAudioPlayer
from pyglet.media.listener import AbstractListener
from pyglet.media.threads import PlayerWorker

_debug = debug_print('debug_media')


def _convert_coordinates(coordinates):
    x, y, z = coordinates
    return (x, y, -z)


def _gain2db(gain):
    """
    Convert linear gain in range [0.0, 1.0] to 100ths of dB.

    Power gain = P1/P2
    dB = 10 log(P1/P2)
    dB * 100 = 1000 * log(power gain)
    """
    if gain <= 0:
        return -10000
    return max(-10000, min(int(1000 * math.log10(min(gain, 1))), 0))


def _db2gain(db):
    """Convert 100ths of dB to linear gain."""
    return math.pow(10.0, float(db)/1000.0)


class DirectSoundAudioPlayer(AbstractAudioPlayer):
    # Need to cache these because pyglet API allows update separately, but
    # DSound requires both to be set at once.
    _cone_inner_angle = 360
    _cone_outer_angle = 360

    min_buffer_size = 9600

    def __init__(self, driver, ds_driver, source_group, player):
        super(DirectSoundAudioPlayer, self).__init__(source_group, player)

        self.driver = driver
        self._ds_driver = ds_driver

        # Locking strategy:
        # All DirectSound calls should be locked.  All instance vars relating
        # to buffering/filling/time/events should be locked (used by both
        # application and worker thread).  Other instance vars (consts and
        # 3d vars) do not need to be locked.
        self._lock = threading.RLock()

        # Desired play state (may be actually paused due to underrun -- not
        # implemented yet).
        self._playing = False

        # Up to one audio data may be buffered if too much data was received
        # from the source that could not be written immediately into the
        # buffer.  See refill().
        self._audiodata_buffer = None

        # Theoretical write and play cursors for an infinite buffer.  play
        # cursor is always <= write cursor (when equal, underrun is
        # happening).
        self._write_cursor = 0
        self._play_cursor = 0

        # Cursor position of end of data.  Silence is written after
        # eos for one buffer size.
        self._eos_cursor = None

        # Indexes into DSound circular buffer.  Complications ensue wrt each
        # other to avoid writing over the play cursor.  See get_write_size and
        # write().
        self._play_cursor_ring = 0
        self._write_cursor_ring = 0

        # List of (play_cursor, MediaEvent), in sort order
        self._events = []

        # List of (cursor, timestamp), in sort order (cursor gives expiry
        # place of the timestamp)
        self._timestamps = []

        audio_format = source_group.audio_format

        # DSound buffer
        self._ds_buffer = self._ds_driver.create_buffer(audio_format)
        self._buffer_size = self._ds_buffer.buffer_size

        self._ds_buffer.current_position = 0

        self.refill(self._buffer_size)

    def __del__(self):
        try:
            self.delete()
        except:
            pass

    def delete(self):
        if self.driver and self.driver.worker:
            self.driver.worker.remove(self)

        with self._lock:
            self._ds_buffer = None

    def play(self):
        assert _debug('DirectSound play')
        self.driver.worker.add(self)

        with self._lock:
            if not self._playing:
                self._get_audiodata()  # prebuffer if needed
                self._playing = True
                self._ds_buffer.play()

        assert _debug('return DirectSound play')

    def stop(self):
        assert _debug('DirectSound stop')

        with self._lock:
            if self._playing:
                self._playing = False
                self._ds_buffer.stop()

        assert _debug('return DirectSound stop')

    def clear(self):
        assert _debug('DirectSound clear')
        with self._lock:
            self._ds_buffer.current_position = 0
            self._play_cursor_ring = self._write_cursor_ring = 0
            self._play_cursor = self._write_cursor
            self._eos_cursor = None
            self._audiodata_buffer = None
            del self._events[:]
            del self._timestamps[:]

    def refill(self, write_size):
        with self._lock:
            while write_size > 0:
                assert _debug('refill, write_size =', write_size)
                audio_data = self._get_audiodata()

                if audio_data is not None:
                    assert _debug('write', audio_data.length)
                    length = min(write_size, audio_data.length)
                    self.write(audio_data, length)
                    write_size -= length
                else:
                    assert _debug('write silence')
                    self.write(None, write_size)
                    write_size = 0

    def _has_underrun(self):
        return (self._eos_cursor is not None
                and self._play_cursor > self._eos_cursor)

    def _dispatch_new_event(self, event_name):
        MediaEvent(0, event_name)._sync_dispatch_to_player(self.player)

    def _get_audiodata(self):
        if self._audiodata_buffer is None or self._audiodata_buffer.length == 0:
            self._get_new_audiodata()

        return self._audiodata_buffer

    def _get_new_audiodata(self):
        assert _debug('Getting new audio data buffer.')
        self._audiodata_buffer = self.source_group.get_audio_data(self._buffer_size)

        if self._audiodata_buffer is not None:
            assert _debug('New audio data available: {} bytes'.format(self._audiodata_buffer.length))

            if self._eos_cursor is not None:
                self._move_write_cursor_after_eos()

            self._add_audiodata_events(self._audiodata_buffer)
            self._add_audiodata_timestamp(self._audiodata_buffer)
            self._eos_cursor = None
        elif self._eos_cursor is None:
            assert _debug('No more audio data.')
            self._eos_cursor = self._write_cursor

    def _move_write_cursor_after_eos(self):
        # Set the write cursor back to eos_cursor or play_cursor to prevent gaps
        if self._play_cursor < self._eos_cursor:
            cursor_diff = self._write_cursor - self._eos_cursor
            assert _debug('Moving cursor back', cursor_diff)
            self._write_cursor = self._eos_cursor
            self._write_cursor_ring -= cursor_diff
            self._write_cursor_ring %= self._buffer_size

        else:
            cursor_diff = self._play_cursor - self._eos_cursor
            assert _debug('Moving cursor back', cursor_diff)
            self._write_cursor = self._play_cursor
            self._write_cursor_ring -= cursor_diff
            self._write_cursor_ring %= self._buffer_size

    def _add_audiodata_events(self, audio_data):
        for event in audio_data.events:
            event_cursor = self._write_cursor + event.timestamp * \
                self.source_group.audio_format.bytes_per_second
            assert _debug('Adding event', event, 'at', event_cursor)
            self._events.append((event_cursor, event))

    def _add_audiodata_timestamp(self, audio_data):
        ts_cursor = self._write_cursor + audio_data.length
        self._timestamps.append(
            (ts_cursor, audio_data.timestamp + audio_data.duration))

    def update_play_cursor(self):
        with self._lock:
            play_cursor_ring = self._ds_buffer.current_position.play_cursor
            if play_cursor_ring < self._play_cursor_ring:
                # Wrapped around
                self._play_cursor += self._buffer_size - self._play_cursor_ring
                self._play_cursor_ring = 0
            self._play_cursor += play_cursor_ring - self._play_cursor_ring
            self._play_cursor_ring = play_cursor_ring

        self._dispatch_pending_events()
        self._cleanup_timestamps()
        self._check_underrun()

    def _dispatch_pending_events(self):
        with self._lock:
            pending_events = []
            while self._events and self._events[0][0] <= self._play_cursor:
                _, event = self._events.pop(0)
                pending_events.append(event)
            assert _debug('Dispatching pending events: {}'.format(pending_events))
            assert _debug('Remaining events: {}'.format(self._events))

        for event in pending_events:
            event._sync_dispatch_to_player(self.player)

    def _cleanup_timestamps(self):
        with self._lock:
            while self._timestamps and self._timestamps[0][0] < self._play_cursor:
                del self._timestamps[0]

    def _check_underrun(self):
        if self._playing and self._has_underrun():
            assert _debug('underrun, stopping')
            self.stop()
            self._dispatch_new_event('on_eos')
            self._dispatch_new_event('on_source_group_eos')

    def get_write_size(self):
        self.update_play_cursor()

        with self._lock:
            play_cursor = self._play_cursor
            write_cursor = self._write_cursor

        return self._buffer_size - max(write_cursor - play_cursor, 0)

    def write(self, audio_data, length):
        # Pass audio_data=None to write silence
        if length == 0:
            return 0

        with self._lock:
            write_ptr = self._ds_buffer.lock(self._write_cursor_ring, length)
            assert 0 < length <= self._buffer_size
            assert length == write_ptr.audio_length_1.value + write_ptr.audio_length_2.value

            if audio_data:
                ctypes.memmove(write_ptr.audio_ptr_1, audio_data.data, write_ptr.audio_length_1.value)
                audio_data.consume(write_ptr.audio_length_1.value, self.source_group.audio_format)
                if write_ptr.audio_length_2.value > 0:
                    ctypes.memmove(write_ptr.audio_ptr_2, audio_data.data, write_ptr.audio_length_2.value)
                    audio_data.consume(write_ptr.audio_length_2.value, self.source_group.audio_format)
            else:
                if self.source_group.audio_format.sample_size == 8:
                    c = 0x80
                else:
                    c = 0
                ctypes.memset(write_ptr.audio_ptr_1, c, write_ptr.audio_length_1.value)
                if write_ptr.audio_length_2.value > 0:
                    ctypes.memset(write_ptr.audio_ptr_2, c, write_ptr.audio_length_2.value)
            self._ds_buffer.unlock(write_ptr)

            self._write_cursor += length
            self._write_cursor_ring += length
            self._write_cursor_ring %= self._buffer_size

    def get_time(self):
        with self._lock:
            if self._timestamps:
                cursor, ts = self._timestamps[0]
                result = ts + (self._play_cursor - cursor) / \
                    float(self.source_group.audio_format.bytes_per_second)
            else:
                result = None

        return result

    def set_volume(self, volume):
        with self._lock:
            self._ds_buffer.volume = _gain2db(volume)

    def set_position(self, position):
        if self._ds_buffer.is3d:
            with self._lock:
                self._ds_buffer.position = _convert_coordinates(position)

    def set_min_distance(self, min_distance):
        if self._ds_buffer.is3d:
            with self._lock:
                self._ds_buffer.min_distance = min_distance

    def set_max_distance(self, max_distance):
        if self._ds_buffer.is3d:
            with self._lock:
                self._ds_buffer.max_distance = max_distance

    def set_pitch(self, pitch):
        frequency = int(pitch * self.source_group.audio_format.sample_rate)
        with self._lock:
            self._ds_buffer.frequency = frequency

    def set_cone_orientation(self, cone_orientation):
        if self._ds_buffer.is3d:
            with self._lock:
                self._ds_buffer.cone_orientation = _convert_coordinates(cone_orientation)

    def set_cone_inner_angle(self, cone_inner_angle):
        if self._ds_buffer.is3d:
            self._cone_inner_angle = int(cone_inner_angle)
            self._set_cone_angles()

    def set_cone_outer_angle(self, cone_outer_angle):
        if self._ds_buffer.is3d:
            self._cone_outer_angle = int(cone_outer_angle)
            self._set_cone_angles()

    def _set_cone_angles(self):
        inner = min(self._cone_inner_angle, self._cone_outer_angle)
        outer = max(self._cone_inner_angle, self._cone_outer_angle)
        with self._lock:
            self._ds_buffer.set_cone_angles(inner, outer)

    def set_cone_outer_gain(self, cone_outer_gain):
        if self._ds_buffer.is3d:
            volume = _gain2db(cone_outer_gain)
            with self._lock:
                self._ds_buffer.cone_outside_volume = volume


class DirectSoundDriver(AbstractAudioDriver):
    def __init__(self):
        self._ds_driver = interface.DirectSoundDriver()
        self._ds_listener = self._ds_driver.create_listener()

        assert self._ds_driver is not None
        assert self._ds_listener is not None

        # Create worker thread
        self.worker = PlayerWorker()
        self.worker.start()

    def __del__(self):
        try:
            if self._ds_driver:
                self.delete()
        except:
            pass

    def create_audio_player(self, source_group, player):
        assert self._ds_driver is not None
        return DirectSoundAudioPlayer(self, self._ds_driver, source_group, player)

    def get_listener(self):
        assert self._ds_driver is not None
        assert self._ds_listener is not None
        return DirectSoundListener(self._ds_listener, self._ds_driver.primary_buffer)

    def delete(self):
        self.worker.stop()
        self._ds_listener = None
        self._ds_driver = None


class DirectSoundListener(AbstractListener):
    def __init__(self, ds_listener, ds_buffer):
        self._ds_listener = ds_listener
        self._ds_buffer = ds_buffer

    def _set_volume(self, volume):
        self._volume = volume
        self._ds_buffer.volume = _gain2db(volume)

    def _set_position(self, position):
        self._position = position
        self._ds_listener.position = _convert_coordinates(position)

    def _set_forward_orientation(self, orientation):
        self._forward_orientation = orientation
        self._set_orientation()

    def _set_up_orientation(self, orientation):
        self._up_orientation = orientation
        self._set_orientation()

    def _set_orientation(self):
        self._ds_listener.orientation = (_convert_coordinates(self._forward_orientation)
                                         + _convert_coordinates(self._up_orientation))

