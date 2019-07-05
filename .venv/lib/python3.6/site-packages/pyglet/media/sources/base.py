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

import ctypes
import sys

import pyglet
from pyglet.compat import bytes_type, BytesIO
from pyglet.media.events import MediaEvent
from pyglet.media.exceptions import MediaException

_debug = pyglet.options['debug_media']


class AudioFormat(object):
    """Audio details.

    An instance of this class is provided by sources with audio tracks.  You
    should not modify the fields, as they are used internally to describe the
    format of data provided by the source.

    :Ivariables:
        `channels` : int
            The number of channels: 1 for mono or 2 for stereo (pyglet does
            not yet support surround-sound sources).
        `sample_size` : int
            Bits per sample; only 8 or 16 are supported.
        `sample_rate` : int
            Samples per second (in Hertz).

    """

    def __init__(self, channels, sample_size, sample_rate):
        self.channels = channels
        self.sample_size = sample_size
        self.sample_rate = sample_rate
        
        # Convenience
        self.bytes_per_sample = (sample_size >> 3) * channels
        self.bytes_per_second = self.bytes_per_sample * sample_rate

    def __eq__(self, other):
        return (self.channels == other.channels and 
                self.sample_size == other.sample_size and
                self.sample_rate == other.sample_rate)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return '%s(channels=%d, sample_size=%d, sample_rate=%d)' % (
            self.__class__.__name__, self.channels, self.sample_size,
            self.sample_rate)

class VideoFormat(object):
    """Video details.

    An instance of this class is provided by sources with a video track.  You
    should not modify the fields.

    Note that the sample aspect has no relation to the aspect ratio of the
    video image.  For example, a video image of 640x480 with sample aspect 2.0
    should be displayed at 1280x480.  It is the responsibility of the
    application to perform this scaling.

    :Ivariables:
        `width` : int
            Width of video image, in pixels.
        `height` : int
            Height of video image, in pixels.
        `sample_aspect` : float
            Aspect ratio (width over height) of a single video pixel.
        `frame_rate` : float
            Frame rate (frames per second) of the video.

            AVbin 8 or later is required, otherwise the frame rate will be
            ``None``.

            .. versionadded:: 1.2.

    """
    
    def __init__(self, width, height, sample_aspect=1.0):
        self.width = width
        self.height = height
        self.sample_aspect = sample_aspect
        self.frame_rate = None

class AudioData(object):
    """A single packet of audio data.

    This class is used internally by pyglet.

    :Ivariables:
        `data` : str or ctypes array or pointer
            Sample data.
        `length` : int
            Size of sample data, in bytes.
        `timestamp` : float
            Time of the first sample, in seconds.
        `duration` : float
            Total data duration, in seconds.
        `events` : list of MediaEvent
            List of events contained within this packet.  Events are
            timestamped relative to this audio packet.

    """
    def __init__(self, data, length, timestamp, duration, events):
        self.data = data
        self.length = length
        self.timestamp = timestamp
        self.duration = duration
        self.events = events

    def consume(self, bytes, audio_format):
        """Remove some data from beginning of packet.  All events are
        cleared."""
        self.events = ()
        if bytes >= self.length:
            self.data = None
            self.length = 0
            self.timestamp += self.duration
            self.duration = 0.
            return
        elif bytes == 0:
            return

        if not isinstance(self.data, str):
            # XXX Create a string buffer for the whole packet then
            #     chop it up.  Could do some pointer arith here and
            #     save a bit of data pushing, but my guess is this is
            #     faster than fudging aruond with ctypes (and easier).
            data = ctypes.create_string_buffer(self.length)
            ctypes.memmove(data, self.data, self.length)
            self.data = data
        self.data = self.data[bytes:]
        self.length -= bytes
        self.duration -= bytes / float(audio_format.bytes_per_second)
        self.timestamp += bytes / float(audio_format.bytes_per_second)

    def get_string_data(self):
        """Return data as a string. (Python 3: return as bytes)"""
        if self.data is None:
            return b''

        if isinstance(self.data, bytes_type):
            return self.data

        buf = ctypes.create_string_buffer(self.length)
        ctypes.memmove(buf, self.data, self.length)
        return buf.raw

class SourceInfo(object):
    """Source metadata information.

    Fields are the empty string or zero if the information is not available.

    :Ivariables:
        `title` : str
            Title
        `author` : str
            Author
        `copyright` : str
            Copyright statement
        `comment` : str
            Comment
        `album` : str
            Album name
        `year` : int
            Year
        `track` : int
            Track number
        `genre` : str
            Genre

    .. versionadded:: 1.2
    """

    title = ''
    author = ''
    copyright = ''
    comment = ''
    album = ''
    year = 0
    track = 0
    genre = ''

class Source(object):
    """An audio and/or video source.

    :Ivariables:
        `audio_format` : `AudioFormat`
            Format of the audio in this source, or None if the source is
            silent.
        `video_format` : `VideoFormat`
            Format of the video in this source, or None if there is no
            video.
        `info` : `SourceInfo`
            Source metadata such as title, artist, etc; or None if the
            information is not available.

            .. versionadded:: 1.2
    """

    _duration = None
    
    audio_format = None
    video_format = None
    info = None

    def _get_duration(self):
        return self._duration

    duration = property(lambda self: self._get_duration(),
                        doc="""The length of the source, in seconds.

        Not all source durations can be determined; in this case the value
        is None.

        Read-only.

        :type: float
        """)

    def play(self):
        """Play the source.

        This is a convenience method which creates a Player for
        this source and plays it immediately.

        :rtype: `Player`
        """
        from pyglet.media.player import Player  # XXX Nasty circular dependency
        player = Player()
        player.queue(self)
        player.play()
        return player

    def get_animation(self):
        """Import all video frames into memory as an :py:class:`~pyglet.image.Animation`.

        An empty animation will be returned if the source has no video.
        Otherwise, the animation will contain all unplayed video frames (the
        entire source, if it has not been queued on a player).  After creating
        the animation, the source will be at EOS.

        This method is unsuitable for videos running longer than a
        few seconds.

        .. versionadded:: 1.1

        :rtype: `pyglet.image.Animation`
        """
        from pyglet.image import Animation, AnimationFrame
        if not self.video_format:
            # XXX: This causes an assertion in the constructor of Animation
            return Animation([])
        else:
            frames = []
            last_ts = 0
            next_ts = self.get_next_video_timestamp()
            while next_ts is not None:
                image = self.get_next_video_frame()
                if image is not None:
                    delay = next_ts - last_ts
                    frames.append(AnimationFrame(image, delay))
                    last_ts = next_ts
                next_ts = self.get_next_video_timestamp()
            return Animation(frames)

    def get_next_video_timestamp(self):
        """Get the timestamp of the next video frame.

        .. versionadded:: 1.1

        :rtype: float
        :return: The next timestamp, or ``None`` if there are no more video
            frames.
        """
        pass

    def get_next_video_frame(self):
        """Get the next video frame.

        Video frames may share memory: the previous frame may be invalidated
        or corrupted when this method is called unless the application has
        made a copy of it.

        .. versionadded:: 1.1

        :rtype: `pyglet.image.AbstractImage`
        :return: The next video frame image, or ``None`` if the video frame
            could not be decoded or there are no more video frames.
        """
        pass

    # Internal methods that SourceGroup calls on the source:

    def seek(self, timestamp):
        """Seek to given timestamp."""
        raise CannotSeekException()

    def _get_queue_source(self):
        """Return the `Source` to be used as the queue source for a player.

        Default implementation returns self."""
        return self

    def get_audio_data(self, bytes):
        """Get next packet of audio data.

        :Parameters:
            `bytes` : int
                Maximum number of bytes of data to return.

        :rtype: `AudioData`
        :return: Next packet of audio data, or None if there is no (more)
            data.
        """
        return None

class StreamingSource(Source):
    """A source that is decoded as it is being played, and can only be
    queued once.
    """
    
    _is_queued = False

    is_queued = property(lambda self: self._is_queued,
                         doc="""Determine if this source has been queued
        on a :py:class:`~pyglet.media.player.Player` yet.

        Read-only.

        :type: bool
        """)

    def _get_queue_source(self):
        """Return the `Source` to be used as the queue source for a player.

        Default implementation returns self."""
        if self._is_queued:
            raise MediaException('This source is already queued on a player.')
        self._is_queued = True
        return self

    def delete(self):
        pass

class StaticSource(Source):
    """A source that has been completely decoded in memory.  This source can
    be queued onto multiple players any number of times.
    """
    
    def __init__(self, source):
        """Construct a :py:class:`~pyglet.media.StaticSource` for the data in `source`.

        :Parameters:
            `source` : `Source`
                The source to read and decode audio and video data from.

        """
        source = source._get_queue_source()
        if source.video_format:
            raise NotImplementedError(
                'Static sources not supported for video yet.')

        self.audio_format = source.audio_format
        if not self.audio_format:
            self._data = None
            self._duration = 0.
            return

        # Arbitrary: number of bytes to request at a time.
        buffer_size = 1 << 20 # 1 MB

        # Naive implementation.  Driver-specific implementations may override
        # to load static audio data into device (or at least driver) memory. 
        data = BytesIO()
        while True:
            audio_data = source.get_audio_data(buffer_size)
            if not audio_data:
                break
            data.write(audio_data.get_string_data())
        self._data = data.getvalue()

        self._duration = len(self._data) / \
                float(self.audio_format.bytes_per_second)

    def _get_queue_source(self):
        if self._data is not None:
            return StaticMemorySource(self._data, self.audio_format)

    def get_audio_data(self, bytes):
        raise RuntimeError('StaticSource cannot be queued.')

class StaticMemorySource(StaticSource):
    """Helper class for default implementation of :py:class:`~pyglet.media.StaticSource`.  Do not use
    directly."""

    def __init__(self, data, audio_format):
        """Construct a memory source over the given data buffer.
        """
        self._file = BytesIO(data)
        self._max_offset = len(data)
        self.audio_format = audio_format
        self._duration = len(data) / float(audio_format.bytes_per_second)

    def seek(self, timestamp):
        offset = int(timestamp * self.audio_format.bytes_per_second)

        # Align to sample
        if self.audio_format.bytes_per_sample == 2:
            offset &= 0xfffffffe
        elif self.audio_format.bytes_per_sample == 4:
            offset &= 0xfffffffc

        self._file.seek(offset)

    def get_audio_data(self, bytes):
        offset = self._file.tell()
        timestamp = float(offset) / self.audio_format.bytes_per_second

        # Align to sample size
        if self.audio_format.bytes_per_sample == 2:
            bytes &= 0xfffffffe
        elif self.audio_format.bytes_per_sample == 4:
            bytes &= 0xfffffffc

        data = self._file.read(bytes)
        if not len(data):
            return None

        duration = float(len(data)) / self.audio_format.bytes_per_second
        return AudioData(data, len(data), timestamp, duration, [])

class SourceGroup(object):
    """Read data from a queue of sources, with support for looping.  All
    sources must share the same audio format.
    
    :Ivariables:
        `audio_format` : `AudioFormat`
            Required audio format for queued sources.

    """

    # TODO can sources list go empty?  what behaviour (ignore or error)?

    _advance_after_eos = False
    _loop = False

    def __init__(self, audio_format, video_format):
        self.audio_format = audio_format
        self.video_format = video_format
        self.duration = 0.
        self._timestamp_offset = 0.
        self._dequeued_durations = []
        self._sources = []

    def seek(self, time):
        if self._sources:
            self._sources[0].seek(time)

    def queue(self, source):
        source = source._get_queue_source()
        assert(source.audio_format == self.audio_format)
        self._sources.append(source)
        self.duration += source.duration

    def has_next(self):
        return len(self._sources) > 1

    def next_source(self, immediate=True):
        if immediate:
            self._advance()
        else:
            self._advance_after_eos = True

    #: :deprecated: Use `next_source` instead.
    next = next_source  # old API, worked badly with 2to3

    def get_current_source(self):
        if self._sources:
            return self._sources[0]

    def _advance(self):
        if self._sources:
            self._timestamp_offset += self._sources[0].duration
            self._dequeued_durations.insert(0, self._sources[0].duration)
            old_source = self._sources.pop(0)
            self.duration -= old_source.duration

            if isinstance(old_source, StreamingSource):
                old_source.delete()
                del old_source

    def _get_loop(self):
        return self._loop

    def _set_loop(self, loop):
        self._loop = loop        

    loop = property(_get_loop, _set_loop, 
                    doc="""Loop the current source indefinitely or until 
    `next` is called.  Initially False.

    :type: bool
    """)

    def get_audio_data(self, bytes):
        """Get next audio packet.

        :Parameters:
            `bytes` : int
                Hint for preferred size of audio packet; may be ignored.

        :rtype: `AudioData`
        :return: Audio data, or None if there is no more data.
        """

        if not self._sources:
            return None
        data = self._sources[0].get_audio_data(bytes)
        eos = False
        while not data:
            eos = True
            if self._loop and not self._advance_after_eos:
                self._timestamp_offset += self._sources[0].duration
                self._dequeued_durations.insert(0, self._sources[0].duration)
                self._sources[0].seek(0)
            else:
                self._advance_after_eos = False

                # Advance source if there's something to advance to.
                # Otherwise leave last source paused at EOS.
                if len(self._sources) > 1:
                    self._advance()
                else:
                    return None

            data = self._sources[0].get_audio_data(bytes) # TODO method rename

        data.timestamp += self._timestamp_offset
        if eos:
            if _debug:
                print('adding on_eos event to audio data')
            data.events.append(MediaEvent(0, 'on_eos'))
        return data

    def translate_timestamp(self, timestamp):
        """Get source-relative timestamp for the audio player's timestamp."""
        # XXX 
        if timestamp is None:
            return None

        timestamp = timestamp - self._timestamp_offset
        if timestamp < 0:
            # _dequeued_durations is already ordered last to first
            for duration in self._dequeued_durations:
                timestamp += duration
                if timestamp > 0:
                    break
            assert timestamp >= 0, 'Timestamp beyond dequeued source memory'
        return timestamp

    def get_next_video_timestamp(self):
        """Get the timestamp of the next video frame.

        :rtype: float
        :return: The next timestamp, or ``None`` if there are no more video
            frames.
        """
        # TODO track current video source independently from audio source for
        # better prebuffering.
        if not self._sources:
            return None
        timestamp = self._sources[0].get_next_video_timestamp()
        if timestamp is not None: 
            timestamp += self._timestamp_offset
        return timestamp

    def get_next_video_frame(self):
        """Get the next video frame.

        Video frames may share memory: the previous frame may be invalidated
        or corrupted when this method is called unless the application has
        made a copy of it.

        :rtype: `pyglet.image.AbstractImage`
        :return: The next video frame image, or ``None`` if the video frame
            could not be decoded or there are no more video frames.
        """
        if self._sources:
            return self._sources[0].get_next_video_frame()


