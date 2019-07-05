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
"""
Pythonic interface to DirectSound.
"""
from collections import namedtuple
import ctypes

from pyglet.debug import debug_print
from pyglet.window.win32 import _user32

from . import lib_dsound as lib
from .exceptions import DirectSoundNativeError

_debug_media = debug_print('debug_media')

def _check(hresult):
    if hresult != lib.DS_OK:
        raise DirectSoundNativeError(hresult)


class DirectSoundDriver(object):
    def __init__(self):
        assert _debug_media('Constructing DirectSoundDriver')

        self._native_dsound = lib.IDirectSound()
        _check(
            lib.DirectSoundCreate(None, ctypes.byref(self._native_dsound), None)
        )

        # A trick used by mplayer.. use desktop as window handle since it
        # would be complex to use pyglet window handles (and what to do when
        # application is audio only?).
        hwnd = _user32.GetDesktopWindow()
        _check(
            self._native_dsound.SetCooperativeLevel(hwnd, lib.DSSCL_NORMAL)
        )

        self._buffer_factory = DirectSoundBufferFactory(self, self._native_dsound)
        self.primary_buffer = self._buffer_factory.create_primary_buffer()

    def __del__(self):
        assert _debug_media('Destroying DirectSoundDriver')
        del self.primary_buffer
        self._native_dsound.Release()

    def create_buffer(self, audio_format):
        return self._buffer_factory.create_buffer(audio_format)

    def create_listener(self):
        return self.primary_buffer.create_listener()


class DirectSoundBufferFactory(object):
    default_buffer_size = 2.0

    def __init__(self, driver, native_dsound):
        self.driver = driver
        self._native_dsound = native_dsound

    def create_buffer(self, audio_format):
        buffer_size = int(audio_format.sample_rate * self.default_buffer_size)
        wave_format = self._create_wave_format(audio_format)
        buffer_desc = self._create_buffer_desc(wave_format, buffer_size)
        return DirectSoundBuffer(
                self.driver,
                self._create_buffer(buffer_desc),
                audio_format,
                buffer_size)

    def create_primary_buffer(self):
        return DirectSoundBuffer(
                self,
                self._create_buffer(self._create_primary_buffer_desc()),
                None,
                0)

    def _create_buffer(self, buffer_desc):
        buf = lib.IDirectSoundBuffer()
        _check(
            self._native_dsound.CreateSoundBuffer(buffer_desc, ctypes.byref(buf), None)
        )
        return buf

    @staticmethod
    def _create_wave_format(audio_format):
        wfx = lib.WAVEFORMATEX()
        wfx.wFormatTag = lib.WAVE_FORMAT_PCM
        wfx.nChannels = audio_format.channels
        wfx.nSamplesPerSec = audio_format.sample_rate
        wfx.wBitsPerSample = audio_format.sample_size
        wfx.nBlockAlign = wfx.wBitsPerSample * wfx.nChannels // 8
        wfx.nAvgBytesPerSec = wfx.nSamplesPerSec * wfx.nBlockAlign
        return wfx

    @classmethod
    def _create_buffer_desc(cls, wave_format, buffer_size):
        dsbdesc = lib.DSBUFFERDESC()
        dsbdesc.dwSize = ctypes.sizeof(dsbdesc)
        dsbdesc.dwFlags = (lib.DSBCAPS_GLOBALFOCUS |
                           lib.DSBCAPS_GETCURRENTPOSITION2 |
                           lib.DSBCAPS_CTRLFREQUENCY |
                           lib.DSBCAPS_CTRLVOLUME)
        if wave_format.nChannels == 1:
            dsbdesc.dwFlags |= lib.DSBCAPS_CTRL3D
        dsbdesc.dwBufferBytes = buffer_size
        dsbdesc.lpwfxFormat = ctypes.pointer(wave_format)

        return dsbdesc

    @classmethod
    def _create_primary_buffer_desc(cls):
        """Primary buffer with 3D and volume capabilities"""
        buffer_desc = lib.DSBUFFERDESC()
        buffer_desc.dwSize = ctypes.sizeof(buffer_desc)
        buffer_desc.dwFlags = (lib.DSBCAPS_CTRL3D |
                               lib.DSBCAPS_CTRLVOLUME |
                               lib.DSBCAPS_PRIMARYBUFFER)

        return buffer_desc

class DirectSoundBuffer(object):
    def __init__(self, driver, native_buffer, audio_format, buffer_size):
        self.driver = driver
        self.audio_format = audio_format
        self.buffer_size = buffer_size

        self._native_buffer = native_buffer

        if audio_format is not None and audio_format.channels == 1:
            self._native_buffer3d = lib.IDirectSound3DBuffer()
            self._native_buffer.QueryInterface(lib.IID_IDirectSound3DBuffer,
                                               ctypes.byref(self._native_buffer3d))
        else:
            self._native_buffer3d = None

    def __del__(self):
        if self._native_buffer is not None:
            self._native_buffer.Stop()
            self._native_buffer.Release()
            self._native_buffer = None
            if self._native_buffer3d is not None:
                self._native_buffer3d.Release()
                self._native_buffer3d = None

    @property
    def volume(self):
        vol = lib.LONG()
        _check(
            self._native_buffer.GetVolume(ctypes.byref(vol))
        )
        return vol.value

    @volume.setter
    def volume(self, value):
        _check(
            self._native_buffer.SetVolume(value)
        )

    _CurrentPosition = namedtuple('_CurrentPosition', ['play_cursor', 'write_cursor'])

    @property
    def current_position(self):
        """Tuple of current play position and current write position.
        Only play position can be modified, so setter only accepts a single value."""
        play_cursor = lib.DWORD()
        write_cursor = lib.DWORD()
        _check(
            self._native_buffer.GetCurrentPosition(play_cursor,
                                                   write_cursor)
        )
        return self._CurrentPosition(play_cursor.value, write_cursor.value)

    @current_position.setter
    def current_position(self, value):
        _check(
            self._native_buffer.SetCurrentPosition(value)
        )

    @property
    def is3d(self):
        return self._native_buffer3d is not None

    @property
    def is_playing(self):
        return (self._get_status() & lib.DSBSTATUS_PLAYING) != 0

    @property
    def is_buffer_lost(self):
        return (self._get_status() & lib.DSBSTATUS_BUFFERLOST) != 0

    def _get_status(self):
        status = lib.DWORD()
        _check(
            self._native_buffer.GetStatus(status)
        )
        return status.value

    @property
    def position(self):
        if self.is3d:
            position = lib.D3DVECTOR()
            _check(
                self._native_buffer3d.GetPosition(ctypes.byref(position))
            )
            return position.x, position.y, position.z
        else:
            return 0, 0, 0

    @position.setter
    def position(self, position):
        if self.is3d:
            x, y, z = position
            _check(
                self._native_buffer3d.SetPosition(x, y, z, lib.DS3D_IMMEDIATE)
            )

    @property
    def min_distance(self):
        """The minimum distance, which is the distance from the
        listener at which sounds in this buffer begin to be attenuated."""
        if self.is3d:
            value = lib.D3DVALUE()
            _check(
                self._native_buffer3d.GetMinDistance(ctypes.byref(value))
            )
            return value.value
        else:
            return 0

    @min_distance.setter
    def min_distance(self, value):
        if self.is3d:
            _check(
                self._native_buffer3d.SetMinDistance(value, lib.DS3D_IMMEDIATE)
            )

    @property
    def max_distance(self):
        """The maximum distance, which is the distance from the listener beyond which
        sounds in this buffer are no longer attenuated."""
        if self.is3d:
            value = lib.D3DVALUE()
            _check(
                self._native_buffer3d.GetMaxDistance(ctypes.byref(value))
            )
            return value.value
        else:
            return 0

    @max_distance.setter
    def max_distance(self, value):
        if self.is3d:
            _check(
                self._native_buffer3d.SetMaxDistance(value, lib.DS3D_IMMEDIATE)
            )

    @property
    def frequency(self):
        value = lib.DWORD()
        _check(
            self._native_buffer.GetFrequency(value)
        )
        return value.value

    @frequency.setter
    def frequency(self, value):
        """The frequency, in samples per second, at which the buffer is playing."""
        _check(
            self._native_buffer.SetFrequency(value)
        )

    @property
    def cone_orientation(self):
        """The orientation of the sound projection cone."""
        if self.is3d:
            orientation = lib.D3DVECTOR()
            _check(
                self._native_buffer3d.GetConeOrientation(ctypes.byref(orientation))
            )
            return orientation.x, orientation.y, orientation.z
        else:
            return 0, 0, 0

    @cone_orientation.setter
    def cone_orientation(self, value):
        if self.is3d:
            x, y, z = value
            _check(
                self._native_buffer3d.SetConeOrientation(x, y, z, lib.DS3D_IMMEDIATE)
            )

    _ConeAngles = namedtuple('_ConeAngles', ['inside', 'outside'])
    @property
    def cone_angles(self):
        """The inside and outside angles of the sound projection cone."""
        if self.is3d:
            inside = lib.DWORD()
            outside = lib.DWORD()
            _check(
                self._native_buffer3d.GetConeAngles(ctypes.byref(inside), ctypes.byref(outside))
            )
            return self._ConeAngles(inside.value, outside.value)
        else:
            return self._ConeAngles(0, 0)

    def set_cone_angles(self, inside, outside):
        """The inside and outside angles of the sound projection cone."""
        if self.is3d:
            _check(
                self._native_buffer3d.SetConeAngles(inside, outside, lib.DS3D_IMMEDIATE)
            )

    @property
    def cone_outside_volume(self):
        """The volume of the sound outside the outside angle of the sound projection cone."""
        if self.is3d:
            volume = lib.LONG()
            _check(
                self._native_buffer3d.GetConeOutsideVolume(ctypes.byref(volume))
            )
            return volume.value
        else:
            return 0

    @cone_outside_volume.setter
    def cone_outside_volume(self, value):
        if self.is3d:
            _check(
                self._native_buffer3d.SetConeOutsideVolume(value, lib.DS3D_IMMEDIATE)
            )

    def create_listener(self):
        native_listener = lib.IDirectSound3DListener()
        self._native_buffer.QueryInterface(lib.IID_IDirectSound3DListener,
                                           ctypes.byref(native_listener))
        return DirectSoundListener(self, native_listener)

    def play(self):
        _check(
            self._native_buffer.Play(0, 0, lib.DSBPLAY_LOOPING)
        )

    def stop(self):
        _check(
            self._native_buffer.Stop()
        )

    class _WritePointer(object):
        def __init__(self):
            self.audio_ptr_1 = ctypes.c_void_p()
            self.audio_length_1 = lib.DWORD()
            self.audio_ptr_2 = ctypes.c_void_p()
            self.audio_length_2 = lib.DWORD()

    def lock(self, write_cursor, write_size):
        assert _debug_media('DirectSoundBuffer.lock({}, {})'.format(write_cursor, write_size))
        pointer = self._WritePointer()
        _check(
            self._native_buffer.Lock(write_cursor,
                                     write_size,
                                     ctypes.byref(pointer.audio_ptr_1),
                                     pointer.audio_length_1,
                                     ctypes.byref(pointer.audio_ptr_2),
                                     pointer.audio_length_2,
                                     0)
        )
        return pointer

    def unlock(self, pointer):
        _check(
            self._native_buffer.Unlock(pointer.audio_ptr_1,
                                       pointer.audio_length_1,
                                       pointer.audio_ptr_2,
                                       pointer.audio_length_2)
        )


class DirectSoundListener(object):
    def __init__(self, ds_buffer, native_listener):
        self.ds_buffer = ds_buffer
        self._native_listener = native_listener

    def __del__(self):
        self._native_listener.Release()

    @property
    def position(self):
        vector = lib.D3DVECTOR()
        _check(
            self._native_listener.GetPosition(ctypes.byref(vector))
        )
        return (vector.x, vector.y, vector.z)

    @position.setter
    def position(self, value):
        _check(
            self._native_listener.SetPosition(*(list(value) + [lib.DS3D_IMMEDIATE]))
        )

    @property
    def orientation(self):
        front = lib.D3DVECTOR()
        top = lib.D3DVECTOR()
        _check(
            self._native_listener.GetOrientation(ctypes.byref(front), ctypes.byref(top))
        )
        return (front.x, front.y, front.z, top.x, top.y, top.z)

    @orientation.setter
    def orientation(self, orientation):
        _check(
            self._native_listener.SetOrientation(*(list(orientation) + [lib.DS3D_IMMEDIATE]))
        )


