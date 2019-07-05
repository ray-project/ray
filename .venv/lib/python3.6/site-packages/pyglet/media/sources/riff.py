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

"""Simple Python-only RIFF reader, supports uncompressed WAV files.
"""
from __future__ import division
from builtins import object

# RIFF reference:
# http://www.saettler.com/RIFFMCI/riffmci.html
#
# More readable WAVE summaries:
#
# http://www.borg.com/~jglatt/tech/wave.htm
# http://www.sonicspot.com/guide/wavefiles.html

from pyglet.media.exceptions import MediaFormatException
from pyglet.media.sources.base import StreamingSource, AudioData, AudioFormat
from pyglet.compat import BytesIO, asbytes

import struct

WAVE_FORMAT_PCM = 0x0001
IBM_FORMAT_MULAW = 0x0101
IBM_FORMAT_ALAW = 0x0102
IBM_FORMAT_ADPCM = 0x0103

class RIFFFormatException(MediaFormatException):
    pass

class WAVEFormatException(RIFFFormatException):
    pass

class RIFFChunk(object):
    header_fmt = '<4sL'
    header_length = struct.calcsize(header_fmt)

    def __init__(self, file, name, length, offset):
        self.file = file
        self.name = name
        self.length = length
        self.offset = offset

    def get_data(self):
        self.file.seek(self.offset)
        return self.file.read(self.length)

    def __repr__(self):
        return '%s(%r, offset=%r, length=%r)' % (
            self.__class__.__name__,
            self.name,
            self.offset,
            self.length)

class RIFFForm(object):
    _chunks = None

    def __init__(self, file, offset):
        self.file = file
        self.offset = offset

    def get_chunks(self):
        if self._chunks:
            return self._chunks

        self._chunks = []
        self.file.seek(self.offset)
        offset = self.offset
        while True:
            header = self.file.read(RIFFChunk.header_length)
            if not header:
                break
            name, length = struct.unpack(RIFFChunk.header_fmt, header)
            offset += RIFFChunk.header_length

            cls = self._chunk_types.get(name, RIFFChunk)
            chunk = cls(self.file, name, length, offset)
            self._chunks.append(chunk)

            offset += length
            if offset & 0x3 != 0:
                offset = (offset | 0x3) + 1
            self.file.seek(offset)
        return self._chunks

    def __repr__(self):
        return '%s(offset=%r)' % (self.__class__.__name__, self.offset)

class RIFFType(RIFFChunk):
    def __init__(self, *args, **kwargs):
        super(RIFFType, self).__init__(*args, **kwargs)
        
        self.file.seek(self.offset)
        form = self.file.read(4)
        if form != asbytes('WAVE'):
            raise RIFFFormatException('Unsupported RIFF form "%s"' % form)

        self.form = WaveForm(self.file, self.offset + 4)

class RIFFFile(RIFFForm):
    _chunk_types = {
        asbytes('RIFF'): RIFFType,
    }

    def __init__(self, file):
        if not hasattr(file, 'seek'):
            file = BytesIO(file.read())

        super(RIFFFile, self).__init__(file, 0)

    def get_wave_form(self):
        chunks = self.get_chunks()
        if len(chunks) == 1 and isinstance(chunks[0], RIFFType):
            return chunks[0].form

class WaveFormatChunk(RIFFChunk):
    def __init__(self, *args, **kwargs):
        super(WaveFormatChunk, self).__init__(*args, **kwargs)
        
        fmt = '<HHLLHH'
        if struct.calcsize(fmt) != self.length:
            raise RIFFFormatException('Size of format chunk is incorrect.')

        (self.wFormatTag,
         self.wChannels,
         self.dwSamplesPerSec,
         self.dwAvgBytesPerSec,
         self.wBlockAlign,
         self.wBitsPerSample) = struct.unpack(fmt, self.get_data())

class WaveDataChunk(RIFFChunk):
    pass

class WaveForm(RIFFForm):
    _chunk_types = {
        asbytes('fmt '): WaveFormatChunk,
        asbytes('data'): WaveDataChunk
    }

    def get_format_chunk(self):
        for chunk in self.get_chunks():
            if isinstance(chunk, WaveFormatChunk):
                return chunk
        
    def get_data_chunk(self):
        for chunk in self.get_chunks():
            if isinstance(chunk, WaveDataChunk):
                return chunk

class WaveSource(StreamingSource):
    def __init__(self, filename, file=None):
        if file is None:
            file = open(filename, 'rb')

        self._file = file

        # Read RIFF format, get format and data chunks
        riff = RIFFFile(file)
        wave_form = riff.get_wave_form()
        if wave_form:
            format = wave_form.get_format_chunk()
            data_chunk = wave_form.get_data_chunk()

        if not wave_form or not format or not data_chunk:
            if not filename or filename.lower().endswith('.wav'):
                raise WAVEFormatException('Not a WAVE file')
            else:
                raise WAVEFormatException(
                    'AVbin is required to decode compressed media')

        if format.wFormatTag != WAVE_FORMAT_PCM:
            raise WAVEFormatException('Unsupported WAVE format category')

        if format.wBitsPerSample not in (8, 16):
            raise WAVEFormatException('Unsupported sample bit size: %d' %
                format.wBitsPerSample)

        self.audio_format = AudioFormat(
            channels=format.wChannels,
            sample_size=format.wBitsPerSample,
            sample_rate=format.dwSamplesPerSec)
        self._duration = \
            float(data_chunk.length) / self.audio_format.bytes_per_second

        self._start_offset = data_chunk.offset
        self._max_offset = data_chunk.length
        self._offset = 0
        self._file.seek(self._start_offset)

    def get_audio_data(self, bytes):
        bytes = min(bytes, self._max_offset - self._offset)
        if not bytes:
            return None

        data = self._file.read(bytes)
        self._offset += len(data)

        timestamp = float(self._offset) / self.audio_format.bytes_per_second
        duration = float(bytes) / self.audio_format.bytes_per_second

        return AudioData(data, len(data), timestamp, duration, [])

    def seek(self, timestamp):
        offset = int(timestamp * self.audio_format.bytes_per_second)

        # Bound within duration
        offset = min(max(offset, 0), self._max_offset)

        # Align to sample
        if self.audio_format.bytes_per_sample == 2:
            offset &= 0xfffffffe
        elif self.audio_format.bytes_per_sample == 4:
            offset &= 0xfffffffc

        self._file.seek(offset + self._start_offset)
        self._offset = offset
