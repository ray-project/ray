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

"""Audio and video playback.

pyglet can play WAV files, and if AVbin is installed, many other audio and
video formats.

Playback is handled by the :py:class:`Player` class, which reads raw data from
:py:class:`Source` objects and provides methods for pausing, seeking, adjusting
the volume, and so on. The :py:class:`Player` class implements the best
available audio device (currently, only OpenAL is supported)::

    player = Player()

A :py:class:`Source` is used to decode arbitrary audio and video files.  It is
associated with a single player by "queuing" it::

    source = load('background_music.mp3')
    player.queue(source)

Use the :py:class:`Player` to control playback.

If the source contains video, the :py:meth:`Source.video_format` attribute will
be non-None, and the :py:attr:`Player.texture` attribute will contain the
current video image synchronised to the audio.

Decoding sounds can be processor-intensive and may introduce latency,
particularly for short sounds that must be played quickly, such as bullets or
explosions.  You can force such sounds to be decoded and retained in memory
rather than streamed from disk by wrapping the source in a
:py:class:`StaticSource`::

    bullet_sound = StaticSource(load('bullet.wav'))

The other advantage of a :py:class:`StaticSource` is that it can be queued on
any number of players, and so played many times simultaneously.

pyglet relies on Python's garbage collector to release resources when a player
has finished playing a source. In this way some operations that could affect
the application performance can be delayed.

The player provides a :py:meth:`Player.delete` method that can be used to
release resources immediately. Also an explicit call to ``gc.collect()`` can be
used to collect unused resources.
"""

# Collect public interface from all submodules/packages
from .drivers import get_audio_driver
from .exceptions import *
from .player import Player, PlayerGroup
from .sources import *

# For backwards compatibility, deprecate?
from .sources import procedural

