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
from abc import ABCMeta, abstractmethod
from future.utils import with_metaclass


class AbstractAudioPlayer(with_metaclass(ABCMeta, object)):
    """Base class for driver audio players.
    """

    def __init__(self, source_group, player):
        """Create a new audio player.

        :Parameters:
            `source_group` : `SourceGroup`
                Source group to play from.
            `player` : `Player`
                Player to receive EOS and video frame sync events.

        """
        self.source_group = source_group
        self.player = player

    @abstractmethod
    def play(self):
        """Begin playback."""

    @abstractmethod
    def stop(self):
        """Stop (pause) playback."""

    @abstractmethod
    def delete(self):
        """Stop playing and clean up all resources used by player."""

    def _play_group(self, audio_players):
        """Begin simultaneous playback on a list of audio players."""
        # This should be overridden by subclasses for better synchrony.
        for player in audio_players:
            player.play()

    def _stop_group(self, audio_players):
        """Stop simultaneous playback on a list of audio players."""
        # This should be overridden by subclasses for better synchrony.
        for player in audio_players:
            player.stop()

    @abstractmethod
    def clear(self):
        """Clear all buffered data and prepare for replacement data.

        The player should be stopped before calling this method.
        """

    @abstractmethod
    def get_time(self):
        """Return approximation of current playback time within current source.

        Returns ``None`` if the audio player does not know what the playback
        time is (for example, before any valid audio data has been read).

        :rtype: float
        :return: current play cursor time, in seconds.
        """
        # TODO determine which source within group

    def set_volume(self, volume):
        """See `Player.volume`."""
        pass

    def set_position(self, position):
        """See :py:attr:`~pyglet.media.Player.position`."""
        pass

    def set_min_distance(self, min_distance):
        """See `Player.min_distance`."""
        pass

    def set_max_distance(self, max_distance):
        """See `Player.max_distance`."""
        pass

    def set_pitch(self, pitch):
        """See :py:attr:`~pyglet.media.Player.pitch`."""
        pass

    def set_cone_orientation(self, cone_orientation):
        """See `Player.cone_orientation`."""
        pass

    def set_cone_inner_angle(self, cone_inner_angle):
        """See `Player.cone_inner_angle`."""
        pass

    def set_cone_outer_angle(self, cone_outer_angle):
        """See `Player.cone_outer_angle`."""
        pass

    def set_cone_outer_gain(self, cone_outer_gain):
        """See `Player.cone_outer_gain`."""
        pass


class AbstractAudioDriver(with_metaclass(ABCMeta, object)):
    @abstractmethod
    def create_audio_player(self, source_group, player):
        pass

    @abstractmethod
    def get_listener(self):
        pass

    @abstractmethod
    def delete(self):
        pass
