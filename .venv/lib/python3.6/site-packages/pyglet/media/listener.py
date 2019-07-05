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
from abc import ABCMeta, abstractmethod
from future.utils import with_metaclass


class AbstractListener(with_metaclass(ABCMeta, object)):
    """The listener properties for positional audio.

    You can obtain the singleton instance of this class by calling
    `AbstractAudioDriver.get_listener`.
    """

    _volume = 1.0
    _position = (0, 0, 0)
    _forward_orientation = (0, 0, -1)
    _up_orientation = (0, 1, 0)

    @abstractmethod
    def _set_volume(self, volume):
        pass

    volume = property(lambda self: self._volume,
                      lambda self, volume: self._set_volume(volume),
                      doc="""The master volume for sound playback.

        All sound volumes are multiplied by this master volume before being
        played.  A value of 0 will silence playback (but still consume
        resources).  The nominal volume is 1.0.

        :type: float
        """)

    @abstractmethod
    def _set_position(self, position):
        pass

    position = property(lambda self: self._position,
                        lambda self, position: self._set_position(position),
                        doc="""The position of the listener in 3D space.

        The position is given as a tuple of floats (x, y, z).  The unit
        defaults to meters, but can be modified with the listener
        properties.

        :type: 3-tuple of float
        """)

    @abstractmethod
    def _set_forward_orientation(self, orientation):
        pass

    forward_orientation = property(lambda self: self._forward_orientation,
                               lambda self, o: self._set_forward_orientation(o),
                               doc="""A vector giving the direction the
        listener is facing.

        The orientation is given as a tuple of floats (x, y, z), and has
        no unit.  The forward orientation should be orthagonal to the
        up orientation.

        :type: 3-tuple of float
        """)

    @abstractmethod
    def _set_up_orientation(self, orientation):
        pass

    up_orientation = property(lambda self: self._up_orientation,
                              lambda self, o: self._set_up_orientation(o),
                              doc="""A vector giving the "up" orientation
        of the listener.

        The orientation is given as a tuple of floats (x, y, z), and has
        no unit.  The up orientation should be orthagonal to the
        forward orientation.

        :type: 3-tuple of float
        """)

