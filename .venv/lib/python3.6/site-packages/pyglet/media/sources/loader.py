from __future__ import print_function
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

import pyglet
from .base import StaticSource
from future.utils import with_metaclass

_debug = pyglet.options['debug_media']


def load(filename, file=None, streaming=True):
    """Load a source from a file.

    Currently the `file` argument is not supported; media files must exist
    as real paths.

    :Parameters:
        `filename` : str
            Filename of the media file to load.
        `file` : file-like object
            Not yet supported.
        `streaming` : bool
            If False, a :py:class:`~pyglet.media.StaticSource` will be returned; otherwise (default) a
            `StreamingSource` is created.

    :rtype: `Source`
    """
    source = get_source_loader().load(filename, file)
    if not streaming:
        source = StaticSource(source)
    return source


class AbstractSourceLoader(with_metaclass(ABCMeta, object)):
    @abstractmethod
    def load(self, filename, file):
        pass


class AVbinSourceLoader(AbstractSourceLoader):
    def load(self, filename, file):
        from .avbin import AVbinSource
        return AVbinSource(filename, file)


class RIFFSourceLoader(AbstractSourceLoader):
    def load(self, filename, file):
        from .riff import WaveSource
        return WaveSource(filename, file)


def get_source_loader():
    global _source_loader

    if _source_loader is not None:
        return _source_loader

    if have_avbin():
        _source_loader = AVbinSourceLoader()
        if _debug:
            print('AVbin available, using to load media files')
    else:
        _source_loader = RIFFSourceLoader()
        if _debug:
            print('AVbin not available. Only supporting wave files.')

    return _source_loader

_source_loader = None


def have_avbin():
    """Returns ``True`` iff AVBin is installed and accessible on the user's
    system.
    """
    global _have_avbin
    if _have_avbin is None:
        try:
            from .avbin import AVbinSource
            _have_avbin = True
        except ImportError:
            _have_avbin = False
    return _have_avbin

_have_avbin = None

