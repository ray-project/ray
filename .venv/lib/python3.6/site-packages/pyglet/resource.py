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

"""Load application resources from a known path.

Loading resources by specifying relative paths to filenames is often
problematic in Python, as the working directory is not necessarily the same
directory as the application's script files.

This module allows applications to specify a search path for resources.
Relative paths are taken to be relative to the application's ``__main__``
module. ZIP files can appear on the path; they will be searched inside.  The
resource module also behaves as expected when applications are bundled using py2exe or py2app.

As well as providing file references (with the :py:func:`file` function), the
resource module also contains convenience functions for loading images,
textures, fonts, media and documents.

3rd party modules or packages not bound to a specific application should
construct their own :py:class:`Loader` instance and override the path to use the
resources in the module's directory.

Path format
^^^^^^^^^^^

The resource path :py:attr:`path` (see also :py:meth:`Loader.__init__` and
:py:meth:`Loader.path`)
is a list of locations to search for resources.  Locations are searched in the
order given in the path.  If a location is not valid (for example, if the
directory does not exist), it is skipped.

Locations in the path beginning with an ampersand (''@'' symbol) specify
Python packages.  Other locations specify a ZIP archive or directory on the
filesystem.  Locations that are not absolute are assumed to be relative to the
script home.  Some examples::

    # Search just the `res` directory, assumed to be located alongside the
    # main script file.
    path = ['res']

    # Search the directory containing the module `levels.level1`, followed
    # by the `res/images` directory.
    path = ['@levels.level1', 'res/images']

Paths are always case-sensitive and forward slashes are always used as path
separators, even in cases when the filesystem or platform does not do this.
This avoids a common programmer error when porting applications between
platforms.

The default path is ``['.']``.  If you modify the path, you must call
:py:func:`reindex`.

.. versionadded:: 1.1
"""
from future import standard_library

standard_library.install_aliases()
from builtins import object
from past.builtins import basestring

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

import os
import weakref
import sys
import zipfile

import pyglet
from pyglet.compat import BytesIO


class ResourceNotFoundException(Exception):
    """The named resource was not found on the search path."""

    def __init__(self, name):
        message = ('Resource "%s" was not found on the path.  '
                   'Ensure that the filename has the correct captialisation.') % name
        Exception.__init__(self, message)


def get_script_home():
    """Get the directory containing the program entry module.

    For ordinary Python scripts, this is the directory containing the
    ``__main__`` module.  For executables created with py2exe the result is
    the directory containing the running executable file.  For OS X bundles
    created using Py2App the result is the Resources directory within the
    running bundle.

    If none of the above cases apply and the file for ``__main__`` cannot
    be determined the working directory is returned.

    When the script is being run by a Python profiler, this function
    may return the directory where the profiler is running instead of
    the directory of the real script. To workaround this behaviour the
    full path to the real script can be specified in :py:attr:`pyglet.resource.path`.

    :rtype: str
    """
    frozen = getattr(sys, 'frozen', None)
    meipass = getattr(sys, '_MEIPASS', None)
    if meipass:
        # PyInstaller
        return meipass
    elif frozen in ('windows_exe', 'console_exe'):
        return os.path.dirname(sys.executable)
    elif frozen == 'macosx_app':
        # py2app
        return os.environ['RESOURCEPATH']
    else:
        main = sys.modules['__main__']
        if hasattr(main, '__file__'):
            return os.path.dirname(os.path.abspath(main.__file__))
        else:
            if 'python' in os.path.basename(sys.executable):
                # interactive
                return os.getcwd()
            else:
                # cx_Freeze
                return os.path.dirname(sys.executable)


def get_settings_path(name):
    """Get a directory to save user preferences.

    Different platforms have different conventions for where to save user
    preferences, saved games, and settings.  This function implements those
    conventions.  Note that the returned path may not exist: applications
    should use ``os.makedirs`` to construct it if desired.

    On Linux, a directory `name` in the user's configuration directory is
    returned (usually under ``~/.config``).

    On Windows (including under Cygwin) the `name` directory in the user's
    ``Application Settings`` directory is returned.

    On Mac OS X the `name` directory under ``~/Library/Application Support``
    is returned.

    :Parameters:
        `name` : str
            The name of the application.

    :rtype: str
    """

    if pyglet.compat_platform in ('cygwin', 'win32'):
        if 'APPDATA' in os.environ:
            return os.path.join(os.environ['APPDATA'], name)
        else:
            return os.path.expanduser('~/%s' % name)
    elif pyglet.compat_platform == 'darwin':
        return os.path.expanduser('~/Library/Application Support/%s' % name)
    elif pyglet.compat_platform.startswith('linux'):
        if 'XDG_CONFIG_HOME' in os.environ:
            return os.path.join(os.environ['XDG_CONFIG_HOME'], name)
        else:
            return os.path.expanduser('~/.config/%s' % name)
    else:
        return os.path.expanduser('~/.%s' % name)


class Location(object):
    """Abstract resource location.

    Given a location, a file can be loaded from that location with the `open`
    method.  This provides a convenient way to specify a path to load files
    from, and not necessarily have that path reside on the filesystem.
    """

    def open(self, filename, mode='rb'):
        """Open a file at this location.

        :Parameters:
            `filename` : str
                The filename to open.  Absolute paths are not supported.
                Relative paths are not supported by most locations (you
                should specify only a filename with no path component).
            `mode` : str
                The file mode to open with.  Only files opened on the
                filesystem make use of this parameter; others ignore it.

        :rtype: file object
        """
        raise NotImplementedError('abstract')


class FileLocation(Location):
    """Location on the filesystem.
    """

    def __init__(self, path):
        """Create a location given a relative or absolute path.

        :Parameters:
            `path` : str
                Path on the filesystem.
        """
        self.path = path

    def open(self, filename, mode='rb'):
        return open(os.path.join(self.path, filename), mode)


class ZIPLocation(Location):
    """Location within a ZIP file.
    """

    def __init__(self, zip, dir):
        """Create a location given an open ZIP file and a path within that
        file.

        :Parameters:
            `zip` : ``zipfile.ZipFile``
                An open ZIP file from the ``zipfile`` module.
            `dir` : str
                A path within that ZIP file.  Can be empty to specify files at
                the top level of the ZIP file.

        """
        self.zip = zip
        self.dir = dir

    def open(self, filename, mode='rb'):
        if self.dir:
            path = self.dir + '/' + filename
        else:
            path = filename
        text = self.zip.read(path)
        return BytesIO(text)


class URLLocation(Location):
    """Location on the network.

    This class uses the ``urlparse`` and ``urllib2`` modules to open files on
    the network given a URL.
    """

    def __init__(self, base_url):
        """Create a location given a base URL.

        :Parameters:
            `base_url` : str
                URL string to prepend to filenames.

        """
        self.base = base_url

    def open(self, filename, mode='rb'):
        import urllib.request, urllib.error, urllib.parse
        url = urllib.parse.urljoin(self.base, filename)
        return urllib.request.urlopen(url)


class Loader(object):
    """Load program resource files from disk.

    The loader contains a search path which can include filesystem
    directories, ZIP archives and Python packages.

    :Ivariables:
        `path` : list of str
            List of search locations.  After modifying the path you must
            call the `reindex` method.
        `script_home` : str
            Base resource location, defaulting to the location of the
            application script.

    """
    def __init__(self, path=None, script_home=None):
        """Create a loader for the given path.

        If no path is specified it defaults to ``['.']``; that is, just the
        program directory.

        See the module documentation for details on the path format.

        :Parameters:
            `path` : list of str
                List of locations to search for resources.
            `script_home` : str
                Base location of relative files.  Defaults to the result of
                `get_script_home`.

        """
        if path is None:
            path = ['.']
        if isinstance(path, basestring):
            path = [path]
        self.path = list(path)
        if script_home is None:
            script_home = get_script_home()
        self._script_home = script_home
        self._index = None

        # Map bin size to list of atlases
        self._texture_atlas_bins = {}

    def _require_index(self):
        if self._index is None:
            self.reindex()

    def reindex(self):
        """Refresh the file index.

        You must call this method if `path` is changed or the filesystem
        layout changes.
        """
        # map name to image etc.
        self._cached_textures = weakref.WeakValueDictionary()
        self._cached_images = weakref.WeakValueDictionary()
        self._cached_animations = weakref.WeakValueDictionary()

        self._index = {}
        for path in self.path:
            if path.startswith('@'):
                # Module
                name = path[1:]

                try:
                    module = __import__(name)
                except:
                    continue

                for component in name.split('.')[1:]:
                    module = getattr(module, component)

                if hasattr(module, '__file__'):
                    path = os.path.dirname(module.__file__)
                else:
                    path = ''  # interactive
            elif not os.path.isabs(path):
                # Add script base unless absolute
                assert '\\' not in path, \
                    'Backslashes not permitted in relative path'
                path = os.path.join(self._script_home, path)

            if os.path.isdir(path):
                # Filesystem directory
                path = path.rstrip(os.path.sep)
                location = FileLocation(path)
                for dirpath, dirnames, filenames in os.walk(path):
                    dirpath = dirpath[len(path) + 1:]
                    # Force forward slashes for index
                    if dirpath:
                        parts = [part
                                 for part
                                 in dirpath.split(os.sep)
                                 if part is not None]
                        dirpath = '/'.join(parts)
                    for filename in filenames:
                        if dirpath:
                            index_name = dirpath + '/' + filename
                        else:
                            index_name = filename
                        self._index_file(index_name, location)
            else:
                # Find path component that is the ZIP file.
                dir = ''
                old_path = None
                while path and not os.path.isfile(path):
                    old_path = path
                    path, tail_dir = os.path.split(path)
                    if path == old_path:
                        break
                    dir = '/'.join((tail_dir, dir))
                if path == old_path:
                    continue
                dir = dir.rstrip('/')

                # path is a ZIP file, dir resides within ZIP
                if path and zipfile.is_zipfile(path):
                    zip = zipfile.ZipFile(path, 'r')
                    location = ZIPLocation(zip, dir)
                    for zip_name in zip.namelist():
                        # zip_name_dir, zip_name = os.path.split(zip_name)
                        # assert '\\' not in name_dir
                        # assert not name_dir.endswith('/')
                        if zip_name.startswith(dir):
                            if dir:
                                zip_name = zip_name[len(dir) + 1:]
                            self._index_file(zip_name, location)

    def _index_file(self, name, location):
        if name not in self._index:
            self._index[name] = location

    def file(self, name, mode='rb'):
        """Load a resource.

        :Parameters:
            `name` : str
                Filename of the resource to load.
            `mode` : str
                Combination of ``r``, ``w``, ``a``, ``b`` and ``t`` characters
                with the meaning as for the builtin ``open`` function.

        :rtype: file object
        """
        self._require_index()
        try:
            location = self._index[name]
            return location.open(name, mode)
        except KeyError:
            raise ResourceNotFoundException(name)

    def location(self, name):
        """Get the location of a resource.

        This method is useful for opening files referenced from a resource.
        For example, an HTML file loaded as a resource might reference some
        images.  These images should be located relative to the HTML file, not
        looked up individually in the loader's path.

        :Parameters:
            `name` : str
                Filename of the resource to locate.

        :rtype: `Location`
        """
        self._require_index()
        try:
            return self._index[name]
        except KeyError:
            raise ResourceNotFoundException(name)

    def add_font(self, name):
        """Add a font resource to the application.

        Fonts not installed on the system must be added to pyglet before they
        can be used with `font.load`.  Although the font is added with
        its filename using this function, it is loaded by specifying its
        family name.  For example::

            resource.add_font('action_man.ttf')
            action_man = font.load('Action Man')

        :Parameters:
            `name` : str
                Filename of the font resource to add.

        """
        self._require_index()
        from pyglet import font
        file = self.file(name)
        font.add_file(file)

    def _alloc_image(self, name, atlas=True):
        file = self.file(name)
        try:
            img = pyglet.image.load(name, file=file)
        finally:
            file.close()

        if not atlas:
            return img.get_texture(True)

        # find an atlas suitable for the image
        bin = self._get_texture_atlas_bin(img.width, img.height)
        if bin is None:
            return img.get_texture(True)

        return bin.add(img)

    def _get_texture_atlas_bin(self, width, height):
        """A heuristic for determining the atlas bin to use for a given image
        size.  Returns None if the image should not be placed in an atlas (too
        big), otherwise the bin (a list of TextureAtlas).
        """
        # Large images are not placed in an atlas
        max_texture_size = pyglet.image.atlas.get_max_texture_size()
        max_size = min(1024, max_texture_size / 2)
        if width > max_size or height > max_size:
            return None

        # Group images with small height separately to larger height
        # (as the allocator can't stack within a single row).
        bin_size = 1
        if height > max_size / 4:
            bin_size = 2

        try:
            texture_bin = self._texture_atlas_bins[bin_size]
        except KeyError:
            texture_bin = self._texture_atlas_bins[bin_size] =\
                pyglet.image.atlas.TextureBin()

        return texture_bin

    def image(self, name, flip_x=False, flip_y=False, rotate=0, atlas=True):
        """Load an image with optional transformation.

        This is similar to `texture`, except the resulting image will be
        packed into a :py:class:`~pyglet.image.atlas.TextureBin` if it is an appropriate size for packing.
        This is more efficient than loading images into separate textures.

        :Parameters:
            `name` : str
                Filename of the image source to load.
            `flip_x` : bool
                If True, the returned image will be flipped horizontally.
            `flip_y` : bool
                If True, the returned image will be flipped vertically.
            `rotate` : int
                The returned image will be rotated clockwise by the given
                number of degrees (a multiple of 90).
            `atlas` : bool
                If True, the image will be loaded into an atlas managed by
                pyglet. If atlas loading is not appropriate for specific
                texturing reasons (e.g. border control is required) then set
                this argument to False.

        :rtype: `Texture`
        :return: A complete texture if the image is large or not in an atlas,
            otherwise a :py:class:`~pyglet.image.TextureRegion` of a texture atlas.
        """
        self._require_index()
        if name in self._cached_images:
            identity = self._cached_images[name]
        else:
            identity = self._cached_images[name] = self._alloc_image(name,
                                                                     atlas=atlas)

        if not rotate and not flip_x and not flip_y:
            return identity

        return identity.get_transform(flip_x, flip_y, rotate)

    def animation(self, name, flip_x=False, flip_y=False, rotate=0):
        """Load an animation with optional transformation.

        Animations loaded from the same source but with different
        transformations will use the same textures.

        :Parameters:
            `name` : str
                Filename of the animation source to load.
            `flip_x` : bool
                If True, the returned image will be flipped horizontally.
            `flip_y` : bool
                If True, the returned image will be flipped vertically.
            `rotate` : int
                The returned image will be rotated clockwise by the given
                number of degrees (a multiple of 90).

        :rtype: :py:class:`~pyglet.image.Animation`
        """
        self._require_index()
        try:
            identity = self._cached_animations[name]
        except KeyError:
            animation = pyglet.image.load_animation(name, self.file(name))
            bin = self._get_texture_atlas_bin(animation.get_max_width(),
                                              animation.get_max_height())
            if bin:
                animation.add_to_texture_bin(bin)

            identity = self._cached_animations[name] = animation

        if not rotate and not flip_x and not flip_y:
            return identity

        return identity.get_transform(flip_x, flip_y, rotate)

    def get_cached_image_names(self):
        """Get a list of image filenames that have been cached.

        This is useful for debugging and profiling only.

        :rtype: list
        :return: List of str
        """
        self._require_index()
        return list(self._cached_images.keys())

    def get_cached_animation_names(self):
        """Get a list of animation filenames that have been cached.

        This is useful for debugging and profiling only.

        :rtype: list
        :return: List of str
        """
        self._require_index()
        return list(self._cached_animations.keys())

    def get_texture_bins(self):
        """Get a list of texture bins in use.

        This is useful for debugging and profiling only.

        :rtype: list
        :return: List of :py:class:`~pyglet.image.atlas.TextureBin`
        """
        self._require_index()
        return list(self._texture_atlas_bins.values())

    def media(self, name, streaming=True):
        """Load a sound or video resource.

        The meaning of `streaming` is as for `media.load`.  Compressed
        sources cannot be streamed (that is, video and compressed audio
        cannot be streamed from a ZIP archive).

        :Parameters:
            `name` : str
                Filename of the media source to load.
            `streaming` : bool
                True if the source should be streamed from disk, False if
                it should be entirely decoded into memory immediately.

        :rtype: `media.Source`
        """
        self._require_index()
        from pyglet import media
        try:
            location = self._index[name]
            if isinstance(location, FileLocation):
                # Don't open the file if it's streamed from disk -- AVbin
                # needs to do it.
                path = os.path.join(location.path, name)
                return media.load(path, streaming=streaming)
            else:
                file = location.open(name)
                return media.load(name, file=file, streaming=streaming)
        except KeyError:
            raise ResourceNotFoundException(name)

    def texture(self, name):
        """Load a texture.

        The named image will be loaded as a single OpenGL texture.  If the
        dimensions of the image are not powers of 2 a :py:class:`~pyglet.image.TextureRegion` will
        be returned.

        :Parameters:
            `name` : str
                Filename of the image resource to load.

        :rtype: `Texture`
        """
        self._require_index()
        if name in self._cached_textures:
            return self._cached_textures[name]

        file = self.file(name)
        texture = pyglet.image.load(name, file=file).get_texture()
        self._cached_textures[name] = texture
        return texture

    def html(self, name):
        """Load an HTML document.

        :Parameters:
            `name` : str
                Filename of the HTML resource to load.

        :rtype: `FormattedDocument`
        """
        self._require_index()
        file = self.file(name)
        return pyglet.text.load(name, file, 'text/html')

    def attributed(self, name):
        """Load an attributed text document.

        See `pyglet.text.formats.attributed` for details on this format.

        :Parameters:
            `name` : str
                Filename of the attribute text resource to load.

        :rtype: `FormattedDocument`
        """
        self._require_index()
        file = self.file(name)
        return pyglet.text.load(name, file, 'text/vnd.pyglet-attributed')

    def text(self, name):
        """Load a plain text document.

        :Parameters:
            `name` : str
                Filename of the plain text resource to load.

        :rtype: `UnformattedDocument`
        """
        self._require_index()
        file = self.file(name)
        return pyglet.text.load(name, file, 'text/plain')

    def get_cached_texture_names(self):
        """Get the names of textures currently cached.

        :rtype: list of str
        """
        self._require_index()
        return list(self._cached_textures.keys())


#: Default resource search path.
#:
#: Locations in the search path are searched in order and are always
#: case-sensitive.  After changing the path you must call `reindex`.
#:
#: See the module documentation for details on the path format.
#:
#: :type: list of str
path = []


class _DefaultLoader(Loader):

    @property
    def path(self):
        return path

    @path.setter
    def path(self, value):
        global path
        path = value


_default_loader = _DefaultLoader()
reindex = _default_loader.reindex
file = _default_loader.file
location = _default_loader.location
add_font = _default_loader.add_font
image = _default_loader.image
animation = _default_loader.animation
get_cached_image_names = _default_loader.get_cached_image_names
get_cached_animation_names = _default_loader.get_cached_animation_names
get_texture_bins = _default_loader.get_texture_bins
media = _default_loader.media
texture = _default_loader.texture
html = _default_loader.html
attributed = _default_loader.attributed
text = _default_loader.text
get_cached_texture_names = _default_loader.get_cached_texture_names
