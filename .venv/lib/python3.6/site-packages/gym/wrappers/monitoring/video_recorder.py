import json
import os
import subprocess
import tempfile
import os.path
import distutils.spawn, distutils.version
import numpy as np
from six import StringIO
import six
from gym import error, logger

def touch(path):
    open(path, 'a').close()

class VideoRecorder(object):
    """VideoRecorder renders a nice movie of a rollout, frame by frame. It
    comes with an `enabled` option so you can still use the same code
    on episodes where you don't want to record video.

    Note:
        You are responsible for calling `close` on a created
        VideoRecorder, or else you may leak an encoder process.

    Args:
        env (Env): Environment to take video of.
        path (Optional[str]): Path to the video file; will be randomly chosen if omitted.
        base_path (Optional[str]): Alternatively, path to the video file without extension, which will be added.
        metadata (Optional[dict]): Contents to save to the metadata file.
        enabled (bool): Whether to actually record video, or just no-op (for convenience)
    """

    def __init__(self, env, path=None, metadata=None, enabled=True, base_path=None):
        modes = env.metadata.get('render.modes', [])
        self._async = env.metadata.get('semantics.async')
        self.enabled = enabled

        # Don't bother setting anything else if not enabled
        if not self.enabled:
            return

        self.ansi_mode = False
        if 'rgb_array' not in modes:
            if 'ansi' in modes:
                self.ansi_mode = True
            else:
                logger.info('Disabling video recorder because {} neither supports video mode "rgb_array" nor "ansi".'.format(env))
                # Whoops, turns out we shouldn't be enabled after all
                self.enabled = False
                return

        if path is not None and base_path is not None:
            raise error.Error("You can pass at most one of `path` or `base_path`.")

        self.last_frame = None
        self.env = env

        required_ext = '.json' if self.ansi_mode else '.mp4'
        if path is None:
            if base_path is not None:
                # Base path given, append ext
                path = base_path + required_ext
            else:
                # Otherwise, just generate a unique filename
                with tempfile.NamedTemporaryFile(suffix=required_ext, delete=False) as f:
                    path = f.name
        self.path = path

        path_base, actual_ext = os.path.splitext(self.path)

        if actual_ext != required_ext:
            hint = " HINT: The environment is text-only, therefore we're recording its text output in a structured JSON format." if self.ansi_mode else ''
            raise error.Error("Invalid path given: {} -- must have file extension {}.{}".format(self.path, required_ext, hint))
        # Touch the file in any case, so we know it's present. (This
        # corrects for platform platform differences. Using ffmpeg on
        # OS X, the file is precreated, but not on Linux.
        touch(path)

        self.frames_per_sec = env.metadata.get('video.frames_per_second', 30)
        self.encoder = None # lazily start the process
        self.broken = False

        # Dump metadata
        self.metadata = metadata or {}
        self.metadata['content_type'] = 'video/vnd.openai.ansivid' if self.ansi_mode else 'video/mp4'
        self.metadata_path = '{}.meta.json'.format(path_base)
        self.write_metadata()

        logger.info('Starting new video recorder writing to %s', self.path)
        self.empty = True

    @property
    def functional(self):
        return self.enabled and not self.broken

    def capture_frame(self):
        """Render the given `env` and add the resulting frame to the video."""
        if not self.functional: return
        logger.debug('Capturing video frame: path=%s', self.path)

        render_mode = 'ansi' if self.ansi_mode else 'rgb_array'
        frame = self.env.render(mode=render_mode)

        if frame is None:
            if self._async:
                return
            else:
                # Indicates a bug in the environment: don't want to raise
                # an error here.
                logger.warn('Env returned None on render(). Disabling further rendering for video recorder by marking as disabled: path=%s metadata_path=%s', self.path, self.metadata_path)
                self.broken = True
        else:
            self.last_frame = frame
            if self.ansi_mode:
                self._encode_ansi_frame(frame)
            else:
                self._encode_image_frame(frame)

    def close(self):
        """Make sure to manually close, or else you'll leak the encoder process"""
        if not self.enabled:
            return

        if self.encoder:
            logger.debug('Closing video encoder: path=%s', self.path)
            self.encoder.close()
            self.encoder = None
        else:
            # No frames captured. Set metadata, and remove the empty output file.
            os.remove(self.path)

            if self.metadata is None:
                self.metadata = {}
            self.metadata['empty'] = True

        # If broken, get rid of the output file, otherwise we'd leak it.
        if self.broken:
            logger.info('Cleaning up paths for broken video recorder: path=%s metadata_path=%s', self.path, self.metadata_path)

            # Might have crashed before even starting the output file, don't try to remove in that case.
            if os.path.exists(self.path):
                os.remove(self.path)

            if self.metadata is None:
                self.metadata = {}
            self.metadata['broken'] = True

        self.write_metadata()

    def write_metadata(self):
        with open(self.metadata_path, 'w') as f:
            json.dump(self.metadata, f)

    def _encode_ansi_frame(self, frame):
        if not self.encoder:
            self.encoder = TextEncoder(self.path, self.frames_per_sec)
            self.metadata['encoder_version'] = self.encoder.version_info
        self.encoder.capture_frame(frame)
        self.empty = False

    def _encode_image_frame(self, frame):
        if not self.encoder:
            self.encoder = ImageEncoder(self.path, frame.shape, self.frames_per_sec)
            self.metadata['encoder_version'] = self.encoder.version_info

        try:
            self.encoder.capture_frame(frame)
        except error.InvalidFrame as e:
            logger.warn('Tried to pass invalid video frame, marking as broken: %s', e)
            self.broken = True
        else:
            self.empty = False


class TextEncoder(object):
    """Store a moving picture made out of ANSI frames. Format adapted from
    https://github.com/asciinema/asciinema/blob/master/doc/asciicast-v1.md"""

    def __init__(self, output_path, frames_per_sec):
        self.output_path = output_path
        self.frames_per_sec = frames_per_sec
        self.frames = []

    def capture_frame(self, frame):
        from six import string_types
        string = None
        if isinstance(frame, string_types):
            string = frame
        elif isinstance(frame, StringIO):
            string = frame.getvalue()
        else:
            raise error.InvalidFrame('Wrong type {} for {}: text frame must be a string or StringIO'.format(type(frame), frame))

        frame_bytes = string.encode('utf-8')

        if frame_bytes[-1:] != six.b('\n'):
            raise error.InvalidFrame('Frame must end with a newline: """{}"""'.format(string))

        if six.b('\r') in frame_bytes:
            raise error.InvalidFrame('Frame contains carriage returns (only newlines are allowed: """{}"""'.format(string))

        self.frames.append(frame_bytes)

    def close(self):
        #frame_duration = float(1) / self.frames_per_sec
        frame_duration = .5

        # Turn frames into events: clear screen beforehand
        # https://rosettacode.org/wiki/Terminal_control/Clear_the_screen#Python
        # https://rosettacode.org/wiki/Terminal_control/Cursor_positioning#Python
        clear_code = six.b("%c[2J\033[1;1H" % (27))
        # Decode the bytes as UTF-8 since JSON may only contain UTF-8
        events = [ (frame_duration, (clear_code+frame.replace(six.b('\n'),six.b('\r\n'))).decode('utf-8'))  for frame in self.frames ]

        # Calculate frame size from the largest frames.
        # Add some padding since we'll get cut off otherwise.
        height = max([frame.count(six.b('\n')) for frame in self.frames]) + 1
        width = max([max([len(line) for line in frame.split(six.b('\n'))]) for frame in self.frames]) + 2

        data = {
            "version": 1,
            "width": width,
            "height": height,
            "duration": len(self.frames)*frame_duration,
            "command": "-",
            "title": "gym VideoRecorder episode",
            "env": {}, # could add some env metadata here
            "stdout": events,
        }

        with open(self.output_path, 'w') as f:
            json.dump(data, f)

    @property
    def version_info(self):
        return {'backend':'TextEncoder','version':1}

class ImageEncoder(object):
    def __init__(self, output_path, frame_shape, frames_per_sec):
        self.proc = None
        self.output_path = output_path
        # Frame shape should be lines-first, so w and h are swapped
        h, w, pixfmt = frame_shape
        if pixfmt != 3 and pixfmt != 4:
            raise error.InvalidFrame("Your frame has shape {}, but we require (w,h,3) or (w,h,4), i.e., RGB values for a w-by-h image, with an optional alpha channel.".format(frame_shape))
        self.wh = (w,h)
        self.includes_alpha = (pixfmt == 4)
        self.frame_shape = frame_shape
        self.frames_per_sec = frames_per_sec

        if distutils.spawn.find_executable('avconv') is not None:
            self.backend = 'avconv'
        elif distutils.spawn.find_executable('ffmpeg') is not None:
            self.backend = 'ffmpeg'
        else:
            raise error.DependencyNotInstalled("""Found neither the ffmpeg nor avconv executables. On OS X, you can install ffmpeg via `brew install ffmpeg`. On most Ubuntu variants, `sudo apt-get install ffmpeg` should do it. On Ubuntu 14.04, however, you'll need to install avconv with `sudo apt-get install libav-tools`.""")

        self.start()

    @property
    def version_info(self):
        return {
            'backend':self.backend,
            'version':str(subprocess.check_output([self.backend, '-version'],
                                                  stderr=subprocess.STDOUT)),
            'cmdline':self.cmdline
        }

    def start(self):
        self.cmdline = (self.backend,
                     '-nostats',
                     '-loglevel', 'error', # suppress warnings
                     '-y',
                     '-r', '%d' % self.frames_per_sec,

                     # input
                     '-f', 'rawvideo',
                     '-s:v', '{}x{}'.format(*self.wh),
                     '-pix_fmt',('rgb32' if self.includes_alpha else 'rgb24'),
                     '-i', '-', # this used to be /dev/stdin, which is not Windows-friendly

                     # output
                     '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
                     '-vcodec', 'libx264',
                     '-pix_fmt', 'yuv420p',
                     self.output_path
                     )

        logger.debug('Starting ffmpeg with "%s"', ' '.join(self.cmdline))
        if hasattr(os,'setsid'): #setsid not present on Windows
            self.proc = subprocess.Popen(self.cmdline, stdin=subprocess.PIPE, preexec_fn=os.setsid)
        else:
            self.proc = subprocess.Popen(self.cmdline, stdin=subprocess.PIPE)

    def capture_frame(self, frame):
        if not isinstance(frame, (np.ndarray, np.generic)):
            raise error.InvalidFrame('Wrong type {} for {} (must be np.ndarray or np.generic)'.format(type(frame), frame))
        if frame.shape != self.frame_shape:
            raise error.InvalidFrame("Your frame has shape {}, but the VideoRecorder is configured for shape {}.".format(frame.shape, self.frame_shape))
        if frame.dtype != np.uint8:
            raise error.InvalidFrame("Your frame has data type {}, but we require uint8 (i.e. RGB values from 0-255).".format(frame.dtype))

        if distutils.version.LooseVersion(np.__version__) >= distutils.version.LooseVersion('1.9.0'):
            self.proc.stdin.write(frame.tobytes())
        else:
            self.proc.stdin.write(frame.tostring())

    def close(self):
        self.proc.stdin.close()
        ret = self.proc.wait()
        if ret != 0:
            logger.error("VideoRecorder encoder exited with status {}".format(ret))
