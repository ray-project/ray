"""Sources for media playback."""

# Collect public interface
from .loader import load, have_avbin
from .base import AudioFormat, VideoFormat, AudioData, SourceInfo
from .base import Source, StreamingSource, StaticSource, SourceGroup

# help the docs figure out where these are supposed to live (they live here)
__all__ = [
  'load',
  'have_avbin',
  'AudioFormat',
  'VideoFormat',
  'AudioData',
  'SourceInfo',
  'Source',
  'StreamingSource',
  'StaticSource',
  'SourceGroup',
]
