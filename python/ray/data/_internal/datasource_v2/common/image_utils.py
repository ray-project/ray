"""Image decoding helpers shared by datasources.

These used to live in the legacy ``ray.data._internal.datasource.image_utils``
module. They moved here so that ``datasource_v2`` does not depend on the legacy
package; the legacy module imports them from here instead.
"""

# Allowlist of PIL plugins the reader is permitted to decode with. PIL selects a
# plugin by file signature, not by extension, so without this a payload named
# e.g. ``a.jpg`` can still be routed to a dangerous plugin (the EPS plugin shells
# out to Ghostscript). Restricting to these raster formats keeps untrusted bytes
# away from such parsers. Keep in sync with ``_FILE_EXTENSIONS``.
_ALLOWED_PIL_FORMATS = ["PNG", "JPEG", "TIFF", "BMP", "GIF"]
