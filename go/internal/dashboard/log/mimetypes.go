// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"path"
	"strings"
)

// contentTypes maps a lowercase file extension to the Content-Type aiohttp's
// static file handler reports for it. Python resolves the type through the
// mimetypes module; the entries aiohttp can produce for log-browser files are
// reproduced here so the Go agent serves the same headers without a dependency.
// The values follow Python 3.12+, the oldest interpreter Ray supports, where
// JavaScript maps to text/javascript (older Pythons used application/javascript).
var contentTypes = map[string]string{
	".3g2":         "audio/3gpp2",
	".3gp":         "audio/3gpp",
	".3gpp":        "audio/3gpp",
	".3gpp2":       "audio/3gpp2",
	".a":           "application/octet-stream",
	".aac":         "audio/aac",
	".adts":        "audio/aac",
	".ai":          "application/postscript",
	".aif":         "audio/x-aiff",
	".aifc":        "audio/x-aiff",
	".aiff":        "audio/x-aiff",
	".ass":         "audio/aac",
	".au":          "audio/basic",
	".avi":         "video/x-msvideo",
	".avif":        "image/avif",
	".bat":         "text/plain",
	".bcpio":       "application/x-bcpio",
	".bin":         "application/octet-stream",
	".bmp":         "image/bmp",
	".br":          "application/x-brotli",
	".bz2":         "application/x-bzip2",
	".c":           "text/plain",
	".cdf":         "application/x-netcdf",
	".cpio":        "application/x-cpio",
	".csh":         "application/x-csh",
	".css":         "text/css",
	".csv":         "text/csv",
	".dll":         "application/octet-stream",
	".doc":         "application/msword",
	".dot":         "application/msword",
	".dvi":         "application/x-dvi",
	".eml":         "message/rfc822",
	".eps":         "application/postscript",
	".etx":         "text/x-setext",
	".exe":         "application/octet-stream",
	".gif":         "image/gif",
	".gtar":        "application/x-gtar",
	".gz":          "application/gzip",
	".h":           "text/plain",
	".h5":          "application/x-hdf5",
	".hdf":         "application/x-hdf",
	".heic":        "image/heic",
	".heif":        "image/heif",
	".htm":         "text/html",
	".html":        "text/html",
	".ico":         "image/vnd.microsoft.icon",
	".ief":         "image/ief",
	".jpe":         "image/jpeg",
	".jpeg":        "image/jpeg",
	".jpg":         "image/jpeg",
	".js":          "text/javascript",
	".json":        "application/json",
	".ksh":         "text/plain",
	".latex":       "application/x-latex",
	".loas":        "audio/aac",
	".m1v":         "video/mpeg",
	".m3u":         "application/vnd.apple.mpegurl",
	".m3u8":        "application/vnd.apple.mpegurl",
	".man":         "application/x-troff-man",
	".me":          "application/x-troff-me",
	".mht":         "message/rfc822",
	".mhtml":       "message/rfc822",
	".mif":         "application/x-mif",
	".mjs":         "text/javascript",
	".mov":         "video/quicktime",
	".movie":       "video/x-sgi-movie",
	".mp2":         "audio/mpeg",
	".mp3":         "audio/mpeg",
	".mp4":         "video/mp4",
	".mpa":         "video/mpeg",
	".mpe":         "video/mpeg",
	".mpeg":        "video/mpeg",
	".mpg":         "video/mpeg",
	".ms":          "application/x-troff-ms",
	".n3":          "text/n3",
	".nc":          "application/x-netcdf",
	".nq":          "application/n-quads",
	".nt":          "application/n-triples",
	".nws":         "message/rfc822",
	".o":           "application/octet-stream",
	".obj":         "application/octet-stream",
	".oda":         "application/oda",
	".opus":        "audio/opus",
	".p12":         "application/x-pkcs12",
	".p7c":         "application/pkcs7-mime",
	".pbm":         "image/x-portable-bitmap",
	".pdf":         "application/pdf",
	".pfx":         "application/x-pkcs12",
	".pgm":         "image/x-portable-graymap",
	".pl":          "text/plain",
	".png":         "image/png",
	".pnm":         "image/x-portable-anymap",
	".pot":         "application/vnd.ms-powerpoint",
	".ppa":         "application/vnd.ms-powerpoint",
	".ppm":         "image/x-portable-pixmap",
	".pps":         "application/vnd.ms-powerpoint",
	".ppt":         "application/vnd.ms-powerpoint",
	".ps":          "application/postscript",
	".pwz":         "application/vnd.ms-powerpoint",
	".py":          "text/x-python",
	".pyc":         "application/x-python-code",
	".pyo":         "application/x-python-code",
	".qt":          "video/quicktime",
	".ra":          "audio/x-pn-realaudio",
	".ram":         "application/x-pn-realaudio",
	".ras":         "image/x-cmu-raster",
	".rdf":         "application/xml",
	".rgb":         "image/x-rgb",
	".roff":        "application/x-troff",
	".rtx":         "text/richtext",
	".sgm":         "text/x-sgml",
	".sgml":        "text/x-sgml",
	".sh":          "application/x-sh",
	".shar":        "application/x-shar",
	".snd":         "audio/basic",
	".so":          "application/octet-stream",
	".src":         "application/x-wais-source",
	".srt":         "text/plain",
	".sv4cpio":     "application/x-sv4cpio",
	".sv4crc":      "application/x-sv4crc",
	".svg":         "image/svg+xml",
	".swf":         "application/x-shockwave-flash",
	".t":           "application/x-troff",
	".tar":         "application/x-tar",
	".tcl":         "application/x-tcl",
	".tex":         "application/x-tex",
	".texi":        "application/x-texinfo",
	".texinfo":     "application/x-texinfo",
	".tif":         "image/tiff",
	".tiff":        "image/tiff",
	".tr":          "application/x-troff",
	".trig":        "application/trig",
	".tsv":         "text/tab-separated-values",
	".txt":         "text/plain",
	".ustar":       "application/x-ustar",
	".vcf":         "text/x-vcard",
	".vtt":         "text/vtt",
	".wasm":        "application/wasm",
	".wav":         "audio/x-wav",
	".webm":        "video/webm",
	".webmanifest": "application/manifest+json",
	".wiz":         "application/msword",
	".wsdl":        "application/xml",
	".xbm":         "image/x-xbitmap",
	".xlb":         "application/vnd.ms-excel",
	".xls":         "application/vnd.ms-excel",
	".xml":         "text/xml",
	".xpdl":        "application/xml",
	".xpm":         "image/x-xpixmap",
	".xsl":         "application/xml",
	".xwd":         "image/x-xwindowdump",
	".xz":          "application/x-xz",
	".zip":         "application/zip",
}

// fallbackContentType is what aiohttp reports when no extension mapping
// exists.
const fallbackContentType = "application/octet-stream"

// contentTypeFor returns the Content-Type for a file name, matching the Python
// mimetypes lookup aiohttp performs for static responses.
func contentTypeFor(name string) string {
	ext := strings.ToLower(path.Ext(name))
	if ct, ok := contentTypes[ext]; ok {
		return ct
	}
	return fallbackContentType
}
