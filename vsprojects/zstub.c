#ifdef __cplusplus
extern "C" {
#endif
// We don't need zlib, and this stub is to allow us to avoid compiling it as a dependency
int deflate(struct z_stream_s *strm, int flush) { return -2; }
int deflateEnd(struct z_stream_s *strm) { return -2; }
int deflateInit_(struct z_stream_s *stream, int level, const char *version, int stream_size) { return -2; }
int deflateInit2_(struct z_stream_s *strm, int  level, int  method, int windowBits, int memLevel, int strategy, const char *version, int stream_size) { return -2; }
int inflate(struct z_stream_s *strm, int flush) { return -2; }
int inflateEnd(struct z_stream_s *strm) { return -2; }
int inflateInit_(struct z_stream_s *stream, const char *version, int stream_size) { return -2; }
int inflateInit2_(struct z_stream_s *strm, int  windowBits, const char *version, int stream_size) { return -2; }
const char *gzerror(struct gzFile_s * file, int *errnum) { return 0; }
const char *zError(int err) { return 0; }
#ifdef __cplusplus
}
#endif
