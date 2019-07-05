'''Wrapper for pulse

Generated with:
tools/genwrappers.py pulseaudio

Do not modify this file.
'''

__docformat__ =  'restructuredtext'
__version__ = '$Id$'

import ctypes
from ctypes import *

import pyglet.lib

_lib = pyglet.lib.load_library('pulse')

_int_types = (c_int16, c_int32)
if hasattr(ctypes, 'c_int64'):
    # Some builds of ctypes apparently do not have c_int64
    # defined; it's a pretty good bet that these builds do not
    # have 64-bit pointers.
    _int_types += (ctypes.c_int64,)
for t in _int_types:
    if sizeof(t) == sizeof(c_size_t):
        c_ptrdiff_t = t

class c_void(Structure):
    # c_void_p is a buggy return type, converting to int, so
    # POINTER(None) == c_void_p is actually written as
    # POINTER(c_void), so it can be treated as a real pointer.
    _fields_ = [('dummy', c_int)]



# /usr/include/pulse/version.h:40
pa_get_library_version = _lib.pa_get_library_version
pa_get_library_version.restype = c_char_p
pa_get_library_version.argtypes = []

PA_API_VERSION = 12 	# /usr/include/pulse/version.h:46
PA_PROTOCOL_VERSION = 30 	# /usr/include/pulse/version.h:50
PA_MAJOR = 6 	# /usr/include/pulse/version.h:53
PA_MINOR = 0 	# /usr/include/pulse/version.h:56
PA_MICRO = 0 	# /usr/include/pulse/version.h:59
PA_CHANNELS_MAX = 32 	# /usr/include/pulse/sample.h:128
PA_RATE_MAX = 192000 	# /usr/include/pulse/sample.h:131
enum_pa_sample_format = c_int
PA_SAMPLE_U8 = 0
PA_SAMPLE_ALAW = 1
PA_SAMPLE_ULAW = 2
PA_SAMPLE_S16LE = 3
PA_SAMPLE_S16BE = 4
PA_SAMPLE_FLOAT32LE = 5
PA_SAMPLE_FLOAT32BE = 6
PA_SAMPLE_S32LE = 7
PA_SAMPLE_S32BE = 8
PA_SAMPLE_S24LE = 9
PA_SAMPLE_S24BE = 10
PA_SAMPLE_S24_32LE = 11
PA_SAMPLE_S24_32BE = 12
PA_SAMPLE_MAX = 13
PA_SAMPLE_INVALID = -1
pa_sample_format_t = enum_pa_sample_format 	# /usr/include/pulse/sample.h:179
class struct_pa_sample_spec(Structure):
    __slots__ = [
        'format',
        'rate',
        'channels',
    ]
struct_pa_sample_spec._fields_ = [
    ('format', pa_sample_format_t),
    ('rate', c_uint32),
    ('channels', c_uint8),
]

pa_sample_spec = struct_pa_sample_spec 	# /usr/include/pulse/sample.h:257
pa_usec_t = c_uint64 	# /usr/include/pulse/sample.h:260
# /usr/include/pulse/sample.h:263
pa_bytes_per_second = _lib.pa_bytes_per_second
pa_bytes_per_second.restype = c_size_t
pa_bytes_per_second.argtypes = [POINTER(pa_sample_spec)]

# /usr/include/pulse/sample.h:266
pa_frame_size = _lib.pa_frame_size
pa_frame_size.restype = c_size_t
pa_frame_size.argtypes = [POINTER(pa_sample_spec)]

# /usr/include/pulse/sample.h:269
pa_sample_size = _lib.pa_sample_size
pa_sample_size.restype = c_size_t
pa_sample_size.argtypes = [POINTER(pa_sample_spec)]

# /usr/include/pulse/sample.h:273
pa_sample_size_of_format = _lib.pa_sample_size_of_format
pa_sample_size_of_format.restype = c_size_t
pa_sample_size_of_format.argtypes = [pa_sample_format_t]

# /usr/include/pulse/sample.h:278
pa_bytes_to_usec = _lib.pa_bytes_to_usec
pa_bytes_to_usec.restype = pa_usec_t
pa_bytes_to_usec.argtypes = [c_uint64, POINTER(pa_sample_spec)]

# /usr/include/pulse/sample.h:283
pa_usec_to_bytes = _lib.pa_usec_to_bytes
pa_usec_to_bytes.restype = c_size_t
pa_usec_to_bytes.argtypes = [pa_usec_t, POINTER(pa_sample_spec)]

# /usr/include/pulse/sample.h:288
pa_sample_spec_init = _lib.pa_sample_spec_init
pa_sample_spec_init.restype = POINTER(pa_sample_spec)
pa_sample_spec_init.argtypes = [POINTER(pa_sample_spec)]

# /usr/include/pulse/sample.h:291
#pa_sample_format_valid = _lib.pa_sample_format_valid
#pa_sample_format_valid.restype = c_int
#pa_sample_format_valid.argtypes = [c_uint]

# /usr/include/pulse/sample.h:294
#pa_sample_rate_valid = _lib.pa_sample_rate_valid
#pa_sample_rate_valid.restype = c_int
#pa_sample_rate_valid.argtypes = [c_uint32]

# /usr/include/pulse/sample.h:298
#pa_channels_valid = _lib.pa_channels_valid
#pa_channels_valid.restype = c_int
#pa_channels_valid.argtypes = [c_uint8]

# /usr/include/pulse/sample.h:301
pa_sample_spec_valid = _lib.pa_sample_spec_valid
pa_sample_spec_valid.restype = c_int
pa_sample_spec_valid.argtypes = [POINTER(pa_sample_spec)]

# /usr/include/pulse/sample.h:304
pa_sample_spec_equal = _lib.pa_sample_spec_equal
pa_sample_spec_equal.restype = c_int
pa_sample_spec_equal.argtypes = [POINTER(pa_sample_spec), POINTER(pa_sample_spec)]

# /usr/include/pulse/sample.h:307
pa_sample_format_to_string = _lib.pa_sample_format_to_string
pa_sample_format_to_string.restype = c_char_p
pa_sample_format_to_string.argtypes = [pa_sample_format_t]

# /usr/include/pulse/sample.h:310
pa_parse_sample_format = _lib.pa_parse_sample_format
pa_parse_sample_format.restype = pa_sample_format_t
pa_parse_sample_format.argtypes = [c_char_p]

PA_SAMPLE_SPEC_SNPRINT_MAX = 32 	# /usr/include/pulse/sample.h:317
# /usr/include/pulse/sample.h:320
pa_sample_spec_snprint = _lib.pa_sample_spec_snprint
pa_sample_spec_snprint.restype = c_char_p
pa_sample_spec_snprint.argtypes = [c_char_p, c_size_t, POINTER(pa_sample_spec)]

PA_BYTES_SNPRINT_MAX = 11 	# /usr/include/pulse/sample.h:327
# /usr/include/pulse/sample.h:330
pa_bytes_snprint = _lib.pa_bytes_snprint
pa_bytes_snprint.restype = c_char_p
pa_bytes_snprint.argtypes = [c_char_p, c_size_t, c_uint]

# /usr/include/pulse/sample.h:334
pa_sample_format_is_le = _lib.pa_sample_format_is_le
pa_sample_format_is_le.restype = c_int
pa_sample_format_is_le.argtypes = [pa_sample_format_t]

# /usr/include/pulse/sample.h:338
pa_sample_format_is_be = _lib.pa_sample_format_is_be
pa_sample_format_is_be.restype = c_int
pa_sample_format_is_be.argtypes = [pa_sample_format_t]

enum_pa_context_state = c_int
PA_CONTEXT_UNCONNECTED = 0
PA_CONTEXT_CONNECTING = 1
PA_CONTEXT_AUTHORIZING = 2
PA_CONTEXT_SETTING_NAME = 3
PA_CONTEXT_READY = 4
PA_CONTEXT_FAILED = 5
PA_CONTEXT_TERMINATED = 6
pa_context_state_t = enum_pa_context_state 	# /usr/include/pulse/def.h:45
enum_pa_stream_state = c_int
PA_STREAM_UNCONNECTED = 0
PA_STREAM_CREATING = 1
PA_STREAM_READY = 2
PA_STREAM_FAILED = 3
PA_STREAM_TERMINATED = 4
pa_stream_state_t = enum_pa_stream_state 	# /usr/include/pulse/def.h:74
enum_pa_operation_state = c_int
PA_OPERATION_RUNNING = 0
PA_OPERATION_DONE = 1
PA_OPERATION_CANCELLED = 2
pa_operation_state_t = enum_pa_operation_state 	# /usr/include/pulse/def.h:102
enum_pa_context_flags = c_int
PA_CONTEXT_NOFLAGS = 0
PA_CONTEXT_NOAUTOSPAWN = 1
PA_CONTEXT_NOFAIL = 2
pa_context_flags_t = enum_pa_context_flags 	# /usr/include/pulse/def.h:122
enum_pa_direction = c_int
PA_DIRECTION_OUTPUT = 1
PA_DIRECTION_INPUT = 2
pa_direction_t = enum_pa_direction 	# /usr/include/pulse/def.h:137
enum_pa_device_type = c_int
PA_DEVICE_TYPE_SINK = 0
PA_DEVICE_TYPE_SOURCE = 1
pa_device_type_t = enum_pa_device_type 	# /usr/include/pulse/def.h:148
enum_pa_stream_direction = c_int
PA_STREAM_NODIRECTION = 0
PA_STREAM_PLAYBACK = 1
PA_STREAM_RECORD = 2
PA_STREAM_UPLOAD = 3
pa_stream_direction_t = enum_pa_stream_direction 	# /usr/include/pulse/def.h:161
enum_pa_stream_flags = c_int
PA_STREAM_NOFLAGS = 0
PA_STREAM_START_CORKED = 1
PA_STREAM_INTERPOLATE_TIMING = 2
PA_STREAM_NOT_MONOTONIC = 4
PA_STREAM_AUTO_TIMING_UPDATE = 8
PA_STREAM_NO_REMAP_CHANNELS = 16
PA_STREAM_NO_REMIX_CHANNELS = 32
PA_STREAM_FIX_FORMAT = 64
PA_STREAM_FIX_RATE = 128
PA_STREAM_FIX_CHANNELS = 256
PA_STREAM_DONT_MOVE = 512
PA_STREAM_VARIABLE_RATE = 1024
PA_STREAM_PEAK_DETECT = 2048
PA_STREAM_START_MUTED = 4096
PA_STREAM_ADJUST_LATENCY = 8192
PA_STREAM_EARLY_REQUESTS = 16384
PA_STREAM_DONT_INHIBIT_AUTO_SUSPEND = 32768
PA_STREAM_START_UNMUTED = 65536
PA_STREAM_FAIL_ON_SUSPEND = 131072
PA_STREAM_RELATIVE_VOLUME = 262144
PA_STREAM_PASSTHROUGH = 524288
pa_stream_flags_t = enum_pa_stream_flags 	# /usr/include/pulse/def.h:355
class struct_pa_buffer_attr(Structure):
    __slots__ = [
        'maxlength',
        'tlength',
        'prebuf',
        'minreq',
        'fragsize',
    ]
struct_pa_buffer_attr._fields_ = [
    ('maxlength', c_uint32),
    ('tlength', c_uint32),
    ('prebuf', c_uint32),
    ('minreq', c_uint32),
    ('fragsize', c_uint32),
]

pa_buffer_attr = struct_pa_buffer_attr 	# /usr/include/pulse/def.h:452
enum_pa_error_code = c_int
PA_OK = 0
PA_ERR_ACCESS = 1
PA_ERR_COMMAND = 2
PA_ERR_INVALID = 3
PA_ERR_EXIST = 4
PA_ERR_NOENTITY = 5
PA_ERR_CONNECTIONREFUSED = 6
PA_ERR_PROTOCOL = 7
PA_ERR_TIMEOUT = 8
PA_ERR_AUTHKEY = 9
PA_ERR_INTERNAL = 10
PA_ERR_CONNECTIONTERMINATED = 11
PA_ERR_KILLED = 12
PA_ERR_INVALIDSERVER = 13
PA_ERR_MODINITFAILED = 14
PA_ERR_BADSTATE = 15
PA_ERR_NODATA = 16
PA_ERR_VERSION = 17
PA_ERR_TOOLARGE = 18
PA_ERR_NOTSUPPORTED = 19
PA_ERR_UNKNOWN = 20
PA_ERR_NOEXTENSION = 21
PA_ERR_OBSOLETE = 22
PA_ERR_NOTIMPLEMENTED = 23
PA_ERR_FORKED = 24
PA_ERR_IO = 25
PA_ERR_BUSY = 26
PA_ERR_MAX = 27
pa_error_code_t = enum_pa_error_code 	# /usr/include/pulse/def.h:484
enum_pa_subscription_mask = c_int
PA_SUBSCRIPTION_MASK_NULL = 0
PA_SUBSCRIPTION_MASK_SINK = 1
PA_SUBSCRIPTION_MASK_SOURCE = 2
PA_SUBSCRIPTION_MASK_SINK_INPUT = 4
PA_SUBSCRIPTION_MASK_SOURCE_OUTPUT = 8
PA_SUBSCRIPTION_MASK_MODULE = 16
PA_SUBSCRIPTION_MASK_CLIENT = 32
PA_SUBSCRIPTION_MASK_SAMPLE_CACHE = 64
PA_SUBSCRIPTION_MASK_SERVER = 128
PA_SUBSCRIPTION_MASK_AUTOLOAD = 256
PA_SUBSCRIPTION_MASK_CARD = 512
PA_SUBSCRIPTION_MASK_ALL = 767
pa_subscription_mask_t = enum_pa_subscription_mask 	# /usr/include/pulse/def.h:554
enum_pa_subscription_event_type = c_int
PA_SUBSCRIPTION_EVENT_SINK = 0
PA_SUBSCRIPTION_EVENT_SOURCE = 1
PA_SUBSCRIPTION_EVENT_SINK_INPUT = 2
PA_SUBSCRIPTION_EVENT_SOURCE_OUTPUT = 3
PA_SUBSCRIPTION_EVENT_MODULE = 4
PA_SUBSCRIPTION_EVENT_CLIENT = 5
PA_SUBSCRIPTION_EVENT_SAMPLE_CACHE = 6
PA_SUBSCRIPTION_EVENT_SERVER = 7
PA_SUBSCRIPTION_EVENT_AUTOLOAD = 8
PA_SUBSCRIPTION_EVENT_CARD = 9
PA_SUBSCRIPTION_EVENT_FACILITY_MASK = 15
PA_SUBSCRIPTION_EVENT_NEW = 0
PA_SUBSCRIPTION_EVENT_CHANGE = 16
PA_SUBSCRIPTION_EVENT_REMOVE = 32
PA_SUBSCRIPTION_EVENT_TYPE_MASK = 48
pa_subscription_event_type_t = enum_pa_subscription_event_type 	# /usr/include/pulse/def.h:605
class struct_pa_timing_info(Structure):
    __slots__ = [
        'timestamp',
        'synchronized_clocks',
        'sink_usec',
        'source_usec',
        'transport_usec',
        'playing',
        'write_index_corrupt',
        'write_index',
        'read_index_corrupt',
        'read_index',
        'configured_sink_usec',
        'configured_source_usec',
        'since_underrun',
    ]
class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

struct_pa_timing_info._fields_ = [
    ('timestamp', struct_timeval),
    ('synchronized_clocks', c_int),
    ('sink_usec', pa_usec_t),
    ('source_usec', pa_usec_t),
    ('transport_usec', pa_usec_t),
    ('playing', c_int),
    ('write_index_corrupt', c_int),
    ('write_index', c_int64),
    ('read_index_corrupt', c_int),
    ('read_index', c_int64),
    ('configured_sink_usec', pa_usec_t),
    ('configured_source_usec', pa_usec_t),
    ('since_underrun', c_int64),
]

pa_timing_info = struct_pa_timing_info 	# /usr/include/pulse/def.h:725
class struct_pa_spawn_api(Structure):
    __slots__ = [
        'prefork',
        'postfork',
        'atfork',
    ]
struct_pa_spawn_api._fields_ = [
    ('prefork', POINTER(CFUNCTYPE(None))),
    ('postfork', POINTER(CFUNCTYPE(None))),
    ('atfork', POINTER(CFUNCTYPE(None))),
]

pa_spawn_api = struct_pa_spawn_api 	# /usr/include/pulse/def.h:749
enum_pa_seek_mode = c_int
PA_SEEK_RELATIVE = 0
PA_SEEK_ABSOLUTE = 1
PA_SEEK_RELATIVE_ON_READ = 2
PA_SEEK_RELATIVE_END = 3
pa_seek_mode_t = enum_pa_seek_mode 	# /usr/include/pulse/def.h:764
enum_pa_sink_flags = c_int
PA_SINK_NOFLAGS = 0
PA_SINK_HW_VOLUME_CTRL = 1
PA_SINK_LATENCY = 2
PA_SINK_HARDWARE = 4
PA_SINK_NETWORK = 8
PA_SINK_HW_MUTE_CTRL = 16
PA_SINK_DECIBEL_VOLUME = 32
PA_SINK_FLAT_VOLUME = 64
PA_SINK_DYNAMIC_LATENCY = 128
PA_SINK_SET_FORMATS = 256
pa_sink_flags_t = enum_pa_sink_flags 	# /usr/include/pulse/def.h:829
enum_pa_sink_state = c_int
PA_SINK_INVALID_STATE = -1
PA_SINK_RUNNING = 0
PA_SINK_IDLE = 1
PA_SINK_SUSPENDED = 2
PA_SINK_INIT = -2
PA_SINK_UNLINKED = -3
pa_sink_state_t = enum_pa_sink_state 	# /usr/include/pulse/def.h:875
enum_pa_source_flags = c_int
PA_SOURCE_NOFLAGS = 0
PA_SOURCE_HW_VOLUME_CTRL = 1
PA_SOURCE_LATENCY = 2
PA_SOURCE_HARDWARE = 4
PA_SOURCE_NETWORK = 8
PA_SOURCE_HW_MUTE_CTRL = 16
PA_SOURCE_DECIBEL_VOLUME = 32
PA_SOURCE_DYNAMIC_LATENCY = 64
PA_SOURCE_FLAT_VOLUME = 128
pa_source_flags_t = enum_pa_source_flags 	# /usr/include/pulse/def.h:946
enum_pa_source_state = c_int
PA_SOURCE_INVALID_STATE = -1
PA_SOURCE_RUNNING = 0
PA_SOURCE_IDLE = 1
PA_SOURCE_SUSPENDED = 2
PA_SOURCE_INIT = -2
PA_SOURCE_UNLINKED = -3
pa_source_state_t = enum_pa_source_state 	# /usr/include/pulse/def.h:991
pa_free_cb_t = CFUNCTYPE(None, POINTER(None)) 	# /usr/include/pulse/def.h:1014
enum_pa_port_available = c_int
PA_PORT_AVAILABLE_UNKNOWN = 0
PA_PORT_AVAILABLE_NO = 1
PA_PORT_AVAILABLE_YES = 2
pa_port_available_t = enum_pa_port_available 	# /usr/include/pulse/def.h:1040
class struct_pa_mainloop_api(Structure):
    __slots__ = [
    ]
struct_pa_mainloop_api._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_pa_mainloop_api(Structure):
    __slots__ = [
    ]
struct_pa_mainloop_api._fields_ = [
    ('_opaque_struct', c_int)
]

pa_mainloop_api = struct_pa_mainloop_api 	# /usr/include/pulse/mainloop-api.h:47
enum_pa_io_event_flags = c_int
PA_IO_EVENT_NULL = 0
PA_IO_EVENT_INPUT = 1
PA_IO_EVENT_OUTPUT = 2
PA_IO_EVENT_HANGUP = 4
PA_IO_EVENT_ERROR = 8
pa_io_event_flags_t = enum_pa_io_event_flags 	# /usr/include/pulse/mainloop-api.h:56
class struct_pa_io_event(Structure):
    __slots__ = [
    ]
struct_pa_io_event._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_pa_io_event(Structure):
    __slots__ = [
    ]
struct_pa_io_event._fields_ = [
    ('_opaque_struct', c_int)
]

pa_io_event = struct_pa_io_event 	# /usr/include/pulse/mainloop-api.h:59
pa_io_event_cb_t = CFUNCTYPE(None, POINTER(pa_mainloop_api), POINTER(pa_io_event), c_int, pa_io_event_flags_t, POINTER(None)) 	# /usr/include/pulse/mainloop-api.h:61
pa_io_event_destroy_cb_t = CFUNCTYPE(None, POINTER(pa_mainloop_api), POINTER(pa_io_event), POINTER(None)) 	# /usr/include/pulse/mainloop-api.h:63
class struct_pa_time_event(Structure):
    __slots__ = [
    ]
struct_pa_time_event._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_pa_time_event(Structure):
    __slots__ = [
    ]
struct_pa_time_event._fields_ = [
    ('_opaque_struct', c_int)
]

pa_time_event = struct_pa_time_event 	# /usr/include/pulse/mainloop-api.h:66
class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

pa_time_event_cb_t = CFUNCTYPE(None, POINTER(pa_mainloop_api), POINTER(pa_time_event), POINTER(struct_timeval), POINTER(None)) 	# /usr/include/pulse/mainloop-api.h:68
pa_time_event_destroy_cb_t = CFUNCTYPE(None, POINTER(pa_mainloop_api), POINTER(pa_time_event), POINTER(None)) 	# /usr/include/pulse/mainloop-api.h:70
class struct_pa_defer_event(Structure):
    __slots__ = [
    ]
struct_pa_defer_event._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_pa_defer_event(Structure):
    __slots__ = [
    ]
struct_pa_defer_event._fields_ = [
    ('_opaque_struct', c_int)
]

pa_defer_event = struct_pa_defer_event 	# /usr/include/pulse/mainloop-api.h:73
pa_defer_event_cb_t = CFUNCTYPE(None, POINTER(pa_mainloop_api), POINTER(pa_defer_event), POINTER(None)) 	# /usr/include/pulse/mainloop-api.h:75
pa_defer_event_destroy_cb_t = CFUNCTYPE(None, POINTER(pa_mainloop_api), POINTER(pa_defer_event), POINTER(None)) 	# /usr/include/pulse/mainloop-api.h:77
# /usr/include/pulse/mainloop-api.h:120
pa_mainloop_api_once = _lib.pa_mainloop_api_once
pa_mainloop_api_once.restype = None
pa_mainloop_api_once.argtypes = [POINTER(pa_mainloop_api), CFUNCTYPE(None, POINTER(pa_mainloop_api), POINTER(None)), POINTER(None)]

enum_pa_channel_position = c_int
PA_CHANNEL_POSITION_INVALID = -1
PA_CHANNEL_POSITION_MONO = 0
PA_CHANNEL_POSITION_FRONT_LEFT = 1
PA_CHANNEL_POSITION_FRONT_RIGHT = 2
PA_CHANNEL_POSITION_FRONT_CENTER = 3
PA_CHANNEL_POSITION_LEFT = 0
PA_CHANNEL_POSITION_RIGHT = 0
PA_CHANNEL_POSITION_CENTER = 0
PA_CHANNEL_POSITION_REAR_CENTER = 1
PA_CHANNEL_POSITION_REAR_LEFT = 2
PA_CHANNEL_POSITION_REAR_RIGHT = 3
PA_CHANNEL_POSITION_LFE = 4
PA_CHANNEL_POSITION_SUBWOOFER = 0
PA_CHANNEL_POSITION_FRONT_LEFT_OF_CENTER = 1
PA_CHANNEL_POSITION_FRONT_RIGHT_OF_CENTER = 2
PA_CHANNEL_POSITION_SIDE_LEFT = 3
PA_CHANNEL_POSITION_SIDE_RIGHT = 4
PA_CHANNEL_POSITION_AUX0 = 5
PA_CHANNEL_POSITION_AUX1 = 6
PA_CHANNEL_POSITION_AUX2 = 7
PA_CHANNEL_POSITION_AUX3 = 8
PA_CHANNEL_POSITION_AUX4 = 9
PA_CHANNEL_POSITION_AUX5 = 10
PA_CHANNEL_POSITION_AUX6 = 11
PA_CHANNEL_POSITION_AUX7 = 12
PA_CHANNEL_POSITION_AUX8 = 13
PA_CHANNEL_POSITION_AUX9 = 14
PA_CHANNEL_POSITION_AUX10 = 15
PA_CHANNEL_POSITION_AUX11 = 16
PA_CHANNEL_POSITION_AUX12 = 17
PA_CHANNEL_POSITION_AUX13 = 18
PA_CHANNEL_POSITION_AUX14 = 19
PA_CHANNEL_POSITION_AUX15 = 20
PA_CHANNEL_POSITION_AUX16 = 21
PA_CHANNEL_POSITION_AUX17 = 22
PA_CHANNEL_POSITION_AUX18 = 23
PA_CHANNEL_POSITION_AUX19 = 24
PA_CHANNEL_POSITION_AUX20 = 25
PA_CHANNEL_POSITION_AUX21 = 26
PA_CHANNEL_POSITION_AUX22 = 27
PA_CHANNEL_POSITION_AUX23 = 28
PA_CHANNEL_POSITION_AUX24 = 29
PA_CHANNEL_POSITION_AUX25 = 30
PA_CHANNEL_POSITION_AUX26 = 31
PA_CHANNEL_POSITION_AUX27 = 32
PA_CHANNEL_POSITION_AUX28 = 33
PA_CHANNEL_POSITION_AUX29 = 34
PA_CHANNEL_POSITION_AUX30 = 35
PA_CHANNEL_POSITION_AUX31 = 36
PA_CHANNEL_POSITION_TOP_CENTER = 37
PA_CHANNEL_POSITION_TOP_FRONT_LEFT = 38
PA_CHANNEL_POSITION_TOP_FRONT_RIGHT = 39
PA_CHANNEL_POSITION_TOP_FRONT_CENTER = 40
PA_CHANNEL_POSITION_TOP_REAR_LEFT = 41
PA_CHANNEL_POSITION_TOP_REAR_RIGHT = 42
PA_CHANNEL_POSITION_TOP_REAR_CENTER = 43
PA_CHANNEL_POSITION_MAX = 44
pa_channel_position_t = enum_pa_channel_position 	# /usr/include/pulse/channelmap.h:147
pa_channel_position_mask_t = c_uint64 	# /usr/include/pulse/channelmap.h:210
enum_pa_channel_map_def = c_int
PA_CHANNEL_MAP_AIFF = 0
PA_CHANNEL_MAP_ALSA = 1
PA_CHANNEL_MAP_AUX = 2
PA_CHANNEL_MAP_WAVEEX = 3
PA_CHANNEL_MAP_OSS = 4
PA_CHANNEL_MAP_DEF_MAX = 5
PA_CHANNEL_MAP_DEFAULT = 0
pa_channel_map_def_t = enum_pa_channel_map_def 	# /usr/include/pulse/channelmap.h:247
class struct_pa_channel_map(Structure):
    __slots__ = [
        'channels',
        'map',
    ]
struct_pa_channel_map._fields_ = [
    ('channels', c_uint8),
    ('map', pa_channel_position_t * 32),
]

pa_channel_map = struct_pa_channel_map 	# /usr/include/pulse/channelmap.h:268
# /usr/include/pulse/channelmap.h:273
pa_channel_map_init = _lib.pa_channel_map_init
pa_channel_map_init.restype = POINTER(pa_channel_map)
pa_channel_map_init.argtypes = [POINTER(pa_channel_map)]

# /usr/include/pulse/channelmap.h:276
pa_channel_map_init_mono = _lib.pa_channel_map_init_mono
pa_channel_map_init_mono.restype = POINTER(pa_channel_map)
pa_channel_map_init_mono.argtypes = [POINTER(pa_channel_map)]

# /usr/include/pulse/channelmap.h:279
pa_channel_map_init_stereo = _lib.pa_channel_map_init_stereo
pa_channel_map_init_stereo.restype = POINTER(pa_channel_map)
pa_channel_map_init_stereo.argtypes = [POINTER(pa_channel_map)]

# /usr/include/pulse/channelmap.h:285
pa_channel_map_init_auto = _lib.pa_channel_map_init_auto
pa_channel_map_init_auto.restype = POINTER(pa_channel_map)
pa_channel_map_init_auto.argtypes = [POINTER(pa_channel_map), c_uint, pa_channel_map_def_t]

# /usr/include/pulse/channelmap.h:291
pa_channel_map_init_extend = _lib.pa_channel_map_init_extend
pa_channel_map_init_extend.restype = POINTER(pa_channel_map)
pa_channel_map_init_extend.argtypes = [POINTER(pa_channel_map), c_uint, pa_channel_map_def_t]

# /usr/include/pulse/channelmap.h:294
pa_channel_position_to_string = _lib.pa_channel_position_to_string
pa_channel_position_to_string.restype = c_char_p
pa_channel_position_to_string.argtypes = [pa_channel_position_t]

# /usr/include/pulse/channelmap.h:297
pa_channel_position_from_string = _lib.pa_channel_position_from_string
pa_channel_position_from_string.restype = pa_channel_position_t
pa_channel_position_from_string.argtypes = [c_char_p]

# /usr/include/pulse/channelmap.h:300
pa_channel_position_to_pretty_string = _lib.pa_channel_position_to_pretty_string
pa_channel_position_to_pretty_string.restype = c_char_p
pa_channel_position_to_pretty_string.argtypes = [pa_channel_position_t]

PA_CHANNEL_MAP_SNPRINT_MAX = 336 	# /usr/include/pulse/channelmap.h:307
# /usr/include/pulse/channelmap.h:310
pa_channel_map_snprint = _lib.pa_channel_map_snprint
pa_channel_map_snprint.restype = c_char_p
pa_channel_map_snprint.argtypes = [c_char_p, c_size_t, POINTER(pa_channel_map)]

# /usr/include/pulse/channelmap.h:316
pa_channel_map_parse = _lib.pa_channel_map_parse
pa_channel_map_parse.restype = POINTER(pa_channel_map)
pa_channel_map_parse.argtypes = [POINTER(pa_channel_map), c_char_p]

# /usr/include/pulse/channelmap.h:319
pa_channel_map_equal = _lib.pa_channel_map_equal
pa_channel_map_equal.restype = c_int
pa_channel_map_equal.argtypes = [POINTER(pa_channel_map), POINTER(pa_channel_map)]

# /usr/include/pulse/channelmap.h:322
pa_channel_map_valid = _lib.pa_channel_map_valid
pa_channel_map_valid.restype = c_int
pa_channel_map_valid.argtypes = [POINTER(pa_channel_map)]

# /usr/include/pulse/channelmap.h:326
pa_channel_map_compatible = _lib.pa_channel_map_compatible
pa_channel_map_compatible.restype = c_int
pa_channel_map_compatible.argtypes = [POINTER(pa_channel_map), POINTER(pa_sample_spec)]

# /usr/include/pulse/channelmap.h:329
pa_channel_map_superset = _lib.pa_channel_map_superset
pa_channel_map_superset.restype = c_int
pa_channel_map_superset.argtypes = [POINTER(pa_channel_map), POINTER(pa_channel_map)]

# /usr/include/pulse/channelmap.h:334
pa_channel_map_can_balance = _lib.pa_channel_map_can_balance
pa_channel_map_can_balance.restype = c_int
pa_channel_map_can_balance.argtypes = [POINTER(pa_channel_map)]

# /usr/include/pulse/channelmap.h:339
pa_channel_map_can_fade = _lib.pa_channel_map_can_fade
pa_channel_map_can_fade.restype = c_int
pa_channel_map_can_fade.argtypes = [POINTER(pa_channel_map)]

# /usr/include/pulse/channelmap.h:345
pa_channel_map_to_name = _lib.pa_channel_map_to_name
pa_channel_map_to_name.restype = c_char_p
pa_channel_map_to_name.argtypes = [POINTER(pa_channel_map)]

# /usr/include/pulse/channelmap.h:350
pa_channel_map_to_pretty_name = _lib.pa_channel_map_to_pretty_name
pa_channel_map_to_pretty_name.restype = c_char_p
pa_channel_map_to_pretty_name.argtypes = [POINTER(pa_channel_map)]

# /usr/include/pulse/channelmap.h:354
pa_channel_map_has_position = _lib.pa_channel_map_has_position
pa_channel_map_has_position.restype = c_int
pa_channel_map_has_position.argtypes = [POINTER(pa_channel_map), pa_channel_position_t]

# /usr/include/pulse/channelmap.h:357
pa_channel_map_mask = _lib.pa_channel_map_mask
pa_channel_map_mask.restype = pa_channel_position_mask_t
pa_channel_map_mask.argtypes = [POINTER(pa_channel_map)]

class struct_pa_operation(Structure):
    __slots__ = [
    ]
struct_pa_operation._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_pa_operation(Structure):
    __slots__ = [
    ]
struct_pa_operation._fields_ = [
    ('_opaque_struct', c_int)
]

pa_operation = struct_pa_operation 	# /usr/include/pulse/operation.h:33
pa_operation_notify_cb_t = CFUNCTYPE(None, POINTER(pa_operation), POINTER(None)) 	# /usr/include/pulse/operation.h:36
# /usr/include/pulse/operation.h:39
pa_operation_ref = _lib.pa_operation_ref
pa_operation_ref.restype = POINTER(pa_operation)
pa_operation_ref.argtypes = [POINTER(pa_operation)]

# /usr/include/pulse/operation.h:42
pa_operation_unref = _lib.pa_operation_unref
pa_operation_unref.restype = None
pa_operation_unref.argtypes = [POINTER(pa_operation)]

# /usr/include/pulse/operation.h:49
pa_operation_cancel = _lib.pa_operation_cancel
pa_operation_cancel.restype = None
pa_operation_cancel.argtypes = [POINTER(pa_operation)]

# /usr/include/pulse/operation.h:52
pa_operation_get_state = _lib.pa_operation_get_state
pa_operation_get_state.restype = pa_operation_state_t
pa_operation_get_state.argtypes = [POINTER(pa_operation)]

# /usr/include/pulse/operation.h:60
pa_operation_set_state_callback = _lib.pa_operation_set_state_callback
pa_operation_set_state_callback.restype = None
pa_operation_set_state_callback.argtypes = [POINTER(pa_operation), pa_operation_notify_cb_t, POINTER(None)]

class struct_pa_context(Structure):
    __slots__ = [
    ]
struct_pa_context._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_pa_context(Structure):
    __slots__ = [
    ]
struct_pa_context._fields_ = [
    ('_opaque_struct', c_int)
]

pa_context = struct_pa_context 	# /usr/include/pulse/context.h:154
pa_context_notify_cb_t = CFUNCTYPE(None, POINTER(pa_context), POINTER(None)) 	# /usr/include/pulse/context.h:157
pa_context_success_cb_t = CFUNCTYPE(None, POINTER(pa_context), c_int, POINTER(None)) 	# /usr/include/pulse/context.h:160
class struct_pa_proplist(Structure):
    __slots__ = [
    ]
struct_pa_proplist._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_pa_proplist(Structure):
    __slots__ = [
    ]
struct_pa_proplist._fields_ = [
    ('_opaque_struct', c_int)
]

pa_proplist = struct_pa_proplist 	# /usr/include/pulse/proplist.h:272
pa_context_event_cb_t = CFUNCTYPE(None, POINTER(pa_context), c_char_p, POINTER(pa_proplist), POINTER(None)) 	# /usr/include/pulse/context.h:167
# /usr/include/pulse/context.h:172
pa_context_new = _lib.pa_context_new
pa_context_new.restype = POINTER(pa_context)
pa_context_new.argtypes = [POINTER(pa_mainloop_api), c_char_p]

# /usr/include/pulse/context.h:177
pa_context_new_with_proplist = _lib.pa_context_new_with_proplist
pa_context_new_with_proplist.restype = POINTER(pa_context)
pa_context_new_with_proplist.argtypes = [POINTER(pa_mainloop_api), c_char_p, POINTER(pa_proplist)]

# /usr/include/pulse/context.h:180
pa_context_unref = _lib.pa_context_unref
pa_context_unref.restype = None
pa_context_unref.argtypes = [POINTER(pa_context)]

# /usr/include/pulse/context.h:183
pa_context_ref = _lib.pa_context_ref
pa_context_ref.restype = POINTER(pa_context)
pa_context_ref.argtypes = [POINTER(pa_context)]

# /usr/include/pulse/context.h:186
pa_context_set_state_callback = _lib.pa_context_set_state_callback
pa_context_set_state_callback.restype = None
pa_context_set_state_callback.argtypes = [POINTER(pa_context), pa_context_notify_cb_t, POINTER(None)]

# /usr/include/pulse/context.h:190
pa_context_set_event_callback = _lib.pa_context_set_event_callback
pa_context_set_event_callback.restype = None
pa_context_set_event_callback.argtypes = [POINTER(pa_context), pa_context_event_cb_t, POINTER(None)]

# /usr/include/pulse/context.h:193
pa_context_errno = _lib.pa_context_errno
pa_context_errno.restype = c_int
pa_context_errno.argtypes = [POINTER(pa_context)]

# /usr/include/pulse/context.h:196
pa_context_is_pending = _lib.pa_context_is_pending
pa_context_is_pending.restype = c_int
pa_context_is_pending.argtypes = [POINTER(pa_context)]

# /usr/include/pulse/context.h:199
pa_context_get_state = _lib.pa_context_get_state
pa_context_get_state.restype = pa_context_state_t
pa_context_get_state.argtypes = [POINTER(pa_context)]

# /usr/include/pulse/context.h:209
pa_context_connect = _lib.pa_context_connect
pa_context_connect.restype = c_int
pa_context_connect.argtypes = [POINTER(pa_context), c_char_p, pa_context_flags_t, POINTER(pa_spawn_api)]

# /usr/include/pulse/context.h:212
pa_context_disconnect = _lib.pa_context_disconnect
pa_context_disconnect.restype = None
pa_context_disconnect.argtypes = [POINTER(pa_context)]

# /usr/include/pulse/context.h:215
pa_context_drain = _lib.pa_context_drain
pa_context_drain.restype = POINTER(pa_operation)
pa_context_drain.argtypes = [POINTER(pa_context), pa_context_notify_cb_t, POINTER(None)]

# /usr/include/pulse/context.h:220
pa_context_exit_daemon = _lib.pa_context_exit_daemon
pa_context_exit_daemon.restype = POINTER(pa_operation)
pa_context_exit_daemon.argtypes = [POINTER(pa_context), pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/context.h:223
pa_context_set_default_sink = _lib.pa_context_set_default_sink
pa_context_set_default_sink.restype = POINTER(pa_operation)
pa_context_set_default_sink.argtypes = [POINTER(pa_context), c_char_p, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/context.h:226
pa_context_set_default_source = _lib.pa_context_set_default_source
pa_context_set_default_source.restype = POINTER(pa_operation)
pa_context_set_default_source.argtypes = [POINTER(pa_context), c_char_p, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/context.h:229
pa_context_is_local = _lib.pa_context_is_local
pa_context_is_local.restype = c_int
pa_context_is_local.argtypes = [POINTER(pa_context)]

# /usr/include/pulse/context.h:232
pa_context_set_name = _lib.pa_context_set_name
pa_context_set_name.restype = POINTER(pa_operation)
pa_context_set_name.argtypes = [POINTER(pa_context), c_char_p, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/context.h:235
pa_context_get_server = _lib.pa_context_get_server
pa_context_get_server.restype = c_char_p
pa_context_get_server.argtypes = [POINTER(pa_context)]

# /usr/include/pulse/context.h:238
pa_context_get_protocol_version = _lib.pa_context_get_protocol_version
pa_context_get_protocol_version.restype = c_uint32
pa_context_get_protocol_version.argtypes = [POINTER(pa_context)]

# /usr/include/pulse/context.h:241
pa_context_get_server_protocol_version = _lib.pa_context_get_server_protocol_version
pa_context_get_server_protocol_version.restype = c_uint32
pa_context_get_server_protocol_version.argtypes = [POINTER(pa_context)]

enum_pa_update_mode = c_int
PA_UPDATE_SET = 0
PA_UPDATE_MERGE = 1
PA_UPDATE_REPLACE = 2
pa_update_mode_t = enum_pa_update_mode 	# /usr/include/pulse/proplist.h:337
# /usr/include/pulse/context.h:248
pa_context_proplist_update = _lib.pa_context_proplist_update
pa_context_proplist_update.restype = POINTER(pa_operation)
pa_context_proplist_update.argtypes = [POINTER(pa_context), pa_update_mode_t, POINTER(pa_proplist), pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/context.h:251
pa_context_proplist_remove = _lib.pa_context_proplist_remove
pa_context_proplist_remove.restype = POINTER(pa_operation)
pa_context_proplist_remove.argtypes = [POINTER(pa_context), POINTER(c_char_p), pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/context.h:256
pa_context_get_index = _lib.pa_context_get_index
pa_context_get_index.restype = c_uint32
pa_context_get_index.argtypes = [POINTER(pa_context)]

# /usr/include/pulse/context.h:260
pa_context_rttime_new = _lib.pa_context_rttime_new
pa_context_rttime_new.restype = POINTER(pa_time_event)
pa_context_rttime_new.argtypes = [POINTER(pa_context), pa_usec_t, pa_time_event_cb_t, POINTER(None)]

# /usr/include/pulse/context.h:264
pa_context_rttime_restart = _lib.pa_context_rttime_restart
pa_context_rttime_restart.restype = None
pa_context_rttime_restart.argtypes = [POINTER(pa_context), POINTER(pa_time_event), pa_usec_t]

# /usr/include/pulse/context.h:279
pa_context_get_tile_size = _lib.pa_context_get_tile_size
pa_context_get_tile_size.restype = c_size_t
pa_context_get_tile_size.argtypes = [POINTER(pa_context), POINTER(pa_sample_spec)]

# /usr/include/pulse/context.h:287
pa_context_load_cookie_from_file = _lib.pa_context_load_cookie_from_file
pa_context_load_cookie_from_file.restype = c_int
pa_context_load_cookie_from_file.argtypes = [POINTER(pa_context), c_char_p]

pa_volume_t = c_uint32 	# /usr/include/pulse/volume.h:120
class struct_pa_cvolume(Structure):
    __slots__ = [
        'channels',
        'values',
    ]
struct_pa_cvolume._fields_ = [
    ('channels', c_uint8),
    ('values', pa_volume_t * 32),
]

pa_cvolume = struct_pa_cvolume 	# /usr/include/pulse/volume.h:151
# /usr/include/pulse/volume.h:154
pa_cvolume_equal = _lib.pa_cvolume_equal
pa_cvolume_equal.restype = c_int
pa_cvolume_equal.argtypes = [POINTER(pa_cvolume), POINTER(pa_cvolume)]

# /usr/include/pulse/volume.h:159
pa_cvolume_init = _lib.pa_cvolume_init
pa_cvolume_init.restype = POINTER(pa_cvolume)
pa_cvolume_init.argtypes = [POINTER(pa_cvolume)]

# /usr/include/pulse/volume.h:168
pa_cvolume_set = _lib.pa_cvolume_set
pa_cvolume_set.restype = POINTER(pa_cvolume)
pa_cvolume_set.argtypes = [POINTER(pa_cvolume), c_uint, pa_volume_t]

PA_CVOLUME_SNPRINT_MAX = 320 	# /usr/include/pulse/volume.h:175
# /usr/include/pulse/volume.h:178
pa_cvolume_snprint = _lib.pa_cvolume_snprint
pa_cvolume_snprint.restype = c_char_p
pa_cvolume_snprint.argtypes = [c_char_p, c_size_t, POINTER(pa_cvolume)]

PA_SW_CVOLUME_SNPRINT_DB_MAX = 448 	# /usr/include/pulse/volume.h:185
# /usr/include/pulse/volume.h:188
pa_sw_cvolume_snprint_dB = _lib.pa_sw_cvolume_snprint_dB
pa_sw_cvolume_snprint_dB.restype = c_char_p
pa_sw_cvolume_snprint_dB.argtypes = [c_char_p, c_size_t, POINTER(pa_cvolume)]

PA_CVOLUME_SNPRINT_VERBOSE_MAX = 1984 	# /usr/include/pulse/volume.h:194
# /usr/include/pulse/volume.h:200
#pa_cvolume_snprint_verbose = _lib.pa_cvolume_snprint_verbose
#pa_cvolume_snprint_verbose.restype = c_char_p
#pa_cvolume_snprint_verbose.argtypes = [c_char_p, c_size_t, POINTER(pa_cvolume), POINTER(pa_channel_map), c_int]

PA_VOLUME_SNPRINT_MAX = 10 	# /usr/include/pulse/volume.h:207
# /usr/include/pulse/volume.h:210
pa_volume_snprint = _lib.pa_volume_snprint
pa_volume_snprint.restype = c_char_p
pa_volume_snprint.argtypes = [c_char_p, c_size_t, pa_volume_t]

PA_SW_VOLUME_SNPRINT_DB_MAX = 11 	# /usr/include/pulse/volume.h:217
# /usr/include/pulse/volume.h:220
pa_sw_volume_snprint_dB = _lib.pa_sw_volume_snprint_dB
pa_sw_volume_snprint_dB.restype = c_char_p
pa_sw_volume_snprint_dB.argtypes = [c_char_p, c_size_t, pa_volume_t]

PA_VOLUME_SNPRINT_VERBOSE_MAX = 35 	# /usr/include/pulse/volume.h:226
# /usr/include/pulse/volume.h:231
#pa_volume_snprint_verbose = _lib.pa_volume_snprint_verbose
#pa_volume_snprint_verbose.restype = c_char_p
#pa_volume_snprint_verbose.argtypes = [c_char_p, c_size_t, pa_volume_t, c_int]

# /usr/include/pulse/volume.h:234
pa_cvolume_avg = _lib.pa_cvolume_avg
pa_cvolume_avg.restype = pa_volume_t
pa_cvolume_avg.argtypes = [POINTER(pa_cvolume)]

# /usr/include/pulse/volume.h:241
pa_cvolume_avg_mask = _lib.pa_cvolume_avg_mask
pa_cvolume_avg_mask.restype = pa_volume_t
pa_cvolume_avg_mask.argtypes = [POINTER(pa_cvolume), POINTER(pa_channel_map), pa_channel_position_mask_t]

# /usr/include/pulse/volume.h:244
pa_cvolume_max = _lib.pa_cvolume_max
pa_cvolume_max.restype = pa_volume_t
pa_cvolume_max.argtypes = [POINTER(pa_cvolume)]

# /usr/include/pulse/volume.h:251
pa_cvolume_max_mask = _lib.pa_cvolume_max_mask
pa_cvolume_max_mask.restype = pa_volume_t
pa_cvolume_max_mask.argtypes = [POINTER(pa_cvolume), POINTER(pa_channel_map), pa_channel_position_mask_t]

# /usr/include/pulse/volume.h:254
pa_cvolume_min = _lib.pa_cvolume_min
pa_cvolume_min.restype = pa_volume_t
pa_cvolume_min.argtypes = [POINTER(pa_cvolume)]

# /usr/include/pulse/volume.h:261
pa_cvolume_min_mask = _lib.pa_cvolume_min_mask
pa_cvolume_min_mask.restype = pa_volume_t
pa_cvolume_min_mask.argtypes = [POINTER(pa_cvolume), POINTER(pa_channel_map), pa_channel_position_mask_t]

# /usr/include/pulse/volume.h:264
pa_cvolume_valid = _lib.pa_cvolume_valid
pa_cvolume_valid.restype = c_int
pa_cvolume_valid.argtypes = [POINTER(pa_cvolume)]

# /usr/include/pulse/volume.h:267
pa_cvolume_channels_equal_to = _lib.pa_cvolume_channels_equal_to
pa_cvolume_channels_equal_to.restype = c_int
pa_cvolume_channels_equal_to.argtypes = [POINTER(pa_cvolume), pa_volume_t]

# /usr/include/pulse/volume.h:278
pa_sw_volume_multiply = _lib.pa_sw_volume_multiply
pa_sw_volume_multiply.restype = pa_volume_t
pa_sw_volume_multiply.argtypes = [pa_volume_t, pa_volume_t]

# /usr/include/pulse/volume.h:283
pa_sw_cvolume_multiply = _lib.pa_sw_cvolume_multiply
pa_sw_cvolume_multiply.restype = POINTER(pa_cvolume)
pa_sw_cvolume_multiply.argtypes = [POINTER(pa_cvolume), POINTER(pa_cvolume), POINTER(pa_cvolume)]

# /usr/include/pulse/volume.h:289
pa_sw_cvolume_multiply_scalar = _lib.pa_sw_cvolume_multiply_scalar
pa_sw_cvolume_multiply_scalar.restype = POINTER(pa_cvolume)
pa_sw_cvolume_multiply_scalar.argtypes = [POINTER(pa_cvolume), POINTER(pa_cvolume), pa_volume_t]

# /usr/include/pulse/volume.h:295
pa_sw_volume_divide = _lib.pa_sw_volume_divide
pa_sw_volume_divide.restype = pa_volume_t
pa_sw_volume_divide.argtypes = [pa_volume_t, pa_volume_t]

# /usr/include/pulse/volume.h:300
pa_sw_cvolume_divide = _lib.pa_sw_cvolume_divide
pa_sw_cvolume_divide.restype = POINTER(pa_cvolume)
pa_sw_cvolume_divide.argtypes = [POINTER(pa_cvolume), POINTER(pa_cvolume), POINTER(pa_cvolume)]

# /usr/include/pulse/volume.h:306
pa_sw_cvolume_divide_scalar = _lib.pa_sw_cvolume_divide_scalar
pa_sw_cvolume_divide_scalar.restype = POINTER(pa_cvolume)
pa_sw_cvolume_divide_scalar.argtypes = [POINTER(pa_cvolume), POINTER(pa_cvolume), pa_volume_t]

# /usr/include/pulse/volume.h:309
pa_sw_volume_from_dB = _lib.pa_sw_volume_from_dB
pa_sw_volume_from_dB.restype = pa_volume_t
pa_sw_volume_from_dB.argtypes = [c_double]

# /usr/include/pulse/volume.h:312
pa_sw_volume_to_dB = _lib.pa_sw_volume_to_dB
pa_sw_volume_to_dB.restype = c_double
pa_sw_volume_to_dB.argtypes = [pa_volume_t]

# /usr/include/pulse/volume.h:316
pa_sw_volume_from_linear = _lib.pa_sw_volume_from_linear
pa_sw_volume_from_linear.restype = pa_volume_t
pa_sw_volume_from_linear.argtypes = [c_double]

# /usr/include/pulse/volume.h:319
pa_sw_volume_to_linear = _lib.pa_sw_volume_to_linear
pa_sw_volume_to_linear.restype = c_double
pa_sw_volume_to_linear.argtypes = [pa_volume_t]

# /usr/include/pulse/volume.h:329
pa_cvolume_remap = _lib.pa_cvolume_remap
pa_cvolume_remap.restype = POINTER(pa_cvolume)
pa_cvolume_remap.argtypes = [POINTER(pa_cvolume), POINTER(pa_channel_map), POINTER(pa_channel_map)]

# /usr/include/pulse/volume.h:333
pa_cvolume_compatible = _lib.pa_cvolume_compatible
pa_cvolume_compatible.restype = c_int
pa_cvolume_compatible.argtypes = [POINTER(pa_cvolume), POINTER(pa_sample_spec)]

# /usr/include/pulse/volume.h:337
pa_cvolume_compatible_with_channel_map = _lib.pa_cvolume_compatible_with_channel_map
pa_cvolume_compatible_with_channel_map.restype = c_int
pa_cvolume_compatible_with_channel_map.argtypes = [POINTER(pa_cvolume), POINTER(pa_channel_map)]

# /usr/include/pulse/volume.h:344
pa_cvolume_get_balance = _lib.pa_cvolume_get_balance
pa_cvolume_get_balance.restype = c_float
pa_cvolume_get_balance.argtypes = [POINTER(pa_cvolume), POINTER(pa_channel_map)]

# /usr/include/pulse/volume.h:355
pa_cvolume_set_balance = _lib.pa_cvolume_set_balance
pa_cvolume_set_balance.restype = POINTER(pa_cvolume)
pa_cvolume_set_balance.argtypes = [POINTER(pa_cvolume), POINTER(pa_channel_map), c_float]

# /usr/include/pulse/volume.h:362
pa_cvolume_get_fade = _lib.pa_cvolume_get_fade
pa_cvolume_get_fade.restype = c_float
pa_cvolume_get_fade.argtypes = [POINTER(pa_cvolume), POINTER(pa_channel_map)]

# /usr/include/pulse/volume.h:373
pa_cvolume_set_fade = _lib.pa_cvolume_set_fade
pa_cvolume_set_fade.restype = POINTER(pa_cvolume)
pa_cvolume_set_fade.argtypes = [POINTER(pa_cvolume), POINTER(pa_channel_map), c_float]

# /usr/include/pulse/volume.h:378
pa_cvolume_scale = _lib.pa_cvolume_scale
pa_cvolume_scale.restype = POINTER(pa_cvolume)
pa_cvolume_scale.argtypes = [POINTER(pa_cvolume), pa_volume_t]

# /usr/include/pulse/volume.h:384
pa_cvolume_scale_mask = _lib.pa_cvolume_scale_mask
pa_cvolume_scale_mask.restype = POINTER(pa_cvolume)
pa_cvolume_scale_mask.argtypes = [POINTER(pa_cvolume), pa_volume_t, POINTER(pa_channel_map), pa_channel_position_mask_t]

# /usr/include/pulse/volume.h:391
pa_cvolume_set_position = _lib.pa_cvolume_set_position
pa_cvolume_set_position.restype = POINTER(pa_cvolume)
pa_cvolume_set_position.argtypes = [POINTER(pa_cvolume), POINTER(pa_channel_map), pa_channel_position_t, pa_volume_t]

# /usr/include/pulse/volume.h:397
pa_cvolume_get_position = _lib.pa_cvolume_get_position
pa_cvolume_get_position.restype = pa_volume_t
pa_cvolume_get_position.argtypes = [POINTER(pa_cvolume), POINTER(pa_channel_map), pa_channel_position_t]

# /usr/include/pulse/volume.h:402
pa_cvolume_merge = _lib.pa_cvolume_merge
pa_cvolume_merge.restype = POINTER(pa_cvolume)
pa_cvolume_merge.argtypes = [POINTER(pa_cvolume), POINTER(pa_cvolume), POINTER(pa_cvolume)]

# /usr/include/pulse/volume.h:406
pa_cvolume_inc_clamp = _lib.pa_cvolume_inc_clamp
pa_cvolume_inc_clamp.restype = POINTER(pa_cvolume)
pa_cvolume_inc_clamp.argtypes = [POINTER(pa_cvolume), pa_volume_t, pa_volume_t]

# /usr/include/pulse/volume.h:410
pa_cvolume_inc = _lib.pa_cvolume_inc
pa_cvolume_inc.restype = POINTER(pa_cvolume)
pa_cvolume_inc.argtypes = [POINTER(pa_cvolume), pa_volume_t]

# /usr/include/pulse/volume.h:414
pa_cvolume_dec = _lib.pa_cvolume_dec
pa_cvolume_dec.restype = POINTER(pa_cvolume)
pa_cvolume_dec.argtypes = [POINTER(pa_cvolume), pa_volume_t]

class struct_pa_stream(Structure):
    __slots__ = [
    ]
struct_pa_stream._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_pa_stream(Structure):
    __slots__ = [
    ]
struct_pa_stream._fields_ = [
    ('_opaque_struct', c_int)
]

pa_stream = struct_pa_stream 	# /usr/include/pulse/stream.h:335
pa_stream_success_cb_t = CFUNCTYPE(None, POINTER(pa_stream), c_int, POINTER(None)) 	# /usr/include/pulse/stream.h:338
pa_stream_request_cb_t = CFUNCTYPE(None, POINTER(pa_stream), c_size_t, POINTER(None)) 	# /usr/include/pulse/stream.h:341
pa_stream_notify_cb_t = CFUNCTYPE(None, POINTER(pa_stream), POINTER(None)) 	# /usr/include/pulse/stream.h:344
pa_stream_event_cb_t = CFUNCTYPE(None, POINTER(pa_stream), c_char_p, POINTER(pa_proplist), POINTER(None)) 	# /usr/include/pulse/stream.h:352
# /usr/include/pulse/stream.h:357
pa_stream_new = _lib.pa_stream_new
pa_stream_new.restype = POINTER(pa_stream)
pa_stream_new.argtypes = [POINTER(pa_context), c_char_p, POINTER(pa_sample_spec), POINTER(pa_channel_map)]

# /usr/include/pulse/stream.h:366
pa_stream_new_with_proplist = _lib.pa_stream_new_with_proplist
pa_stream_new_with_proplist.restype = POINTER(pa_stream)
pa_stream_new_with_proplist.argtypes = [POINTER(pa_context), c_char_p, POINTER(pa_sample_spec), POINTER(pa_channel_map), POINTER(pa_proplist)]

class struct_pa_format_info(Structure):
    __slots__ = [
        'encoding',
        'plist',
    ]
enum_pa_encoding = c_int
PA_ENCODING_ANY = 0
PA_ENCODING_PCM = 1
PA_ENCODING_AC3_IEC61937 = 2
PA_ENCODING_EAC3_IEC61937 = 3
PA_ENCODING_MPEG_IEC61937 = 4
PA_ENCODING_DTS_IEC61937 = 5
PA_ENCODING_MPEG2_AAC_IEC61937 = 6
PA_ENCODING_MAX = 7
PA_ENCODING_INVALID = -1
pa_encoding_t = enum_pa_encoding 	# /usr/include/pulse/format.h:64
struct_pa_format_info._fields_ = [
    ('encoding', pa_encoding_t),
    ('plist', POINTER(pa_proplist)),
]

pa_format_info = struct_pa_format_info 	# /usr/include/pulse/format.h:91
# /usr/include/pulse/stream.h:377
pa_stream_new_extended = _lib.pa_stream_new_extended
pa_stream_new_extended.restype = POINTER(pa_stream)
pa_stream_new_extended.argtypes = [POINTER(pa_context), c_char_p, POINTER(POINTER(pa_format_info)), c_uint, POINTER(pa_proplist)]

# /usr/include/pulse/stream.h:385
pa_stream_unref = _lib.pa_stream_unref
pa_stream_unref.restype = None
pa_stream_unref.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:388
pa_stream_ref = _lib.pa_stream_ref
pa_stream_ref.restype = POINTER(pa_stream)
pa_stream_ref.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:391
pa_stream_get_state = _lib.pa_stream_get_state
pa_stream_get_state.restype = pa_stream_state_t
pa_stream_get_state.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:394
pa_stream_get_context = _lib.pa_stream_get_context
pa_stream_get_context.restype = POINTER(pa_context)
pa_stream_get_context.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:400
pa_stream_get_index = _lib.pa_stream_get_index
pa_stream_get_index.restype = c_uint32
pa_stream_get_index.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:411
pa_stream_get_device_index = _lib.pa_stream_get_device_index
pa_stream_get_device_index.restype = c_uint32
pa_stream_get_device_index.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:422
pa_stream_get_device_name = _lib.pa_stream_get_device_name
pa_stream_get_device_name.restype = c_char_p
pa_stream_get_device_name.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:428
pa_stream_is_suspended = _lib.pa_stream_is_suspended
pa_stream_is_suspended.restype = c_int
pa_stream_is_suspended.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:432
pa_stream_is_corked = _lib.pa_stream_is_corked
pa_stream_is_corked.restype = c_int
pa_stream_is_corked.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:458
pa_stream_connect_playback = _lib.pa_stream_connect_playback
pa_stream_connect_playback.restype = c_int
pa_stream_connect_playback.argtypes = [POINTER(pa_stream), c_char_p, POINTER(pa_buffer_attr), pa_stream_flags_t, POINTER(pa_cvolume), POINTER(pa_stream)]

# /usr/include/pulse/stream.h:467
pa_stream_connect_record = _lib.pa_stream_connect_record
pa_stream_connect_record.restype = c_int
pa_stream_connect_record.argtypes = [POINTER(pa_stream), c_char_p, POINTER(pa_buffer_attr), pa_stream_flags_t]

# /usr/include/pulse/stream.h:474
pa_stream_disconnect = _lib.pa_stream_disconnect
pa_stream_disconnect.restype = c_int
pa_stream_disconnect.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:508
pa_stream_begin_write = _lib.pa_stream_begin_write
pa_stream_begin_write.restype = c_int
pa_stream_begin_write.argtypes = [POINTER(pa_stream), POINTER(POINTER(None)), POINTER(c_size_t)]

# /usr/include/pulse/stream.h:522
pa_stream_cancel_write = _lib.pa_stream_cancel_write
pa_stream_cancel_write.restype = c_int
pa_stream_cancel_write.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:547
pa_stream_write = _lib.pa_stream_write
pa_stream_write.restype = c_int
pa_stream_write.argtypes = [POINTER(pa_stream), POINTER(None), c_size_t, pa_free_cb_t, c_int64, pa_seek_mode_t]

# /usr/include/pulse/stream.h:557
#pa_stream_write_ext_free = _lib.pa_stream_write_ext_free
#pa_stream_write_ext_free.restype = c_int
#pa_stream_write_ext_free.argtypes = [POINTER(pa_stream), POINTER(None), c_size_t, pa_free_cb_t, POINTER(None), c_int64, pa_seek_mode_t]

# /usr/include/pulse/stream.h:582
pa_stream_peek = _lib.pa_stream_peek
pa_stream_peek.restype = c_int
pa_stream_peek.argtypes = [POINTER(pa_stream), POINTER(POINTER(None)), POINTER(c_size_t)]

# /usr/include/pulse/stream.h:589
pa_stream_drop = _lib.pa_stream_drop
pa_stream_drop.restype = c_int
pa_stream_drop.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:592
pa_stream_writable_size = _lib.pa_stream_writable_size
pa_stream_writable_size.restype = c_size_t
pa_stream_writable_size.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:595
pa_stream_readable_size = _lib.pa_stream_readable_size
pa_stream_readable_size.restype = c_size_t
pa_stream_readable_size.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:601
pa_stream_drain = _lib.pa_stream_drain
pa_stream_drain.restype = POINTER(pa_operation)
pa_stream_drain.argtypes = [POINTER(pa_stream), pa_stream_success_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:607
pa_stream_update_timing_info = _lib.pa_stream_update_timing_info
pa_stream_update_timing_info.restype = POINTER(pa_operation)
pa_stream_update_timing_info.argtypes = [POINTER(pa_stream), pa_stream_success_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:610
pa_stream_set_state_callback = _lib.pa_stream_set_state_callback
pa_stream_set_state_callback.restype = None
pa_stream_set_state_callback.argtypes = [POINTER(pa_stream), pa_stream_notify_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:614
pa_stream_set_write_callback = _lib.pa_stream_set_write_callback
pa_stream_set_write_callback.restype = None
pa_stream_set_write_callback.argtypes = [POINTER(pa_stream), pa_stream_request_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:617
pa_stream_set_read_callback = _lib.pa_stream_set_read_callback
pa_stream_set_read_callback.restype = None
pa_stream_set_read_callback.argtypes = [POINTER(pa_stream), pa_stream_request_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:620
pa_stream_set_overflow_callback = _lib.pa_stream_set_overflow_callback
pa_stream_set_overflow_callback.restype = None
pa_stream_set_overflow_callback.argtypes = [POINTER(pa_stream), pa_stream_notify_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:626
pa_stream_get_underflow_index = _lib.pa_stream_get_underflow_index
pa_stream_get_underflow_index.restype = c_int64
pa_stream_get_underflow_index.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:629
pa_stream_set_underflow_callback = _lib.pa_stream_set_underflow_callback
pa_stream_set_underflow_callback.restype = None
pa_stream_set_underflow_callback.argtypes = [POINTER(pa_stream), pa_stream_notify_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:636
pa_stream_set_started_callback = _lib.pa_stream_set_started_callback
pa_stream_set_started_callback.restype = None
pa_stream_set_started_callback.argtypes = [POINTER(pa_stream), pa_stream_notify_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:641
pa_stream_set_latency_update_callback = _lib.pa_stream_set_latency_update_callback
pa_stream_set_latency_update_callback.restype = None
pa_stream_set_latency_update_callback.argtypes = [POINTER(pa_stream), pa_stream_notify_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:648
pa_stream_set_moved_callback = _lib.pa_stream_set_moved_callback
pa_stream_set_moved_callback.restype = None
pa_stream_set_moved_callback.argtypes = [POINTER(pa_stream), pa_stream_notify_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:658
pa_stream_set_suspended_callback = _lib.pa_stream_set_suspended_callback
pa_stream_set_suspended_callback.restype = None
pa_stream_set_suspended_callback.argtypes = [POINTER(pa_stream), pa_stream_notify_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:662
pa_stream_set_event_callback = _lib.pa_stream_set_event_callback
pa_stream_set_event_callback.restype = None
pa_stream_set_event_callback.argtypes = [POINTER(pa_stream), pa_stream_event_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:669
pa_stream_set_buffer_attr_callback = _lib.pa_stream_set_buffer_attr_callback
pa_stream_set_buffer_attr_callback.restype = None
pa_stream_set_buffer_attr_callback.argtypes = [POINTER(pa_stream), pa_stream_notify_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:681
pa_stream_cork = _lib.pa_stream_cork
pa_stream_cork.restype = POINTER(pa_operation)
pa_stream_cork.argtypes = [POINTER(pa_stream), c_int, pa_stream_success_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:686
pa_stream_flush = _lib.pa_stream_flush
pa_stream_flush.restype = POINTER(pa_operation)
pa_stream_flush.argtypes = [POINTER(pa_stream), pa_stream_success_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:690
pa_stream_prebuf = _lib.pa_stream_prebuf
pa_stream_prebuf.restype = POINTER(pa_operation)
pa_stream_prebuf.argtypes = [POINTER(pa_stream), pa_stream_success_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:695
pa_stream_trigger = _lib.pa_stream_trigger
pa_stream_trigger.restype = POINTER(pa_operation)
pa_stream_trigger.argtypes = [POINTER(pa_stream), pa_stream_success_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:698
pa_stream_set_name = _lib.pa_stream_set_name
pa_stream_set_name.restype = POINTER(pa_operation)
pa_stream_set_name.argtypes = [POINTER(pa_stream), c_char_p, pa_stream_success_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:731
pa_stream_get_time = _lib.pa_stream_get_time
pa_stream_get_time.restype = c_int
pa_stream_get_time.argtypes = [POINTER(pa_stream), POINTER(pa_usec_t)]

# /usr/include/pulse/stream.h:745
pa_stream_get_latency = _lib.pa_stream_get_latency
pa_stream_get_latency.restype = c_int
pa_stream_get_latency.argtypes = [POINTER(pa_stream), POINTER(pa_usec_t), POINTER(c_int)]

# /usr/include/pulse/stream.h:761
pa_stream_get_timing_info = _lib.pa_stream_get_timing_info
pa_stream_get_timing_info.restype = POINTER(pa_timing_info)
pa_stream_get_timing_info.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:764
pa_stream_get_sample_spec = _lib.pa_stream_get_sample_spec
pa_stream_get_sample_spec.restype = POINTER(pa_sample_spec)
pa_stream_get_sample_spec.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:767
pa_stream_get_channel_map = _lib.pa_stream_get_channel_map
pa_stream_get_channel_map.restype = POINTER(pa_channel_map)
pa_stream_get_channel_map.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:770
pa_stream_get_format_info = _lib.pa_stream_get_format_info
pa_stream_get_format_info.restype = POINTER(pa_format_info)
pa_stream_get_format_info.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:780
pa_stream_get_buffer_attr = _lib.pa_stream_get_buffer_attr
pa_stream_get_buffer_attr.restype = POINTER(pa_buffer_attr)
pa_stream_get_buffer_attr.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/stream.h:790
pa_stream_set_buffer_attr = _lib.pa_stream_set_buffer_attr
pa_stream_set_buffer_attr.restype = POINTER(pa_operation)
pa_stream_set_buffer_attr.argtypes = [POINTER(pa_stream), POINTER(pa_buffer_attr), pa_stream_success_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:797
pa_stream_update_sample_rate = _lib.pa_stream_update_sample_rate
pa_stream_update_sample_rate.restype = POINTER(pa_operation)
pa_stream_update_sample_rate.argtypes = [POINTER(pa_stream), c_uint32, pa_stream_success_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:805
pa_stream_proplist_update = _lib.pa_stream_proplist_update
pa_stream_proplist_update.restype = POINTER(pa_operation)
pa_stream_proplist_update.argtypes = [POINTER(pa_stream), pa_update_mode_t, POINTER(pa_proplist), pa_stream_success_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:809
pa_stream_proplist_remove = _lib.pa_stream_proplist_remove
pa_stream_proplist_remove.restype = POINTER(pa_operation)
pa_stream_proplist_remove.argtypes = [POINTER(pa_stream), POINTER(c_char_p), pa_stream_success_cb_t, POINTER(None)]

# /usr/include/pulse/stream.h:815
pa_stream_set_monitor_stream = _lib.pa_stream_set_monitor_stream
pa_stream_set_monitor_stream.restype = c_int
pa_stream_set_monitor_stream.argtypes = [POINTER(pa_stream), c_uint32]

# /usr/include/pulse/stream.h:820
pa_stream_get_monitor_stream = _lib.pa_stream_get_monitor_stream
pa_stream_get_monitor_stream.restype = c_uint32
pa_stream_get_monitor_stream.argtypes = [POINTER(pa_stream)]

class struct_pa_sink_port_info(Structure):
    __slots__ = [
        'name',
        'description',
        'priority',
        'available',
    ]
struct_pa_sink_port_info._fields_ = [
    ('name', c_char_p),
    ('description', c_char_p),
    ('priority', c_uint32),
    ('available', c_int),
]

pa_sink_port_info = struct_pa_sink_port_info 	# /usr/include/pulse/introspect.h:232
class struct_pa_sink_info(Structure):
    __slots__ = [
        'name',
        'index',
        'description',
        'sample_spec',
        'channel_map',
        'owner_module',
        'volume',
        'mute',
        'monitor_source',
        'monitor_source_name',
        'latency',
        'driver',
        'flags',
        'proplist',
        'configured_latency',
        'base_volume',
        'state',
        'n_volume_steps',
        'card',
        'n_ports',
        'ports',
        'active_port',
        'n_formats',
        'formats',
    ]
struct_pa_sink_info._fields_ = [
    ('name', c_char_p),
    ('index', c_uint32),
    ('description', c_char_p),
    ('sample_spec', pa_sample_spec),
    ('channel_map', pa_channel_map),
    ('owner_module', c_uint32),
    ('volume', pa_cvolume),
    ('mute', c_int),
    ('monitor_source', c_uint32),
    ('monitor_source_name', c_char_p),
    ('latency', pa_usec_t),
    ('driver', c_char_p),
    ('flags', pa_sink_flags_t),
    ('proplist', POINTER(pa_proplist)),
    ('configured_latency', pa_usec_t),
    ('base_volume', pa_volume_t),
    ('state', pa_sink_state_t),
    ('n_volume_steps', c_uint32),
    ('card', c_uint32),
    ('n_ports', c_uint32),
    ('ports', POINTER(POINTER(pa_sink_port_info))),
    ('active_port', POINTER(pa_sink_port_info)),
    ('n_formats', c_uint8),
    ('formats', POINTER(POINTER(pa_format_info))),
]

pa_sink_info = struct_pa_sink_info 	# /usr/include/pulse/introspect.h:262
pa_sink_info_cb_t = CFUNCTYPE(None, POINTER(pa_context), POINTER(pa_sink_info), c_int, POINTER(None)) 	# /usr/include/pulse/introspect.h:265
# /usr/include/pulse/introspect.h:268
pa_context_get_sink_info_by_name = _lib.pa_context_get_sink_info_by_name
pa_context_get_sink_info_by_name.restype = POINTER(pa_operation)
pa_context_get_sink_info_by_name.argtypes = [POINTER(pa_context), c_char_p, pa_sink_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:271
pa_context_get_sink_info_by_index = _lib.pa_context_get_sink_info_by_index
pa_context_get_sink_info_by_index.restype = POINTER(pa_operation)
pa_context_get_sink_info_by_index.argtypes = [POINTER(pa_context), c_uint32, pa_sink_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:274
pa_context_get_sink_info_list = _lib.pa_context_get_sink_info_list
pa_context_get_sink_info_list.restype = POINTER(pa_operation)
pa_context_get_sink_info_list.argtypes = [POINTER(pa_context), pa_sink_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:277
pa_context_set_sink_volume_by_index = _lib.pa_context_set_sink_volume_by_index
pa_context_set_sink_volume_by_index.restype = POINTER(pa_operation)
pa_context_set_sink_volume_by_index.argtypes = [POINTER(pa_context), c_uint32, POINTER(pa_cvolume), pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:280
pa_context_set_sink_volume_by_name = _lib.pa_context_set_sink_volume_by_name
pa_context_set_sink_volume_by_name.restype = POINTER(pa_operation)
pa_context_set_sink_volume_by_name.argtypes = [POINTER(pa_context), c_char_p, POINTER(pa_cvolume), pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:283
pa_context_set_sink_mute_by_index = _lib.pa_context_set_sink_mute_by_index
pa_context_set_sink_mute_by_index.restype = POINTER(pa_operation)
pa_context_set_sink_mute_by_index.argtypes = [POINTER(pa_context), c_uint32, c_int, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:286
pa_context_set_sink_mute_by_name = _lib.pa_context_set_sink_mute_by_name
pa_context_set_sink_mute_by_name.restype = POINTER(pa_operation)
pa_context_set_sink_mute_by_name.argtypes = [POINTER(pa_context), c_char_p, c_int, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:289
pa_context_suspend_sink_by_name = _lib.pa_context_suspend_sink_by_name
pa_context_suspend_sink_by_name.restype = POINTER(pa_operation)
pa_context_suspend_sink_by_name.argtypes = [POINTER(pa_context), c_char_p, c_int, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:292
pa_context_suspend_sink_by_index = _lib.pa_context_suspend_sink_by_index
pa_context_suspend_sink_by_index.restype = POINTER(pa_operation)
pa_context_suspend_sink_by_index.argtypes = [POINTER(pa_context), c_uint32, c_int, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:295
pa_context_set_sink_port_by_index = _lib.pa_context_set_sink_port_by_index
pa_context_set_sink_port_by_index.restype = POINTER(pa_operation)
pa_context_set_sink_port_by_index.argtypes = [POINTER(pa_context), c_uint32, c_char_p, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:298
pa_context_set_sink_port_by_name = _lib.pa_context_set_sink_port_by_name
pa_context_set_sink_port_by_name.restype = POINTER(pa_operation)
pa_context_set_sink_port_by_name.argtypes = [POINTER(pa_context), c_char_p, c_char_p, pa_context_success_cb_t, POINTER(None)]

class struct_pa_source_port_info(Structure):
    __slots__ = [
        'name',
        'description',
        'priority',
        'available',
    ]
struct_pa_source_port_info._fields_ = [
    ('name', c_char_p),
    ('description', c_char_p),
    ('priority', c_uint32),
    ('available', c_int),
]

pa_source_port_info = struct_pa_source_port_info 	# /usr/include/pulse/introspect.h:312
class struct_pa_source_info(Structure):
    __slots__ = [
        'name',
        'index',
        'description',
        'sample_spec',
        'channel_map',
        'owner_module',
        'volume',
        'mute',
        'monitor_of_sink',
        'monitor_of_sink_name',
        'latency',
        'driver',
        'flags',
        'proplist',
        'configured_latency',
        'base_volume',
        'state',
        'n_volume_steps',
        'card',
        'n_ports',
        'ports',
        'active_port',
        'n_formats',
        'formats',
    ]
struct_pa_source_info._fields_ = [
    ('name', c_char_p),
    ('index', c_uint32),
    ('description', c_char_p),
    ('sample_spec', pa_sample_spec),
    ('channel_map', pa_channel_map),
    ('owner_module', c_uint32),
    ('volume', pa_cvolume),
    ('mute', c_int),
    ('monitor_of_sink', c_uint32),
    ('monitor_of_sink_name', c_char_p),
    ('latency', pa_usec_t),
    ('driver', c_char_p),
    ('flags', pa_source_flags_t),
    ('proplist', POINTER(pa_proplist)),
    ('configured_latency', pa_usec_t),
    ('base_volume', pa_volume_t),
    ('state', pa_source_state_t),
    ('n_volume_steps', c_uint32),
    ('card', c_uint32),
    ('n_ports', c_uint32),
    ('ports', POINTER(POINTER(pa_source_port_info))),
    ('active_port', POINTER(pa_source_port_info)),
    ('n_formats', c_uint8),
    ('formats', POINTER(POINTER(pa_format_info))),
]

pa_source_info = struct_pa_source_info 	# /usr/include/pulse/introspect.h:342
pa_source_info_cb_t = CFUNCTYPE(None, POINTER(pa_context), POINTER(pa_source_info), c_int, POINTER(None)) 	# /usr/include/pulse/introspect.h:345
# /usr/include/pulse/introspect.h:348
pa_context_get_source_info_by_name = _lib.pa_context_get_source_info_by_name
pa_context_get_source_info_by_name.restype = POINTER(pa_operation)
pa_context_get_source_info_by_name.argtypes = [POINTER(pa_context), c_char_p, pa_source_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:351
pa_context_get_source_info_by_index = _lib.pa_context_get_source_info_by_index
pa_context_get_source_info_by_index.restype = POINTER(pa_operation)
pa_context_get_source_info_by_index.argtypes = [POINTER(pa_context), c_uint32, pa_source_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:354
pa_context_get_source_info_list = _lib.pa_context_get_source_info_list
pa_context_get_source_info_list.restype = POINTER(pa_operation)
pa_context_get_source_info_list.argtypes = [POINTER(pa_context), pa_source_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:357
pa_context_set_source_volume_by_index = _lib.pa_context_set_source_volume_by_index
pa_context_set_source_volume_by_index.restype = POINTER(pa_operation)
pa_context_set_source_volume_by_index.argtypes = [POINTER(pa_context), c_uint32, POINTER(pa_cvolume), pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:360
pa_context_set_source_volume_by_name = _lib.pa_context_set_source_volume_by_name
pa_context_set_source_volume_by_name.restype = POINTER(pa_operation)
pa_context_set_source_volume_by_name.argtypes = [POINTER(pa_context), c_char_p, POINTER(pa_cvolume), pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:363
pa_context_set_source_mute_by_index = _lib.pa_context_set_source_mute_by_index
pa_context_set_source_mute_by_index.restype = POINTER(pa_operation)
pa_context_set_source_mute_by_index.argtypes = [POINTER(pa_context), c_uint32, c_int, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:366
pa_context_set_source_mute_by_name = _lib.pa_context_set_source_mute_by_name
pa_context_set_source_mute_by_name.restype = POINTER(pa_operation)
pa_context_set_source_mute_by_name.argtypes = [POINTER(pa_context), c_char_p, c_int, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:369
pa_context_suspend_source_by_name = _lib.pa_context_suspend_source_by_name
pa_context_suspend_source_by_name.restype = POINTER(pa_operation)
pa_context_suspend_source_by_name.argtypes = [POINTER(pa_context), c_char_p, c_int, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:372
pa_context_suspend_source_by_index = _lib.pa_context_suspend_source_by_index
pa_context_suspend_source_by_index.restype = POINTER(pa_operation)
pa_context_suspend_source_by_index.argtypes = [POINTER(pa_context), c_uint32, c_int, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:375
pa_context_set_source_port_by_index = _lib.pa_context_set_source_port_by_index
pa_context_set_source_port_by_index.restype = POINTER(pa_operation)
pa_context_set_source_port_by_index.argtypes = [POINTER(pa_context), c_uint32, c_char_p, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:378
pa_context_set_source_port_by_name = _lib.pa_context_set_source_port_by_name
pa_context_set_source_port_by_name.restype = POINTER(pa_operation)
pa_context_set_source_port_by_name.argtypes = [POINTER(pa_context), c_char_p, c_char_p, pa_context_success_cb_t, POINTER(None)]

class struct_pa_server_info(Structure):
    __slots__ = [
        'user_name',
        'host_name',
        'server_version',
        'server_name',
        'sample_spec',
        'default_sink_name',
        'default_source_name',
        'cookie',
        'channel_map',
    ]
struct_pa_server_info._fields_ = [
    ('user_name', c_char_p),
    ('host_name', c_char_p),
    ('server_version', c_char_p),
    ('server_name', c_char_p),
    ('sample_spec', pa_sample_spec),
    ('default_sink_name', c_char_p),
    ('default_source_name', c_char_p),
    ('cookie', c_uint32),
    ('channel_map', pa_channel_map),
]

pa_server_info = struct_pa_server_info 	# /usr/include/pulse/introspect.h:397
pa_server_info_cb_t = CFUNCTYPE(None, POINTER(pa_context), POINTER(pa_server_info), POINTER(None)) 	# /usr/include/pulse/introspect.h:400
# /usr/include/pulse/introspect.h:403
pa_context_get_server_info = _lib.pa_context_get_server_info
pa_context_get_server_info.restype = POINTER(pa_operation)
pa_context_get_server_info.argtypes = [POINTER(pa_context), pa_server_info_cb_t, POINTER(None)]

class struct_pa_module_info(Structure):
    __slots__ = [
        'index',
        'name',
        'argument',
        'n_used',
        'auto_unload',
        'proplist',
    ]
struct_pa_module_info._fields_ = [
    ('index', c_uint32),
    ('name', c_char_p),
    ('argument', c_char_p),
    ('n_used', c_uint32),
    ('auto_unload', c_int),
    ('proplist', POINTER(pa_proplist)),
]

pa_module_info = struct_pa_module_info 	# /usr/include/pulse/introspect.h:421
pa_module_info_cb_t = CFUNCTYPE(None, POINTER(pa_context), POINTER(pa_module_info), c_int, POINTER(None)) 	# /usr/include/pulse/introspect.h:424
# /usr/include/pulse/introspect.h:427
pa_context_get_module_info = _lib.pa_context_get_module_info
pa_context_get_module_info.restype = POINTER(pa_operation)
pa_context_get_module_info.argtypes = [POINTER(pa_context), c_uint32, pa_module_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:430
pa_context_get_module_info_list = _lib.pa_context_get_module_info_list
pa_context_get_module_info_list.restype = POINTER(pa_operation)
pa_context_get_module_info_list.argtypes = [POINTER(pa_context), pa_module_info_cb_t, POINTER(None)]

pa_context_index_cb_t = CFUNCTYPE(None, POINTER(pa_context), c_uint32, POINTER(None)) 	# /usr/include/pulse/introspect.h:433
# /usr/include/pulse/introspect.h:436
pa_context_load_module = _lib.pa_context_load_module
pa_context_load_module.restype = POINTER(pa_operation)
pa_context_load_module.argtypes = [POINTER(pa_context), c_char_p, c_char_p, pa_context_index_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:439
pa_context_unload_module = _lib.pa_context_unload_module
pa_context_unload_module.restype = POINTER(pa_operation)
pa_context_unload_module.argtypes = [POINTER(pa_context), c_uint32, pa_context_success_cb_t, POINTER(None)]

class struct_pa_client_info(Structure):
    __slots__ = [
        'index',
        'name',
        'owner_module',
        'driver',
        'proplist',
    ]
struct_pa_client_info._fields_ = [
    ('index', c_uint32),
    ('name', c_char_p),
    ('owner_module', c_uint32),
    ('driver', c_char_p),
    ('proplist', POINTER(pa_proplist)),
]

pa_client_info = struct_pa_client_info 	# /usr/include/pulse/introspect.h:454
pa_client_info_cb_t = CFUNCTYPE(None, POINTER(pa_context), POINTER(pa_client_info), c_int, POINTER(None)) 	# /usr/include/pulse/introspect.h:457
# /usr/include/pulse/introspect.h:460
pa_context_get_client_info = _lib.pa_context_get_client_info
pa_context_get_client_info.restype = POINTER(pa_operation)
pa_context_get_client_info.argtypes = [POINTER(pa_context), c_uint32, pa_client_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:463
pa_context_get_client_info_list = _lib.pa_context_get_client_info_list
pa_context_get_client_info_list.restype = POINTER(pa_operation)
pa_context_get_client_info_list.argtypes = [POINTER(pa_context), pa_client_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:466
pa_context_kill_client = _lib.pa_context_kill_client
pa_context_kill_client.restype = POINTER(pa_operation)
pa_context_kill_client.argtypes = [POINTER(pa_context), c_uint32, pa_context_success_cb_t, POINTER(None)]

class struct_pa_card_profile_info(Structure):
    __slots__ = [
        'name',
        'description',
        'n_sinks',
        'n_sources',
        'priority',
    ]
struct_pa_card_profile_info._fields_ = [
    ('name', c_char_p),
    ('description', c_char_p),
    ('n_sinks', c_uint32),
    ('n_sources', c_uint32),
    ('priority', c_uint32),
]

pa_card_profile_info = struct_pa_card_profile_info 	# /usr/include/pulse/introspect.h:479
class struct_pa_card_profile_info2(Structure):
    __slots__ = [
        'name',
        'description',
        'n_sinks',
        'n_sources',
        'priority',
        'available',
    ]
struct_pa_card_profile_info2._fields_ = [
    ('name', c_char_p),
    ('description', c_char_p),
    ('n_sinks', c_uint32),
    ('n_sources', c_uint32),
    ('priority', c_uint32),
    ('available', c_int),
]

pa_card_profile_info2 = struct_pa_card_profile_info2 	# /usr/include/pulse/introspect.h:496
class struct_pa_card_port_info(Structure):
    __slots__ = [
        'name',
        'description',
        'priority',
        'available',
        'direction',
        'n_profiles',
        'profiles',
        'proplist',
        'latency_offset',
        'profiles2',
    ]
struct_pa_card_port_info._fields_ = [
    ('name', c_char_p),
    ('description', c_char_p),
    ('priority', c_uint32),
    ('available', c_int),
    ('direction', c_int),
    ('n_profiles', c_uint32),
    ('profiles', POINTER(POINTER(pa_card_profile_info))),
    ('proplist', POINTER(pa_proplist)),
    ('latency_offset', c_int64),
    ('profiles2', POINTER(POINTER(pa_card_profile_info2))),
]

pa_card_port_info = struct_pa_card_port_info 	# /usr/include/pulse/introspect.h:512
class struct_pa_card_info(Structure):
    __slots__ = [
        'index',
        'name',
        'owner_module',
        'driver',
        'n_profiles',
        'profiles',
        'active_profile',
        'proplist',
        'n_ports',
        'ports',
        'profiles2',
        'active_profile2',
    ]
struct_pa_card_info._fields_ = [
    ('index', c_uint32),
    ('name', c_char_p),
    ('owner_module', c_uint32),
    ('driver', c_char_p),
    ('n_profiles', c_uint32),
    ('profiles', POINTER(pa_card_profile_info)),
    ('active_profile', POINTER(pa_card_profile_info)),
    ('proplist', POINTER(pa_proplist)),
    ('n_ports', c_uint32),
    ('ports', POINTER(POINTER(pa_card_port_info))),
    ('profiles2', POINTER(POINTER(pa_card_profile_info2))),
    ('active_profile2', POINTER(pa_card_profile_info2)),
]

pa_card_info = struct_pa_card_info 	# /usr/include/pulse/introspect.h:530
pa_card_info_cb_t = CFUNCTYPE(None, POINTER(pa_context), POINTER(pa_card_info), c_int, POINTER(None)) 	# /usr/include/pulse/introspect.h:533
# /usr/include/pulse/introspect.h:536
pa_context_get_card_info_by_index = _lib.pa_context_get_card_info_by_index
pa_context_get_card_info_by_index.restype = POINTER(pa_operation)
pa_context_get_card_info_by_index.argtypes = [POINTER(pa_context), c_uint32, pa_card_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:539
pa_context_get_card_info_by_name = _lib.pa_context_get_card_info_by_name
pa_context_get_card_info_by_name.restype = POINTER(pa_operation)
pa_context_get_card_info_by_name.argtypes = [POINTER(pa_context), c_char_p, pa_card_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:542
pa_context_get_card_info_list = _lib.pa_context_get_card_info_list
pa_context_get_card_info_list.restype = POINTER(pa_operation)
pa_context_get_card_info_list.argtypes = [POINTER(pa_context), pa_card_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:545
pa_context_set_card_profile_by_index = _lib.pa_context_set_card_profile_by_index
pa_context_set_card_profile_by_index.restype = POINTER(pa_operation)
pa_context_set_card_profile_by_index.argtypes = [POINTER(pa_context), c_uint32, c_char_p, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:548
pa_context_set_card_profile_by_name = _lib.pa_context_set_card_profile_by_name
pa_context_set_card_profile_by_name.restype = POINTER(pa_operation)
pa_context_set_card_profile_by_name.argtypes = [POINTER(pa_context), c_char_p, c_char_p, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:551
pa_context_set_port_latency_offset = _lib.pa_context_set_port_latency_offset
pa_context_set_port_latency_offset.restype = POINTER(pa_operation)
pa_context_set_port_latency_offset.argtypes = [POINTER(pa_context), c_char_p, c_char_p, c_int64, pa_context_success_cb_t, POINTER(None)]

class struct_pa_sink_input_info(Structure):
    __slots__ = [
        'index',
        'name',
        'owner_module',
        'client',
        'sink',
        'sample_spec',
        'channel_map',
        'volume',
        'buffer_usec',
        'sink_usec',
        'resample_method',
        'driver',
        'mute',
        'proplist',
        'corked',
        'has_volume',
        'volume_writable',
        'format',
    ]
struct_pa_sink_input_info._fields_ = [
    ('index', c_uint32),
    ('name', c_char_p),
    ('owner_module', c_uint32),
    ('client', c_uint32),
    ('sink', c_uint32),
    ('sample_spec', pa_sample_spec),
    ('channel_map', pa_channel_map),
    ('volume', pa_cvolume),
    ('buffer_usec', pa_usec_t),
    ('sink_usec', pa_usec_t),
    ('resample_method', c_char_p),
    ('driver', c_char_p),
    ('mute', c_int),
    ('proplist', POINTER(pa_proplist)),
    ('corked', c_int),
    ('has_volume', c_int),
    ('volume_writable', c_int),
    ('format', POINTER(pa_format_info)),
]

pa_sink_input_info = struct_pa_sink_input_info 	# /usr/include/pulse/introspect.h:579
pa_sink_input_info_cb_t = CFUNCTYPE(None, POINTER(pa_context), POINTER(pa_sink_input_info), c_int, POINTER(None)) 	# /usr/include/pulse/introspect.h:582
# /usr/include/pulse/introspect.h:585
pa_context_get_sink_input_info = _lib.pa_context_get_sink_input_info
pa_context_get_sink_input_info.restype = POINTER(pa_operation)
pa_context_get_sink_input_info.argtypes = [POINTER(pa_context), c_uint32, pa_sink_input_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:588
pa_context_get_sink_input_info_list = _lib.pa_context_get_sink_input_info_list
pa_context_get_sink_input_info_list.restype = POINTER(pa_operation)
pa_context_get_sink_input_info_list.argtypes = [POINTER(pa_context), pa_sink_input_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:591
pa_context_move_sink_input_by_name = _lib.pa_context_move_sink_input_by_name
pa_context_move_sink_input_by_name.restype = POINTER(pa_operation)
pa_context_move_sink_input_by_name.argtypes = [POINTER(pa_context), c_uint32, c_char_p, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:594
pa_context_move_sink_input_by_index = _lib.pa_context_move_sink_input_by_index
pa_context_move_sink_input_by_index.restype = POINTER(pa_operation)
pa_context_move_sink_input_by_index.argtypes = [POINTER(pa_context), c_uint32, c_uint32, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:597
pa_context_set_sink_input_volume = _lib.pa_context_set_sink_input_volume
pa_context_set_sink_input_volume.restype = POINTER(pa_operation)
pa_context_set_sink_input_volume.argtypes = [POINTER(pa_context), c_uint32, POINTER(pa_cvolume), pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:600
pa_context_set_sink_input_mute = _lib.pa_context_set_sink_input_mute
pa_context_set_sink_input_mute.restype = POINTER(pa_operation)
pa_context_set_sink_input_mute.argtypes = [POINTER(pa_context), c_uint32, c_int, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:603
pa_context_kill_sink_input = _lib.pa_context_kill_sink_input
pa_context_kill_sink_input.restype = POINTER(pa_operation)
pa_context_kill_sink_input.argtypes = [POINTER(pa_context), c_uint32, pa_context_success_cb_t, POINTER(None)]

class struct_pa_source_output_info(Structure):
    __slots__ = [
        'index',
        'name',
        'owner_module',
        'client',
        'source',
        'sample_spec',
        'channel_map',
        'buffer_usec',
        'source_usec',
        'resample_method',
        'driver',
        'proplist',
        'corked',
        'volume',
        'mute',
        'has_volume',
        'volume_writable',
        'format',
    ]
struct_pa_source_output_info._fields_ = [
    ('index', c_uint32),
    ('name', c_char_p),
    ('owner_module', c_uint32),
    ('client', c_uint32),
    ('source', c_uint32),
    ('sample_spec', pa_sample_spec),
    ('channel_map', pa_channel_map),
    ('buffer_usec', pa_usec_t),
    ('source_usec', pa_usec_t),
    ('resample_method', c_char_p),
    ('driver', c_char_p),
    ('proplist', POINTER(pa_proplist)),
    ('corked', c_int),
    ('volume', pa_cvolume),
    ('mute', c_int),
    ('has_volume', c_int),
    ('volume_writable', c_int),
    ('format', POINTER(pa_format_info)),
]

pa_source_output_info = struct_pa_source_output_info 	# /usr/include/pulse/introspect.h:631
pa_source_output_info_cb_t = CFUNCTYPE(None, POINTER(pa_context), POINTER(pa_source_output_info), c_int, POINTER(None)) 	# /usr/include/pulse/introspect.h:634
# /usr/include/pulse/introspect.h:637
pa_context_get_source_output_info = _lib.pa_context_get_source_output_info
pa_context_get_source_output_info.restype = POINTER(pa_operation)
pa_context_get_source_output_info.argtypes = [POINTER(pa_context), c_uint32, pa_source_output_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:640
pa_context_get_source_output_info_list = _lib.pa_context_get_source_output_info_list
pa_context_get_source_output_info_list.restype = POINTER(pa_operation)
pa_context_get_source_output_info_list.argtypes = [POINTER(pa_context), pa_source_output_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:643
pa_context_move_source_output_by_name = _lib.pa_context_move_source_output_by_name
pa_context_move_source_output_by_name.restype = POINTER(pa_operation)
pa_context_move_source_output_by_name.argtypes = [POINTER(pa_context), c_uint32, c_char_p, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:646
pa_context_move_source_output_by_index = _lib.pa_context_move_source_output_by_index
pa_context_move_source_output_by_index.restype = POINTER(pa_operation)
pa_context_move_source_output_by_index.argtypes = [POINTER(pa_context), c_uint32, c_uint32, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:649
pa_context_set_source_output_volume = _lib.pa_context_set_source_output_volume
pa_context_set_source_output_volume.restype = POINTER(pa_operation)
pa_context_set_source_output_volume.argtypes = [POINTER(pa_context), c_uint32, POINTER(pa_cvolume), pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:652
pa_context_set_source_output_mute = _lib.pa_context_set_source_output_mute
pa_context_set_source_output_mute.restype = POINTER(pa_operation)
pa_context_set_source_output_mute.argtypes = [POINTER(pa_context), c_uint32, c_int, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:655
pa_context_kill_source_output = _lib.pa_context_kill_source_output
pa_context_kill_source_output.restype = POINTER(pa_operation)
pa_context_kill_source_output.argtypes = [POINTER(pa_context), c_uint32, pa_context_success_cb_t, POINTER(None)]

class struct_pa_stat_info(Structure):
    __slots__ = [
        'memblock_total',
        'memblock_total_size',
        'memblock_allocated',
        'memblock_allocated_size',
        'scache_size',
    ]
struct_pa_stat_info._fields_ = [
    ('memblock_total', c_uint32),
    ('memblock_total_size', c_uint32),
    ('memblock_allocated', c_uint32),
    ('memblock_allocated_size', c_uint32),
    ('scache_size', c_uint32),
]

pa_stat_info = struct_pa_stat_info 	# /usr/include/pulse/introspect.h:670
pa_stat_info_cb_t = CFUNCTYPE(None, POINTER(pa_context), POINTER(pa_stat_info), POINTER(None)) 	# /usr/include/pulse/introspect.h:673
# /usr/include/pulse/introspect.h:676
pa_context_stat = _lib.pa_context_stat
pa_context_stat.restype = POINTER(pa_operation)
pa_context_stat.argtypes = [POINTER(pa_context), pa_stat_info_cb_t, POINTER(None)]

class struct_pa_sample_info(Structure):
    __slots__ = [
        'index',
        'name',
        'volume',
        'sample_spec',
        'channel_map',
        'duration',
        'bytes',
        'lazy',
        'filename',
        'proplist',
    ]
struct_pa_sample_info._fields_ = [
    ('index', c_uint32),
    ('name', c_char_p),
    ('volume', pa_cvolume),
    ('sample_spec', pa_sample_spec),
    ('channel_map', pa_channel_map),
    ('duration', pa_usec_t),
    ('bytes', c_uint32),
    ('lazy', c_int),
    ('filename', c_char_p),
    ('proplist', POINTER(pa_proplist)),
]

pa_sample_info = struct_pa_sample_info 	# /usr/include/pulse/introspect.h:696
pa_sample_info_cb_t = CFUNCTYPE(None, POINTER(pa_context), POINTER(pa_sample_info), c_int, POINTER(None)) 	# /usr/include/pulse/introspect.h:699
# /usr/include/pulse/introspect.h:702
pa_context_get_sample_info_by_name = _lib.pa_context_get_sample_info_by_name
pa_context_get_sample_info_by_name.restype = POINTER(pa_operation)
pa_context_get_sample_info_by_name.argtypes = [POINTER(pa_context), c_char_p, pa_sample_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:705
pa_context_get_sample_info_by_index = _lib.pa_context_get_sample_info_by_index
pa_context_get_sample_info_by_index.restype = POINTER(pa_operation)
pa_context_get_sample_info_by_index.argtypes = [POINTER(pa_context), c_uint32, pa_sample_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:708
pa_context_get_sample_info_list = _lib.pa_context_get_sample_info_list
pa_context_get_sample_info_list.restype = POINTER(pa_operation)
pa_context_get_sample_info_list.argtypes = [POINTER(pa_context), pa_sample_info_cb_t, POINTER(None)]

enum_pa_autoload_type = c_int
PA_AUTOLOAD_SINK = 0
PA_AUTOLOAD_SOURCE = 1
pa_autoload_type_t = enum_pa_autoload_type 	# /usr/include/pulse/introspect.h:720
class struct_pa_autoload_info(Structure):
    __slots__ = [
        'index',
        'name',
        'type',
        'module',
        'argument',
    ]
struct_pa_autoload_info._fields_ = [
    ('index', c_uint32),
    ('name', c_char_p),
    ('type', pa_autoload_type_t),
    ('module', c_char_p),
    ('argument', c_char_p),
]

pa_autoload_info = struct_pa_autoload_info 	# /usr/include/pulse/introspect.h:731
pa_autoload_info_cb_t = CFUNCTYPE(None, POINTER(pa_context), POINTER(pa_autoload_info), c_int, POINTER(None)) 	# /usr/include/pulse/introspect.h:734
# /usr/include/pulse/introspect.h:737
pa_context_get_autoload_info_by_name = _lib.pa_context_get_autoload_info_by_name
pa_context_get_autoload_info_by_name.restype = POINTER(pa_operation)
pa_context_get_autoload_info_by_name.argtypes = [POINTER(pa_context), c_char_p, pa_autoload_type_t, pa_autoload_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:740
pa_context_get_autoload_info_by_index = _lib.pa_context_get_autoload_info_by_index
pa_context_get_autoload_info_by_index.restype = POINTER(pa_operation)
pa_context_get_autoload_info_by_index.argtypes = [POINTER(pa_context), c_uint32, pa_autoload_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:743
pa_context_get_autoload_info_list = _lib.pa_context_get_autoload_info_list
pa_context_get_autoload_info_list.restype = POINTER(pa_operation)
pa_context_get_autoload_info_list.argtypes = [POINTER(pa_context), pa_autoload_info_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:746
pa_context_add_autoload = _lib.pa_context_add_autoload
pa_context_add_autoload.restype = POINTER(pa_operation)
pa_context_add_autoload.argtypes = [POINTER(pa_context), c_char_p, pa_autoload_type_t, c_char_p, c_char_p, pa_context_index_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:749
pa_context_remove_autoload_by_name = _lib.pa_context_remove_autoload_by_name
pa_context_remove_autoload_by_name.restype = POINTER(pa_operation)
pa_context_remove_autoload_by_name.argtypes = [POINTER(pa_context), c_char_p, pa_autoload_type_t, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/introspect.h:752
pa_context_remove_autoload_by_index = _lib.pa_context_remove_autoload_by_index
pa_context_remove_autoload_by_index.restype = POINTER(pa_operation)
pa_context_remove_autoload_by_index.argtypes = [POINTER(pa_context), c_uint32, pa_context_success_cb_t, POINTER(None)]

pa_context_subscribe_cb_t = CFUNCTYPE(None, POINTER(pa_context), pa_subscription_event_type_t, c_uint32, POINTER(None)) 	# /usr/include/pulse/subscribe.h:73
# /usr/include/pulse/subscribe.h:76
pa_context_subscribe = _lib.pa_context_subscribe
pa_context_subscribe.restype = POINTER(pa_operation)
pa_context_subscribe.argtypes = [POINTER(pa_context), pa_subscription_mask_t, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/subscribe.h:79
pa_context_set_subscribe_callback = _lib.pa_context_set_subscribe_callback
pa_context_set_subscribe_callback.restype = None
pa_context_set_subscribe_callback.argtypes = [POINTER(pa_context), pa_context_subscribe_cb_t, POINTER(None)]

pa_context_play_sample_cb_t = CFUNCTYPE(None, POINTER(pa_context), c_uint32, POINTER(None)) 	# /usr/include/pulse/scache.h:85
# /usr/include/pulse/scache.h:88
pa_stream_connect_upload = _lib.pa_stream_connect_upload
pa_stream_connect_upload.restype = c_int
pa_stream_connect_upload.argtypes = [POINTER(pa_stream), c_size_t]

# /usr/include/pulse/scache.h:93
pa_stream_finish_upload = _lib.pa_stream_finish_upload
pa_stream_finish_upload.restype = c_int
pa_stream_finish_upload.argtypes = [POINTER(pa_stream)]

# /usr/include/pulse/scache.h:96
pa_context_remove_sample = _lib.pa_context_remove_sample
pa_context_remove_sample.restype = POINTER(pa_operation)
pa_context_remove_sample.argtypes = [POINTER(pa_context), c_char_p, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/scache.h:101
pa_context_play_sample = _lib.pa_context_play_sample
pa_context_play_sample.restype = POINTER(pa_operation)
pa_context_play_sample.argtypes = [POINTER(pa_context), c_char_p, c_char_p, pa_volume_t, pa_context_success_cb_t, POINTER(None)]

# /usr/include/pulse/scache.h:113
pa_context_play_sample_with_proplist = _lib.pa_context_play_sample_with_proplist
pa_context_play_sample_with_proplist.restype = POINTER(pa_operation)
pa_context_play_sample_with_proplist.argtypes = [POINTER(pa_context), c_char_p, c_char_p, pa_volume_t, POINTER(pa_proplist), pa_context_play_sample_cb_t, POINTER(None)]

# /usr/include/pulse/error.h:33
pa_strerror = _lib.pa_strerror
pa_strerror.restype = c_char_p
pa_strerror.argtypes = [c_int]

# /usr/include/pulse/xmalloc.h:39
pa_xmalloc = _lib.pa_xmalloc
pa_xmalloc.restype = POINTER(c_void)
pa_xmalloc.argtypes = [c_size_t]

# /usr/include/pulse/xmalloc.h:42
pa_xmalloc0 = _lib.pa_xmalloc0
pa_xmalloc0.restype = POINTER(c_void)
pa_xmalloc0.argtypes = [c_size_t]

# /usr/include/pulse/xmalloc.h:45
pa_xrealloc = _lib.pa_xrealloc
pa_xrealloc.restype = POINTER(c_void)
pa_xrealloc.argtypes = [POINTER(None), c_size_t]

# /usr/include/pulse/xmalloc.h:48
pa_xfree = _lib.pa_xfree
pa_xfree.restype = None
pa_xfree.argtypes = [POINTER(None)]

# /usr/include/pulse/xmalloc.h:51
pa_xstrdup = _lib.pa_xstrdup
pa_xstrdup.restype = c_char_p
pa_xstrdup.argtypes = [c_char_p]

# /usr/include/pulse/xmalloc.h:54
pa_xstrndup = _lib.pa_xstrndup
pa_xstrndup.restype = c_char_p
pa_xstrndup.argtypes = [c_char_p, c_size_t]

# /usr/include/pulse/xmalloc.h:57
pa_xmemdup = _lib.pa_xmemdup
pa_xmemdup.restype = POINTER(c_void)
pa_xmemdup.argtypes = [POINTER(None), c_size_t]

# /usr/include/pulse/utf8.h:35
pa_utf8_valid = _lib.pa_utf8_valid
pa_utf8_valid.restype = c_char_p
pa_utf8_valid.argtypes = [c_char_p]

# /usr/include/pulse/utf8.h:38
pa_ascii_valid = _lib.pa_ascii_valid
pa_ascii_valid.restype = c_char_p
pa_ascii_valid.argtypes = [c_char_p]

# /usr/include/pulse/utf8.h:41
pa_utf8_filter = _lib.pa_utf8_filter
pa_utf8_filter.restype = c_char_p
pa_utf8_filter.argtypes = [c_char_p]

# /usr/include/pulse/utf8.h:44
pa_ascii_filter = _lib.pa_ascii_filter
pa_ascii_filter.restype = c_char_p
pa_ascii_filter.argtypes = [c_char_p]

# /usr/include/pulse/utf8.h:47
pa_utf8_to_locale = _lib.pa_utf8_to_locale
pa_utf8_to_locale.restype = c_char_p
pa_utf8_to_locale.argtypes = [c_char_p]

# /usr/include/pulse/utf8.h:50
pa_locale_to_utf8 = _lib.pa_locale_to_utf8
pa_locale_to_utf8.restype = c_char_p
pa_locale_to_utf8.argtypes = [c_char_p]

class struct_pa_threaded_mainloop(Structure):
    __slots__ = [
    ]
struct_pa_threaded_mainloop._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_pa_threaded_mainloop(Structure):
    __slots__ = [
    ]
struct_pa_threaded_mainloop._fields_ = [
    ('_opaque_struct', c_int)
]

pa_threaded_mainloop = struct_pa_threaded_mainloop 	# /usr/include/pulse/thread-mainloop.h:246
# /usr/include/pulse/thread-mainloop.h:251
pa_threaded_mainloop_new = _lib.pa_threaded_mainloop_new
pa_threaded_mainloop_new.restype = POINTER(pa_threaded_mainloop)
pa_threaded_mainloop_new.argtypes = []

# /usr/include/pulse/thread-mainloop.h:256
pa_threaded_mainloop_free = _lib.pa_threaded_mainloop_free
pa_threaded_mainloop_free.restype = None
pa_threaded_mainloop_free.argtypes = [POINTER(pa_threaded_mainloop)]

# /usr/include/pulse/thread-mainloop.h:259
pa_threaded_mainloop_start = _lib.pa_threaded_mainloop_start
pa_threaded_mainloop_start.restype = c_int
pa_threaded_mainloop_start.argtypes = [POINTER(pa_threaded_mainloop)]

# /usr/include/pulse/thread-mainloop.h:263
pa_threaded_mainloop_stop = _lib.pa_threaded_mainloop_stop
pa_threaded_mainloop_stop.restype = None
pa_threaded_mainloop_stop.argtypes = [POINTER(pa_threaded_mainloop)]

# /usr/include/pulse/thread-mainloop.h:271
pa_threaded_mainloop_lock = _lib.pa_threaded_mainloop_lock
pa_threaded_mainloop_lock.restype = None
pa_threaded_mainloop_lock.argtypes = [POINTER(pa_threaded_mainloop)]

# /usr/include/pulse/thread-mainloop.h:274
pa_threaded_mainloop_unlock = _lib.pa_threaded_mainloop_unlock
pa_threaded_mainloop_unlock.restype = None
pa_threaded_mainloop_unlock.argtypes = [POINTER(pa_threaded_mainloop)]

# /usr/include/pulse/thread-mainloop.h:285
pa_threaded_mainloop_wait = _lib.pa_threaded_mainloop_wait
pa_threaded_mainloop_wait.restype = None
pa_threaded_mainloop_wait.argtypes = [POINTER(pa_threaded_mainloop)]

# /usr/include/pulse/thread-mainloop.h:292
pa_threaded_mainloop_signal = _lib.pa_threaded_mainloop_signal
pa_threaded_mainloop_signal.restype = None
pa_threaded_mainloop_signal.argtypes = [POINTER(pa_threaded_mainloop), c_int]

# /usr/include/pulse/thread-mainloop.h:298
pa_threaded_mainloop_accept = _lib.pa_threaded_mainloop_accept
pa_threaded_mainloop_accept.restype = None
pa_threaded_mainloop_accept.argtypes = [POINTER(pa_threaded_mainloop)]

# /usr/include/pulse/thread-mainloop.h:302
pa_threaded_mainloop_get_retval = _lib.pa_threaded_mainloop_get_retval
pa_threaded_mainloop_get_retval.restype = c_int
pa_threaded_mainloop_get_retval.argtypes = [POINTER(pa_threaded_mainloop)]

# /usr/include/pulse/thread-mainloop.h:307
pa_threaded_mainloop_get_api = _lib.pa_threaded_mainloop_get_api
pa_threaded_mainloop_get_api.restype = POINTER(pa_mainloop_api)
pa_threaded_mainloop_get_api.argtypes = [POINTER(pa_threaded_mainloop)]

# /usr/include/pulse/thread-mainloop.h:310
pa_threaded_mainloop_in_thread = _lib.pa_threaded_mainloop_in_thread
pa_threaded_mainloop_in_thread.restype = c_int
pa_threaded_mainloop_in_thread.argtypes = [POINTER(pa_threaded_mainloop)]

# /usr/include/pulse/thread-mainloop.h:313
#pa_threaded_mainloop_set_name = _lib.pa_threaded_mainloop_set_name
#pa_threaded_mainloop_set_name.restype = None
#pa_threaded_mainloop_set_name.argtypes = [POINTER(pa_threaded_mainloop), c_char_p]

class struct_pa_mainloop(Structure):
    __slots__ = [
    ]
struct_pa_mainloop._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_pa_mainloop(Structure):
    __slots__ = [
    ]
struct_pa_mainloop._fields_ = [
    ('_opaque_struct', c_int)
]

pa_mainloop = struct_pa_mainloop 	# /usr/include/pulse/mainloop.h:78
# /usr/include/pulse/mainloop.h:81
pa_mainloop_new = _lib.pa_mainloop_new
pa_mainloop_new.restype = POINTER(pa_mainloop)
pa_mainloop_new.argtypes = []

# /usr/include/pulse/mainloop.h:84
pa_mainloop_free = _lib.pa_mainloop_free
pa_mainloop_free.restype = None
pa_mainloop_free.argtypes = [POINTER(pa_mainloop)]

# /usr/include/pulse/mainloop.h:89
pa_mainloop_prepare = _lib.pa_mainloop_prepare
pa_mainloop_prepare.restype = c_int
pa_mainloop_prepare.argtypes = [POINTER(pa_mainloop), c_int]

# /usr/include/pulse/mainloop.h:92
pa_mainloop_poll = _lib.pa_mainloop_poll
pa_mainloop_poll.restype = c_int
pa_mainloop_poll.argtypes = [POINTER(pa_mainloop)]

# /usr/include/pulse/mainloop.h:96
pa_mainloop_dispatch = _lib.pa_mainloop_dispatch
pa_mainloop_dispatch.restype = c_int
pa_mainloop_dispatch.argtypes = [POINTER(pa_mainloop)]

# /usr/include/pulse/mainloop.h:99
pa_mainloop_get_retval = _lib.pa_mainloop_get_retval
pa_mainloop_get_retval.restype = c_int
pa_mainloop_get_retval.argtypes = [POINTER(pa_mainloop)]

# /usr/include/pulse/mainloop.h:107
pa_mainloop_iterate = _lib.pa_mainloop_iterate
pa_mainloop_iterate.restype = c_int
pa_mainloop_iterate.argtypes = [POINTER(pa_mainloop), c_int, POINTER(c_int)]

# /usr/include/pulse/mainloop.h:110
pa_mainloop_run = _lib.pa_mainloop_run
pa_mainloop_run.restype = c_int
pa_mainloop_run.argtypes = [POINTER(pa_mainloop), POINTER(c_int)]

# /usr/include/pulse/mainloop.h:115
pa_mainloop_get_api = _lib.pa_mainloop_get_api
pa_mainloop_get_api.restype = POINTER(pa_mainloop_api)
pa_mainloop_get_api.argtypes = [POINTER(pa_mainloop)]

# /usr/include/pulse/mainloop.h:118
pa_mainloop_quit = _lib.pa_mainloop_quit
pa_mainloop_quit.restype = None
pa_mainloop_quit.argtypes = [POINTER(pa_mainloop), c_int]

# /usr/include/pulse/mainloop.h:121
pa_mainloop_wakeup = _lib.pa_mainloop_wakeup
pa_mainloop_wakeup.restype = None
pa_mainloop_wakeup.argtypes = [POINTER(pa_mainloop)]

class struct_pollfd(Structure):
    __slots__ = [
    ]
struct_pollfd._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_pollfd(Structure):
    __slots__ = [
    ]
struct_pollfd._fields_ = [
    ('_opaque_struct', c_int)
]

pa_poll_func = CFUNCTYPE(c_int, POINTER(struct_pollfd), c_ulong, c_int, POINTER(None)) 	# /usr/include/pulse/mainloop.h:124
# /usr/include/pulse/mainloop.h:127
pa_mainloop_set_poll_func = _lib.pa_mainloop_set_poll_func
pa_mainloop_set_poll_func.restype = None
pa_mainloop_set_poll_func.argtypes = [POINTER(pa_mainloop), pa_poll_func, POINTER(None)]

class struct_pa_signal_event(Structure):
    __slots__ = [
    ]
struct_pa_signal_event._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_pa_signal_event(Structure):
    __slots__ = [
    ]
struct_pa_signal_event._fields_ = [
    ('_opaque_struct', c_int)
]

pa_signal_event = struct_pa_signal_event 	# /usr/include/pulse/mainloop-signal.h:39
pa_signal_cb_t = CFUNCTYPE(None, POINTER(pa_mainloop_api), POINTER(pa_signal_event), c_int, POINTER(None)) 	# /usr/include/pulse/mainloop-signal.h:42
pa_signal_destroy_cb_t = CFUNCTYPE(None, POINTER(pa_mainloop_api), POINTER(pa_signal_event), POINTER(None)) 	# /usr/include/pulse/mainloop-signal.h:45
# /usr/include/pulse/mainloop-signal.h:48
pa_signal_init = _lib.pa_signal_init
pa_signal_init.restype = c_int
pa_signal_init.argtypes = [POINTER(pa_mainloop_api)]

# /usr/include/pulse/mainloop-signal.h:51
pa_signal_done = _lib.pa_signal_done
pa_signal_done.restype = None
pa_signal_done.argtypes = []

# /usr/include/pulse/mainloop-signal.h:54
pa_signal_new = _lib.pa_signal_new
pa_signal_new.restype = POINTER(pa_signal_event)
pa_signal_new.argtypes = [c_int, pa_signal_cb_t, POINTER(None)]

# /usr/include/pulse/mainloop-signal.h:57
pa_signal_free = _lib.pa_signal_free
pa_signal_free.restype = None
pa_signal_free.argtypes = [POINTER(pa_signal_event)]

# /usr/include/pulse/mainloop-signal.h:60
pa_signal_set_destroy = _lib.pa_signal_set_destroy
pa_signal_set_destroy.restype = None
pa_signal_set_destroy.argtypes = [POINTER(pa_signal_event), pa_signal_destroy_cb_t]

# /usr/include/pulse/util.h:35
pa_get_user_name = _lib.pa_get_user_name
pa_get_user_name.restype = c_char_p
pa_get_user_name.argtypes = [c_char_p, c_size_t]

# /usr/include/pulse/util.h:38
pa_get_host_name = _lib.pa_get_host_name
pa_get_host_name.restype = c_char_p
pa_get_host_name.argtypes = [c_char_p, c_size_t]

# /usr/include/pulse/util.h:41
pa_get_fqdn = _lib.pa_get_fqdn
pa_get_fqdn.restype = c_char_p
pa_get_fqdn.argtypes = [c_char_p, c_size_t]

# /usr/include/pulse/util.h:44
pa_get_home_dir = _lib.pa_get_home_dir
pa_get_home_dir.restype = c_char_p
pa_get_home_dir.argtypes = [c_char_p, c_size_t]

# /usr/include/pulse/util.h:48
pa_get_binary_name = _lib.pa_get_binary_name
pa_get_binary_name.restype = c_char_p
pa_get_binary_name.argtypes = [c_char_p, c_size_t]

# /usr/include/pulse/util.h:52
pa_path_get_filename = _lib.pa_path_get_filename
pa_path_get_filename.restype = c_char_p
pa_path_get_filename.argtypes = [c_char_p]

# /usr/include/pulse/util.h:55
pa_msleep = _lib.pa_msleep
pa_msleep.restype = c_int
pa_msleep.argtypes = [c_ulong]

class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

# /usr/include/pulse/timeval.h:61
pa_gettimeofday = _lib.pa_gettimeofday
pa_gettimeofday.restype = POINTER(struct_timeval)
pa_gettimeofday.argtypes = [POINTER(struct_timeval)]

class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

# /usr/include/pulse/timeval.h:65
pa_timeval_diff = _lib.pa_timeval_diff
pa_timeval_diff.restype = pa_usec_t
pa_timeval_diff.argtypes = [POINTER(struct_timeval), POINTER(struct_timeval)]

class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

# /usr/include/pulse/timeval.h:68
pa_timeval_cmp = _lib.pa_timeval_cmp
pa_timeval_cmp.restype = c_int
pa_timeval_cmp.argtypes = [POINTER(struct_timeval), POINTER(struct_timeval)]

class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

# /usr/include/pulse/timeval.h:71
pa_timeval_age = _lib.pa_timeval_age
pa_timeval_age.restype = pa_usec_t
pa_timeval_age.argtypes = [POINTER(struct_timeval)]

class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

# /usr/include/pulse/timeval.h:74
pa_timeval_add = _lib.pa_timeval_add
pa_timeval_add.restype = POINTER(struct_timeval)
pa_timeval_add.argtypes = [POINTER(struct_timeval), pa_usec_t]

class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

# /usr/include/pulse/timeval.h:77
pa_timeval_sub = _lib.pa_timeval_sub
pa_timeval_sub.restype = POINTER(struct_timeval)
pa_timeval_sub.argtypes = [POINTER(struct_timeval), pa_usec_t]

class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

# /usr/include/pulse/timeval.h:80
pa_timeval_store = _lib.pa_timeval_store
pa_timeval_store.restype = POINTER(struct_timeval)
pa_timeval_store.argtypes = [POINTER(struct_timeval), pa_usec_t]

class struct_timeval(Structure):
    __slots__ = [
    ]
struct_timeval._fields_ = [
    ('_opaque_struct', c_int)
]

# /usr/include/pulse/timeval.h:83
pa_timeval_load = _lib.pa_timeval_load
pa_timeval_load.restype = pa_usec_t
pa_timeval_load.argtypes = [POINTER(struct_timeval)]


__all__ = ['pa_get_library_version', 'PA_API_VERSION', 'PA_PROTOCOL_VERSION',
'PA_MAJOR', 'PA_MINOR', 'PA_MICRO', 'PA_CHANNELS_MAX', 'PA_RATE_MAX',
'pa_sample_format_t', 'PA_SAMPLE_U8', 'PA_SAMPLE_ALAW', 'PA_SAMPLE_ULAW',
'PA_SAMPLE_S16LE', 'PA_SAMPLE_S16BE', 'PA_SAMPLE_FLOAT32LE',
'PA_SAMPLE_FLOAT32BE', 'PA_SAMPLE_S32LE', 'PA_SAMPLE_S32BE',
'PA_SAMPLE_S24LE', 'PA_SAMPLE_S24BE', 'PA_SAMPLE_S24_32LE',
'PA_SAMPLE_S24_32BE', 'PA_SAMPLE_MAX', 'PA_SAMPLE_INVALID', 'pa_sample_spec',
'pa_usec_t', 'pa_bytes_per_second', 'pa_frame_size', 'pa_sample_size',
'pa_sample_size_of_format', 'pa_bytes_to_usec', 'pa_usec_to_bytes',
'pa_sample_spec_init', 'pa_sample_format_valid', 'pa_sample_rate_valid',
'pa_channels_valid', 'pa_sample_spec_valid', 'pa_sample_spec_equal',
'pa_sample_format_to_string', 'pa_parse_sample_format',
'PA_SAMPLE_SPEC_SNPRINT_MAX', 'pa_sample_spec_snprint',
'PA_BYTES_SNPRINT_MAX', 'pa_bytes_snprint', 'pa_sample_format_is_le',
'pa_sample_format_is_be', 'pa_context_state_t', 'PA_CONTEXT_UNCONNECTED',
'PA_CONTEXT_CONNECTING', 'PA_CONTEXT_AUTHORIZING', 'PA_CONTEXT_SETTING_NAME',
'PA_CONTEXT_READY', 'PA_CONTEXT_FAILED', 'PA_CONTEXT_TERMINATED',
'pa_stream_state_t', 'PA_STREAM_UNCONNECTED', 'PA_STREAM_CREATING',
'PA_STREAM_READY', 'PA_STREAM_FAILED', 'PA_STREAM_TERMINATED',
'pa_operation_state_t', 'PA_OPERATION_RUNNING', 'PA_OPERATION_DONE',
'PA_OPERATION_CANCELLED', 'pa_context_flags_t', 'PA_CONTEXT_NOFLAGS',
'PA_CONTEXT_NOAUTOSPAWN', 'PA_CONTEXT_NOFAIL', 'pa_direction_t',
'PA_DIRECTION_OUTPUT', 'PA_DIRECTION_INPUT', 'pa_device_type_t',
'PA_DEVICE_TYPE_SINK', 'PA_DEVICE_TYPE_SOURCE', 'pa_stream_direction_t',
'PA_STREAM_NODIRECTION', 'PA_STREAM_PLAYBACK', 'PA_STREAM_RECORD',
'PA_STREAM_UPLOAD', 'pa_stream_flags_t', 'PA_STREAM_NOFLAGS',
'PA_STREAM_START_CORKED', 'PA_STREAM_INTERPOLATE_TIMING',
'PA_STREAM_NOT_MONOTONIC', 'PA_STREAM_AUTO_TIMING_UPDATE',
'PA_STREAM_NO_REMAP_CHANNELS', 'PA_STREAM_NO_REMIX_CHANNELS',
'PA_STREAM_FIX_FORMAT', 'PA_STREAM_FIX_RATE', 'PA_STREAM_FIX_CHANNELS',
'PA_STREAM_DONT_MOVE', 'PA_STREAM_VARIABLE_RATE', 'PA_STREAM_PEAK_DETECT',
'PA_STREAM_START_MUTED', 'PA_STREAM_ADJUST_LATENCY',
'PA_STREAM_EARLY_REQUESTS', 'PA_STREAM_DONT_INHIBIT_AUTO_SUSPEND',
'PA_STREAM_START_UNMUTED', 'PA_STREAM_FAIL_ON_SUSPEND',
'PA_STREAM_RELATIVE_VOLUME', 'PA_STREAM_PASSTHROUGH', 'pa_buffer_attr',
'pa_error_code_t', 'PA_OK', 'PA_ERR_ACCESS', 'PA_ERR_COMMAND',
'PA_ERR_INVALID', 'PA_ERR_EXIST', 'PA_ERR_NOENTITY',
'PA_ERR_CONNECTIONREFUSED', 'PA_ERR_PROTOCOL', 'PA_ERR_TIMEOUT',
'PA_ERR_AUTHKEY', 'PA_ERR_INTERNAL', 'PA_ERR_CONNECTIONTERMINATED',
'PA_ERR_KILLED', 'PA_ERR_INVALIDSERVER', 'PA_ERR_MODINITFAILED',
'PA_ERR_BADSTATE', 'PA_ERR_NODATA', 'PA_ERR_VERSION', 'PA_ERR_TOOLARGE',
'PA_ERR_NOTSUPPORTED', 'PA_ERR_UNKNOWN', 'PA_ERR_NOEXTENSION',
'PA_ERR_OBSOLETE', 'PA_ERR_NOTIMPLEMENTED', 'PA_ERR_FORKED', 'PA_ERR_IO',
'PA_ERR_BUSY', 'PA_ERR_MAX', 'pa_subscription_mask_t',
'PA_SUBSCRIPTION_MASK_NULL', 'PA_SUBSCRIPTION_MASK_SINK',
'PA_SUBSCRIPTION_MASK_SOURCE', 'PA_SUBSCRIPTION_MASK_SINK_INPUT',
'PA_SUBSCRIPTION_MASK_SOURCE_OUTPUT', 'PA_SUBSCRIPTION_MASK_MODULE',
'PA_SUBSCRIPTION_MASK_CLIENT', 'PA_SUBSCRIPTION_MASK_SAMPLE_CACHE',
'PA_SUBSCRIPTION_MASK_SERVER', 'PA_SUBSCRIPTION_MASK_AUTOLOAD',
'PA_SUBSCRIPTION_MASK_CARD', 'PA_SUBSCRIPTION_MASK_ALL',
'pa_subscription_event_type_t', 'PA_SUBSCRIPTION_EVENT_SINK',
'PA_SUBSCRIPTION_EVENT_SOURCE', 'PA_SUBSCRIPTION_EVENT_SINK_INPUT',
'PA_SUBSCRIPTION_EVENT_SOURCE_OUTPUT', 'PA_SUBSCRIPTION_EVENT_MODULE',
'PA_SUBSCRIPTION_EVENT_CLIENT', 'PA_SUBSCRIPTION_EVENT_SAMPLE_CACHE',
'PA_SUBSCRIPTION_EVENT_SERVER', 'PA_SUBSCRIPTION_EVENT_AUTOLOAD',
'PA_SUBSCRIPTION_EVENT_CARD', 'PA_SUBSCRIPTION_EVENT_FACILITY_MASK',
'PA_SUBSCRIPTION_EVENT_NEW', 'PA_SUBSCRIPTION_EVENT_CHANGE',
'PA_SUBSCRIPTION_EVENT_REMOVE', 'PA_SUBSCRIPTION_EVENT_TYPE_MASK',
'pa_timing_info', 'pa_spawn_api', 'pa_seek_mode_t', 'PA_SEEK_RELATIVE',
'PA_SEEK_ABSOLUTE', 'PA_SEEK_RELATIVE_ON_READ', 'PA_SEEK_RELATIVE_END',
'pa_sink_flags_t', 'PA_SINK_NOFLAGS', 'PA_SINK_HW_VOLUME_CTRL',
'PA_SINK_LATENCY', 'PA_SINK_HARDWARE', 'PA_SINK_NETWORK',
'PA_SINK_HW_MUTE_CTRL', 'PA_SINK_DECIBEL_VOLUME', 'PA_SINK_FLAT_VOLUME',
'PA_SINK_DYNAMIC_LATENCY', 'PA_SINK_SET_FORMATS', 'pa_sink_state_t',
'PA_SINK_INVALID_STATE', 'PA_SINK_RUNNING', 'PA_SINK_IDLE',
'PA_SINK_SUSPENDED', 'PA_SINK_INIT', 'PA_SINK_UNLINKED', 'pa_source_flags_t',
'PA_SOURCE_NOFLAGS', 'PA_SOURCE_HW_VOLUME_CTRL', 'PA_SOURCE_LATENCY',
'PA_SOURCE_HARDWARE', 'PA_SOURCE_NETWORK', 'PA_SOURCE_HW_MUTE_CTRL',
'PA_SOURCE_DECIBEL_VOLUME', 'PA_SOURCE_DYNAMIC_LATENCY',
'PA_SOURCE_FLAT_VOLUME', 'pa_source_state_t', 'PA_SOURCE_INVALID_STATE',
'PA_SOURCE_RUNNING', 'PA_SOURCE_IDLE', 'PA_SOURCE_SUSPENDED',
'PA_SOURCE_INIT', 'PA_SOURCE_UNLINKED', 'pa_free_cb_t', 'pa_port_available_t',
'PA_PORT_AVAILABLE_UNKNOWN', 'PA_PORT_AVAILABLE_NO', 'PA_PORT_AVAILABLE_YES',
'pa_mainloop_api', 'pa_io_event_flags_t', 'PA_IO_EVENT_NULL',
'PA_IO_EVENT_INPUT', 'PA_IO_EVENT_OUTPUT', 'PA_IO_EVENT_HANGUP',
'PA_IO_EVENT_ERROR', 'pa_io_event', 'pa_io_event_cb_t',
'pa_io_event_destroy_cb_t', 'pa_time_event', 'pa_time_event_cb_t',
'pa_time_event_destroy_cb_t', 'pa_defer_event', 'pa_defer_event_cb_t',
'pa_defer_event_destroy_cb_t', 'pa_mainloop_api_once',
'pa_channel_position_t', 'PA_CHANNEL_POSITION_INVALID',
'PA_CHANNEL_POSITION_MONO', 'PA_CHANNEL_POSITION_FRONT_LEFT',
'PA_CHANNEL_POSITION_FRONT_RIGHT', 'PA_CHANNEL_POSITION_FRONT_CENTER',
'PA_CHANNEL_POSITION_LEFT', 'PA_CHANNEL_POSITION_RIGHT',
'PA_CHANNEL_POSITION_CENTER', 'PA_CHANNEL_POSITION_REAR_CENTER',
'PA_CHANNEL_POSITION_REAR_LEFT', 'PA_CHANNEL_POSITION_REAR_RIGHT',
'PA_CHANNEL_POSITION_LFE', 'PA_CHANNEL_POSITION_SUBWOOFER',
'PA_CHANNEL_POSITION_FRONT_LEFT_OF_CENTER',
'PA_CHANNEL_POSITION_FRONT_RIGHT_OF_CENTER', 'PA_CHANNEL_POSITION_SIDE_LEFT',
'PA_CHANNEL_POSITION_SIDE_RIGHT', 'PA_CHANNEL_POSITION_AUX0',
'PA_CHANNEL_POSITION_AUX1', 'PA_CHANNEL_POSITION_AUX2',
'PA_CHANNEL_POSITION_AUX3', 'PA_CHANNEL_POSITION_AUX4',
'PA_CHANNEL_POSITION_AUX5', 'PA_CHANNEL_POSITION_AUX6',
'PA_CHANNEL_POSITION_AUX7', 'PA_CHANNEL_POSITION_AUX8',
'PA_CHANNEL_POSITION_AUX9', 'PA_CHANNEL_POSITION_AUX10',
'PA_CHANNEL_POSITION_AUX11', 'PA_CHANNEL_POSITION_AUX12',
'PA_CHANNEL_POSITION_AUX13', 'PA_CHANNEL_POSITION_AUX14',
'PA_CHANNEL_POSITION_AUX15', 'PA_CHANNEL_POSITION_AUX16',
'PA_CHANNEL_POSITION_AUX17', 'PA_CHANNEL_POSITION_AUX18',
'PA_CHANNEL_POSITION_AUX19', 'PA_CHANNEL_POSITION_AUX20',
'PA_CHANNEL_POSITION_AUX21', 'PA_CHANNEL_POSITION_AUX22',
'PA_CHANNEL_POSITION_AUX23', 'PA_CHANNEL_POSITION_AUX24',
'PA_CHANNEL_POSITION_AUX25', 'PA_CHANNEL_POSITION_AUX26',
'PA_CHANNEL_POSITION_AUX27', 'PA_CHANNEL_POSITION_AUX28',
'PA_CHANNEL_POSITION_AUX29', 'PA_CHANNEL_POSITION_AUX30',
'PA_CHANNEL_POSITION_AUX31', 'PA_CHANNEL_POSITION_TOP_CENTER',
'PA_CHANNEL_POSITION_TOP_FRONT_LEFT', 'PA_CHANNEL_POSITION_TOP_FRONT_RIGHT',
'PA_CHANNEL_POSITION_TOP_FRONT_CENTER', 'PA_CHANNEL_POSITION_TOP_REAR_LEFT',
'PA_CHANNEL_POSITION_TOP_REAR_RIGHT', 'PA_CHANNEL_POSITION_TOP_REAR_CENTER',
'PA_CHANNEL_POSITION_MAX', 'pa_channel_position_mask_t',
'pa_channel_map_def_t', 'PA_CHANNEL_MAP_AIFF', 'PA_CHANNEL_MAP_ALSA',
'PA_CHANNEL_MAP_AUX', 'PA_CHANNEL_MAP_WAVEEX', 'PA_CHANNEL_MAP_OSS',
'PA_CHANNEL_MAP_DEF_MAX', 'PA_CHANNEL_MAP_DEFAULT', 'pa_channel_map',
'pa_channel_map_init', 'pa_channel_map_init_mono',
'pa_channel_map_init_stereo', 'pa_channel_map_init_auto',
'pa_channel_map_init_extend', 'pa_channel_position_to_string',
'pa_channel_position_from_string', 'pa_channel_position_to_pretty_string',
'PA_CHANNEL_MAP_SNPRINT_MAX', 'pa_channel_map_snprint',
'pa_channel_map_parse', 'pa_channel_map_equal', 'pa_channel_map_valid',
'pa_channel_map_compatible', 'pa_channel_map_superset',
'pa_channel_map_can_balance', 'pa_channel_map_can_fade',
'pa_channel_map_to_name', 'pa_channel_map_to_pretty_name',
'pa_channel_map_has_position', 'pa_channel_map_mask', 'pa_operation',
'pa_operation_notify_cb_t', 'pa_operation_ref', 'pa_operation_unref',
'pa_operation_cancel', 'pa_operation_get_state',
'pa_operation_set_state_callback', 'pa_context', 'pa_context_notify_cb_t',
'pa_context_success_cb_t', 'pa_context_event_cb_t', 'pa_context_new',
'pa_context_new_with_proplist', 'pa_context_unref', 'pa_context_ref',
'pa_context_set_state_callback', 'pa_context_set_event_callback',
'pa_context_errno', 'pa_context_is_pending', 'pa_context_get_state',
'pa_context_connect', 'pa_context_disconnect', 'pa_context_drain',
'pa_context_exit_daemon', 'pa_context_set_default_sink',
'pa_context_set_default_source', 'pa_context_is_local', 'pa_context_set_name',
'pa_context_get_server', 'pa_context_get_protocol_version',
'pa_context_get_server_protocol_version', 'PA_UPDATE_SET', 'PA_UPDATE_MERGE',
'PA_UPDATE_REPLACE', 'pa_context_proplist_update',
'pa_context_proplist_remove', 'pa_context_get_index', 'pa_context_rttime_new',
'pa_context_rttime_restart', 'pa_context_get_tile_size',
'pa_context_load_cookie_from_file', 'pa_volume_t', 'pa_cvolume',
'pa_cvolume_equal', 'pa_cvolume_init', 'pa_cvolume_set',
'PA_CVOLUME_SNPRINT_MAX', 'pa_cvolume_snprint',
'PA_SW_CVOLUME_SNPRINT_DB_MAX', 'pa_sw_cvolume_snprint_dB',
'PA_CVOLUME_SNPRINT_VERBOSE_MAX', 'pa_cvolume_snprint_verbose',
'PA_VOLUME_SNPRINT_MAX', 'pa_volume_snprint', 'PA_SW_VOLUME_SNPRINT_DB_MAX',
'pa_sw_volume_snprint_dB', 'PA_VOLUME_SNPRINT_VERBOSE_MAX',
'pa_volume_snprint_verbose', 'pa_cvolume_avg', 'pa_cvolume_avg_mask',
'pa_cvolume_max', 'pa_cvolume_max_mask', 'pa_cvolume_min',
'pa_cvolume_min_mask', 'pa_cvolume_valid', 'pa_cvolume_channels_equal_to',
'pa_sw_volume_multiply', 'pa_sw_cvolume_multiply',
'pa_sw_cvolume_multiply_scalar', 'pa_sw_volume_divide',
'pa_sw_cvolume_divide', 'pa_sw_cvolume_divide_scalar', 'pa_sw_volume_from_dB',
'pa_sw_volume_to_dB', 'pa_sw_volume_from_linear', 'pa_sw_volume_to_linear',
'pa_cvolume_remap', 'pa_cvolume_compatible',
'pa_cvolume_compatible_with_channel_map', 'pa_cvolume_get_balance',
'pa_cvolume_set_balance', 'pa_cvolume_get_fade', 'pa_cvolume_set_fade',
'pa_cvolume_scale', 'pa_cvolume_scale_mask', 'pa_cvolume_set_position',
'pa_cvolume_get_position', 'pa_cvolume_merge', 'pa_cvolume_inc_clamp',
'pa_cvolume_inc', 'pa_cvolume_dec', 'pa_stream', 'pa_stream_success_cb_t',
'pa_stream_request_cb_t', 'pa_stream_notify_cb_t', 'pa_stream_event_cb_t',
'pa_stream_new', 'pa_stream_new_with_proplist', 'PA_ENCODING_ANY',
'PA_ENCODING_PCM', 'PA_ENCODING_AC3_IEC61937', 'PA_ENCODING_EAC3_IEC61937',
'PA_ENCODING_MPEG_IEC61937', 'PA_ENCODING_DTS_IEC61937',
'PA_ENCODING_MPEG2_AAC_IEC61937', 'PA_ENCODING_MAX', 'PA_ENCODING_INVALID',
'pa_stream_new_extended', 'pa_stream_unref', 'pa_stream_ref',
'pa_stream_get_state', 'pa_stream_get_context', 'pa_stream_get_index',
'pa_stream_get_device_index', 'pa_stream_get_device_name',
'pa_stream_is_suspended', 'pa_stream_is_corked', 'pa_stream_connect_playback',
'pa_stream_connect_record', 'pa_stream_disconnect', 'pa_stream_begin_write',
'pa_stream_cancel_write', 'pa_stream_write', 'pa_stream_write_ext_free',
'pa_stream_peek', 'pa_stream_drop', 'pa_stream_writable_size',
'pa_stream_readable_size', 'pa_stream_drain', 'pa_stream_update_timing_info',
'pa_stream_set_state_callback', 'pa_stream_set_write_callback',
'pa_stream_set_read_callback', 'pa_stream_set_overflow_callback',
'pa_stream_get_underflow_index', 'pa_stream_set_underflow_callback',
'pa_stream_set_started_callback', 'pa_stream_set_latency_update_callback',
'pa_stream_set_moved_callback', 'pa_stream_set_suspended_callback',
'pa_stream_set_event_callback', 'pa_stream_set_buffer_attr_callback',
'pa_stream_cork', 'pa_stream_flush', 'pa_stream_prebuf', 'pa_stream_trigger',
'pa_stream_set_name', 'pa_stream_get_time', 'pa_stream_get_latency',
'pa_stream_get_timing_info', 'pa_stream_get_sample_spec',
'pa_stream_get_channel_map', 'pa_stream_get_format_info',
'pa_stream_get_buffer_attr', 'pa_stream_set_buffer_attr',
'pa_stream_update_sample_rate', 'pa_stream_proplist_update',
'pa_stream_proplist_remove', 'pa_stream_set_monitor_stream',
'pa_stream_get_monitor_stream', 'pa_sink_port_info', 'pa_sink_info',
'pa_sink_info_cb_t', 'pa_context_get_sink_info_by_name',
'pa_context_get_sink_info_by_index', 'pa_context_get_sink_info_list',
'pa_context_set_sink_volume_by_index', 'pa_context_set_sink_volume_by_name',
'pa_context_set_sink_mute_by_index', 'pa_context_set_sink_mute_by_name',
'pa_context_suspend_sink_by_name', 'pa_context_suspend_sink_by_index',
'pa_context_set_sink_port_by_index', 'pa_context_set_sink_port_by_name',
'pa_source_port_info', 'pa_source_info', 'pa_source_info_cb_t',
'pa_context_get_source_info_by_name', 'pa_context_get_source_info_by_index',
'pa_context_get_source_info_list', 'pa_context_set_source_volume_by_index',
'pa_context_set_source_volume_by_name', 'pa_context_set_source_mute_by_index',
'pa_context_set_source_mute_by_name', 'pa_context_suspend_source_by_name',
'pa_context_suspend_source_by_index', 'pa_context_set_source_port_by_index',
'pa_context_set_source_port_by_name', 'pa_server_info', 'pa_server_info_cb_t',
'pa_context_get_server_info', 'pa_module_info', 'pa_module_info_cb_t',
'pa_context_get_module_info', 'pa_context_get_module_info_list',
'pa_context_index_cb_t', 'pa_context_load_module', 'pa_context_unload_module',
'pa_client_info', 'pa_client_info_cb_t', 'pa_context_get_client_info',
'pa_context_get_client_info_list', 'pa_context_kill_client',
'pa_card_profile_info', 'pa_card_profile_info2', 'pa_card_port_info',
'pa_card_info', 'pa_card_info_cb_t', 'pa_context_get_card_info_by_index',
'pa_context_get_card_info_by_name', 'pa_context_get_card_info_list',
'pa_context_set_card_profile_by_index', 'pa_context_set_card_profile_by_name',
'pa_context_set_port_latency_offset', 'pa_sink_input_info',
'pa_sink_input_info_cb_t', 'pa_context_get_sink_input_info',
'pa_context_get_sink_input_info_list', 'pa_context_move_sink_input_by_name',
'pa_context_move_sink_input_by_index', 'pa_context_set_sink_input_volume',
'pa_context_set_sink_input_mute', 'pa_context_kill_sink_input',
'pa_source_output_info', 'pa_source_output_info_cb_t',
'pa_context_get_source_output_info', 'pa_context_get_source_output_info_list',
'pa_context_move_source_output_by_name',
'pa_context_move_source_output_by_index',
'pa_context_set_source_output_volume', 'pa_context_set_source_output_mute',
'pa_context_kill_source_output', 'pa_stat_info', 'pa_stat_info_cb_t',
'pa_context_stat', 'pa_sample_info', 'pa_sample_info_cb_t',
'pa_context_get_sample_info_by_name', 'pa_context_get_sample_info_by_index',
'pa_context_get_sample_info_list', 'pa_autoload_type_t', 'PA_AUTOLOAD_SINK',
'PA_AUTOLOAD_SOURCE', 'pa_autoload_info', 'pa_autoload_info_cb_t',
'pa_context_get_autoload_info_by_name',
'pa_context_get_autoload_info_by_index', 'pa_context_get_autoload_info_list',
'pa_context_add_autoload', 'pa_context_remove_autoload_by_name',
'pa_context_remove_autoload_by_index', 'pa_context_subscribe_cb_t',
'pa_context_subscribe', 'pa_context_set_subscribe_callback',
'pa_context_play_sample_cb_t', 'pa_stream_connect_upload',
'pa_stream_finish_upload', 'pa_context_remove_sample',
'pa_context_play_sample', 'pa_context_play_sample_with_proplist',
'pa_strerror', 'pa_xmalloc', 'pa_xmalloc0', 'pa_xrealloc', 'pa_xfree',
'pa_xstrdup', 'pa_xstrndup', 'pa_xmemdup', '_pa_xnew_internal',
'_pa_xnew0_internal', '_pa_xnewdup_internal', '_pa_xrenew_internal',
'pa_utf8_valid', 'pa_ascii_valid', 'pa_utf8_filter', 'pa_ascii_filter',
'pa_utf8_to_locale', 'pa_locale_to_utf8', 'pa_threaded_mainloop',
'pa_threaded_mainloop_new', 'pa_threaded_mainloop_free',
'pa_threaded_mainloop_start', 'pa_threaded_mainloop_stop',
'pa_threaded_mainloop_lock', 'pa_threaded_mainloop_unlock',
'pa_threaded_mainloop_wait', 'pa_threaded_mainloop_signal',
'pa_threaded_mainloop_accept', 'pa_threaded_mainloop_get_retval',
'pa_threaded_mainloop_get_api', 'pa_threaded_mainloop_in_thread',
'pa_threaded_mainloop_set_name', 'pa_mainloop', 'pa_mainloop_new',
'pa_mainloop_free', 'pa_mainloop_prepare', 'pa_mainloop_poll',
'pa_mainloop_dispatch', 'pa_mainloop_get_retval', 'pa_mainloop_iterate',
'pa_mainloop_run', 'pa_mainloop_get_api', 'pa_mainloop_quit',
'pa_mainloop_wakeup', 'pa_poll_func', 'pa_mainloop_set_poll_func',
'pa_signal_event', 'pa_signal_cb_t', 'pa_signal_destroy_cb_t',
'pa_signal_init', 'pa_signal_done', 'pa_signal_new', 'pa_signal_free',
'pa_signal_set_destroy', 'pa_get_user_name', 'pa_get_host_name',
'pa_get_fqdn', 'pa_get_home_dir', 'pa_get_binary_name',
'pa_path_get_filename', 'pa_msleep', 'pa_gettimeofday', 'pa_timeval_diff',
'pa_timeval_cmp', 'pa_timeval_age', 'pa_timeval_add', 'pa_timeval_sub',
'pa_timeval_store', 'pa_timeval_load']
