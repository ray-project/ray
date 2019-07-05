#!/usr/bin/python
# $Id:$

from __future__ import division
import ctypes

lib = ctypes.windll.wintab32

LONG = ctypes.c_long
BOOL = ctypes.c_int
UINT = ctypes.c_uint
WORD = ctypes.c_uint16
DWORD = ctypes.c_uint32
WCHAR = ctypes.c_wchar
FIX32 = DWORD
WTPKT = DWORD

LCNAMELEN = 40

class AXIS(ctypes.Structure):
    _fields_ = (
        ('axMin', LONG),
        ('axMax', LONG),
        ('axUnits', UINT),
        ('axResolution', FIX32)
    )

    def get_scale(self):
        return 1 / float(self.axMax - self.axMin)

    def get_bias(self):
        return -self.axMin

class ORIENTATION(ctypes.Structure):
    _fields_ = (
        ('orAzimuth', ctypes.c_int),
        ('orAltitude', ctypes.c_int),
        ('orTwist', ctypes.c_int)
    )

class ROTATION(ctypes.Structure):
    _fields_ = (
        ('roPitch', ctypes.c_int),
        ('roRoll', ctypes.c_int),
        ('roYaw', ctypes.c_int),
    )

class LOGCONTEXT(ctypes.Structure):
    _fields_ = (
        ('lcName', WCHAR * LCNAMELEN),
        ('lcOptions', UINT),
        ('lcStatus', UINT),
        ('lcLocks', UINT),
        ('lcMsgBase', UINT),
        ('lcDevice', UINT),
        ('lcPktRate', UINT),
        ('lcPktData', WTPKT),
        ('lcPktMode', WTPKT),
        ('lcMoveMask', WTPKT),
        ('lcBtnDnMask', DWORD),
        ('lcBtnUpMask', DWORD),
        ('lcInOrgX', LONG),
        ('lcInOrgY', LONG),
        ('lcInOrgZ', LONG),
        ('lcInExtX', LONG),
        ('lcInExtY', LONG),
        ('lcInExtZ', LONG),
        ('lcOutOrgX', LONG),
        ('lcOutOrgY', LONG),
        ('lcOutOrgZ', LONG),
        ('lcOutExtX', LONG),
        ('lcOutExtY', LONG),
        ('lcOutExtZ', LONG),
        ('lcSensX', FIX32),
        ('lcSensY', FIX32),
        ('lcSensZ', FIX32),
        ('lcSysMode', BOOL),
        ('lcSysOrgX', ctypes.c_int),
        ('lcSysOrgY', ctypes.c_int),
        ('lcSysExtX', ctypes.c_int),
        ('lcSysExtY', ctypes.c_int),
        ('lcSysSensX', FIX32),
        ('lcSysSensY', FIX32),
    )

# Custom packet format with fields 
#   PK_CHANGED
#   PK_CURSOR
#   PK_BUTTONS
#   PK_X
#   PK_Y
#   PK_Z
#   PK_NORMAL_PRESSURE
#   PK_TANGENT_PRESSURE
#   PK_ORIENTATION (check for tilt extension instead)?
class PACKET(ctypes.Structure):
    _fields_ = (
        ('pkChanged', WTPKT),
        ('pkCursor', UINT),
        ('pkButtons', DWORD),
        ('pkX', LONG),
        ('pkY', LONG),
        ('pkZ', LONG),
        ('pkNormalPressure', UINT),
        ('pkTangentPressure', UINT),
        ('pkOrientation', ORIENTATION),
    )

PK_CONTEXT = 0x0001	# reporting context 
PK_STATUS = 0x0002	# status bits 
PK_TIME = 0x0004	# time stamp 
PK_CHANGED = 0x0008	# change bit vector 
PK_SERIAL_NUMBER = 0x0010	# packet serial number 
PK_CURSOR = 0x0020	# reporting cursor 
PK_BUTTONS = 0x0040	# button information 
PK_X = 0x0080	# x axis 
PK_Y = 0x0100	# y axis 
PK_Z = 0x0200	# z axis 
PK_NORMAL_PRESSURE = 0x0400	# normal or tip pressure 
PK_TANGENT_PRESSURE = 0x0800	# tangential or barrel pressure 
PK_ORIENTATION = 0x1000	# orientation info: tilts 
PK_ROTATION = 0x2000	# rotation info; 1.1 

TU_NONE = 0
TU_INCHES = 1
TU_CENTIMETERS = 2
TU_CIRCLE = 3

# messages
WT_DEFBASE = 0x7ff0
WT_MAXOFFSET = 0xf
WT_PACKET = 0 # remember to add base
WT_CTXOPEN = 1
WT_CTXCLOSE = 2
WT_CTXUPDATE = 3
WT_CTXOVERLAP = 4
WT_PROXIMITY = 5
WT_INFOCHANGE = 6
WT_CSRCHANGE = 7

# system button assignment values 
SBN_NONE = 0x00
SBN_LCLICK = 0x01
SBN_LDBLCLICK = 0x02
SBN_LDRAG = 0x03
SBN_RCLICK = 0x04
SBN_RDBLCLICK = 0x05
SBN_RDRAG = 0x06
SBN_MCLICK = 0x07
SBN_MDBLCLICK = 0x08
SBN_MDRAG = 0x09

# for Pen Windows 
SBN_PTCLICK = 0x10
SBN_PTDBLCLICK = 0x20
SBN_PTDRAG = 0x30
SBN_PNCLICK = 0x40
SBN_PNDBLCLICK = 0x50
SBN_PNDRAG = 0x60
SBN_P1CLICK = 0x70
SBN_P1DBLCLICK = 0x80
SBN_P1DRAG = 0x90
SBN_P2CLICK = 0xA0
SBN_P2DBLCLICK = 0xB0
SBN_P2DRAG = 0xC0
SBN_P3CLICK = 0xD0
SBN_P3DBLCLICK = 0xE0
SBN_P3DRAG = 0xF0

HWC_INTEGRATED = 0x0001
HWC_TOUCH = 0x0002
HWC_HARDPROX = 0x0004
HWC_PHYSID_CURSORS = 0x0008 # 1.1 

CRC_MULTIMODE = 0x0001 # 1.1 
CRC_AGGREGATE = 0x0002 # 1.1 
CRC_INVERT = 0x0004 # 1.1 

WTI_INTERFACE = 1
IFC_WINTABID = 1
IFC_SPECVERSION = 2
IFC_IMPLVERSION = 3
IFC_NDEVICES = 4
IFC_NCURSORS = 5
IFC_NCONTEXTS = 6
IFC_CTXOPTIONS = 7
IFC_CTXSAVESIZE = 8
IFC_NEXTENSIONS = 9
IFC_NMANAGERS = 10
IFC_MAX = 10

WTI_STATUS = 2
STA_CONTEXTS = 1
STA_SYSCTXS = 2
STA_PKTRATE = 3
STA_PKTDATA = 4
STA_MANAGERS = 5
STA_SYSTEM = 6
STA_BUTTONUSE = 7
STA_SYSBTNUSE = 8
STA_MAX = 8

WTI_DEFCONTEXT = 3
WTI_DEFSYSCTX = 4
WTI_DDCTXS = 400 # 1.1 
WTI_DSCTXS = 500 # 1.1 
CTX_NAME = 1
CTX_OPTIONS = 2
CTX_STATUS = 3
CTX_LOCKS = 4
CTX_MSGBASE = 5
CTX_DEVICE = 6
CTX_PKTRATE = 7
CTX_PKTDATA = 8
CTX_PKTMODE = 9
CTX_MOVEMASK = 10
CTX_BTNDNMASK = 11
CTX_BTNUPMASK = 12
CTX_INORGX = 13
CTX_INORGY = 14
CTX_INORGZ = 15
CTX_INEXTX = 16
CTX_INEXTY = 17
CTX_INEXTZ = 18
CTX_OUTORGX = 19
CTX_OUTORGY = 20
CTX_OUTORGZ = 21
CTX_OUTEXTX = 22
CTX_OUTEXTY = 23
CTX_OUTEXTZ = 24
CTX_SENSX = 25
CTX_SENSY = 26
CTX_SENSZ = 27
CTX_SYSMODE = 28
CTX_SYSORGX = 29
CTX_SYSORGY = 30
CTX_SYSEXTX = 31
CTX_SYSEXTY = 32
CTX_SYSSENSX = 33
CTX_SYSSENSY = 34
CTX_MAX = 34

WTI_DEVICES = 100
DVC_NAME = 1
DVC_HARDWARE = 2
DVC_NCSRTYPES = 3
DVC_FIRSTCSR = 4
DVC_PKTRATE = 5
DVC_PKTDATA = 6
DVC_PKTMODE = 7
DVC_CSRDATA = 8
DVC_XMARGIN = 9
DVC_YMARGIN = 10
DVC_ZMARGIN = 11
DVC_X = 12
DVC_Y = 13
DVC_Z = 14
DVC_NPRESSURE = 15
DVC_TPRESSURE = 16
DVC_ORIENTATION = 17
DVC_ROTATION = 18 # 1.1 
DVC_PNPID = 19 # 1.1 
DVC_MAX = 19

WTI_CURSORS = 200
CSR_NAME = 1
CSR_ACTIVE = 2
CSR_PKTDATA = 3
CSR_BUTTONS = 4
CSR_BUTTONBITS = 5
CSR_BTNNAMES = 6
CSR_BUTTONMAP = 7
CSR_SYSBTNMAP = 8
CSR_NPBUTTON = 9
CSR_NPBTNMARKS = 10
CSR_NPRESPONSE = 11
CSR_TPBUTTON = 12
CSR_TPBTNMARKS = 13
CSR_TPRESPONSE = 14
CSR_PHYSID = 15 # 1.1 
CSR_MODE = 16 # 1.1 
CSR_MINPKTDATA = 17 # 1.1 
CSR_MINBUTTONS = 18 # 1.1 
CSR_CAPABILITIES = 19 # 1.1 
CSR_TYPE = 20 # 1.2 
CSR_MAX = 20

WTI_EXTENSIONS = 300
EXT_NAME = 1
EXT_TAG = 2
EXT_MASK = 3
EXT_SIZE = 4
EXT_AXES = 5
EXT_DEFAULT = 6
EXT_DEFCONTEXT = 7
EXT_DEFSYSCTX = 8
EXT_CURSORS = 9 
EXT_MAX = 109 # Allow 100 cursors 
CXO_SYSTEM = 0x0001
CXO_PEN = 0x0002
CXO_MESSAGES = 0x0004
CXO_MARGIN = 0x8000
CXO_MGNINSIDE = 0x4000
CXO_CSRMESSAGES = 0x0008 # 1.1 

# context status values 
CXS_DISABLED = 0x0001
CXS_OBSCURED = 0x0002
CXS_ONTOP = 0x0004

# context lock values 
CXL_INSIZE = 0x0001
CXL_INASPECT = 0x0002
CXL_SENSITIVITY = 0x0004
CXL_MARGIN = 0x0008
CXL_SYSOUT = 0x0010
# packet status values 
TPS_PROXIMITY = 0x0001
TPS_QUEUE_ERR = 0x0002
TPS_MARGIN = 0x0004
TPS_GRAB = 0x0008
TPS_INVERT = 0x0010 # 1.1 

TBN_NONE = 0
TBN_UP = 1
TBN_DOWN = 2
PKEXT_ABSOLUTE = 1
PKEXT_RELATIVE = 2

# Extension tags. 
WTX_OBT = 0	# Out of bounds tracking 
WTX_FKEYS = 1	# Function keys 
WTX_TILT = 2	# Raw Cartesian tilt; 1.1 
WTX_CSRMASK = 3	# select input by cursor type; 1.1 
WTX_XBTNMASK = 4	# Extended button mask; 1.1 
WTX_EXPKEYS = 5	# ExpressKeys; 1.3 
