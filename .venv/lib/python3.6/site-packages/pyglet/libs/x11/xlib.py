'''Wrapper for X11

Generated with:
tools/genwrappers.py xlib

Do not modify this file.
'''

__docformat__ =  'restructuredtext'
__version__ = '$Id$'

import ctypes
from ctypes import *

import pyglet.lib

_lib = pyglet.lib.load_library('X11')

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



XlibSpecificationRelease = 6 	# /usr/include/X11/Xlib.h:39
X_PROTOCOL = 11 	# /usr/include/X11/X.h:53
X_PROTOCOL_REVISION = 0 	# /usr/include/X11/X.h:54
XID = c_ulong 	# /usr/include/X11/X.h:66
Mask = c_ulong 	# /usr/include/X11/X.h:70
Atom = c_ulong 	# /usr/include/X11/X.h:74
VisualID = c_ulong 	# /usr/include/X11/X.h:76
Time = c_ulong 	# /usr/include/X11/X.h:77
Window = XID 	# /usr/include/X11/X.h:96
Drawable = XID 	# /usr/include/X11/X.h:97
Font = XID 	# /usr/include/X11/X.h:100
Pixmap = XID 	# /usr/include/X11/X.h:102
Cursor = XID 	# /usr/include/X11/X.h:103
Colormap = XID 	# /usr/include/X11/X.h:104
GContext = XID 	# /usr/include/X11/X.h:105
KeySym = XID 	# /usr/include/X11/X.h:106
KeyCode = c_ubyte 	# /usr/include/X11/X.h:108
None_ = 0 	# /usr/include/X11/X.h:115
ParentRelative = 1 	# /usr/include/X11/X.h:118
CopyFromParent = 0 	# /usr/include/X11/X.h:121
PointerWindow = 0 	# /usr/include/X11/X.h:126
InputFocus = 1 	# /usr/include/X11/X.h:127
PointerRoot = 1 	# /usr/include/X11/X.h:129
AnyPropertyType = 0 	# /usr/include/X11/X.h:131
AnyKey = 0 	# /usr/include/X11/X.h:133
AnyButton = 0 	# /usr/include/X11/X.h:135
AllTemporary = 0 	# /usr/include/X11/X.h:137
CurrentTime = 0 	# /usr/include/X11/X.h:139
NoSymbol = 0 	# /usr/include/X11/X.h:141
NoEventMask = 0 	# /usr/include/X11/X.h:150
KeyPressMask = 1 	# /usr/include/X11/X.h:151
KeyReleaseMask = 2 	# /usr/include/X11/X.h:152
ButtonPressMask = 4 	# /usr/include/X11/X.h:153
ButtonReleaseMask = 8 	# /usr/include/X11/X.h:154
EnterWindowMask = 16 	# /usr/include/X11/X.h:155
LeaveWindowMask = 32 	# /usr/include/X11/X.h:156
PointerMotionMask = 64 	# /usr/include/X11/X.h:157
PointerMotionHintMask = 128 	# /usr/include/X11/X.h:158
Button1MotionMask = 256 	# /usr/include/X11/X.h:159
Button2MotionMask = 512 	# /usr/include/X11/X.h:160
Button3MotionMask = 1024 	# /usr/include/X11/X.h:161
Button4MotionMask = 2048 	# /usr/include/X11/X.h:162
Button5MotionMask = 4096 	# /usr/include/X11/X.h:163
ButtonMotionMask = 8192 	# /usr/include/X11/X.h:164
KeymapStateMask = 16384 	# /usr/include/X11/X.h:165
ExposureMask = 32768 	# /usr/include/X11/X.h:166
VisibilityChangeMask = 65536 	# /usr/include/X11/X.h:167
StructureNotifyMask = 131072 	# /usr/include/X11/X.h:168
ResizeRedirectMask = 262144 	# /usr/include/X11/X.h:169
SubstructureNotifyMask = 524288 	# /usr/include/X11/X.h:170
SubstructureRedirectMask = 1048576 	# /usr/include/X11/X.h:171
FocusChangeMask = 2097152 	# /usr/include/X11/X.h:172
PropertyChangeMask = 4194304 	# /usr/include/X11/X.h:173
ColormapChangeMask = 8388608 	# /usr/include/X11/X.h:174
OwnerGrabButtonMask = 16777216 	# /usr/include/X11/X.h:175
KeyPress = 2 	# /usr/include/X11/X.h:181
KeyRelease = 3 	# /usr/include/X11/X.h:182
ButtonPress = 4 	# /usr/include/X11/X.h:183
ButtonRelease = 5 	# /usr/include/X11/X.h:184
MotionNotify = 6 	# /usr/include/X11/X.h:185
EnterNotify = 7 	# /usr/include/X11/X.h:186
LeaveNotify = 8 	# /usr/include/X11/X.h:187
FocusIn = 9 	# /usr/include/X11/X.h:188
FocusOut = 10 	# /usr/include/X11/X.h:189
KeymapNotify = 11 	# /usr/include/X11/X.h:190
Expose = 12 	# /usr/include/X11/X.h:191
GraphicsExpose = 13 	# /usr/include/X11/X.h:192
NoExpose = 14 	# /usr/include/X11/X.h:193
VisibilityNotify = 15 	# /usr/include/X11/X.h:194
CreateNotify = 16 	# /usr/include/X11/X.h:195
DestroyNotify = 17 	# /usr/include/X11/X.h:196
UnmapNotify = 18 	# /usr/include/X11/X.h:197
MapNotify = 19 	# /usr/include/X11/X.h:198
MapRequest = 20 	# /usr/include/X11/X.h:199
ReparentNotify = 21 	# /usr/include/X11/X.h:200
ConfigureNotify = 22 	# /usr/include/X11/X.h:201
ConfigureRequest = 23 	# /usr/include/X11/X.h:202
GravityNotify = 24 	# /usr/include/X11/X.h:203
ResizeRequest = 25 	# /usr/include/X11/X.h:204
CirculateNotify = 26 	# /usr/include/X11/X.h:205
CirculateRequest = 27 	# /usr/include/X11/X.h:206
PropertyNotify = 28 	# /usr/include/X11/X.h:207
SelectionClear = 29 	# /usr/include/X11/X.h:208
SelectionRequest = 30 	# /usr/include/X11/X.h:209
SelectionNotify = 31 	# /usr/include/X11/X.h:210
ColormapNotify = 32 	# /usr/include/X11/X.h:211
ClientMessage = 33 	# /usr/include/X11/X.h:212
MappingNotify = 34 	# /usr/include/X11/X.h:213
GenericEvent = 35 	# /usr/include/X11/X.h:214
LASTEvent = 36 	# /usr/include/X11/X.h:215
ShiftMask = 1 	# /usr/include/X11/X.h:221
LockMask = 2 	# /usr/include/X11/X.h:222
ControlMask = 4 	# /usr/include/X11/X.h:223
Mod1Mask = 8 	# /usr/include/X11/X.h:224
Mod2Mask = 16 	# /usr/include/X11/X.h:225
Mod3Mask = 32 	# /usr/include/X11/X.h:226
Mod4Mask = 64 	# /usr/include/X11/X.h:227
Mod5Mask = 128 	# /usr/include/X11/X.h:228
ShiftMapIndex = 0 	# /usr/include/X11/X.h:233
LockMapIndex = 1 	# /usr/include/X11/X.h:234
ControlMapIndex = 2 	# /usr/include/X11/X.h:235
Mod1MapIndex = 3 	# /usr/include/X11/X.h:236
Mod2MapIndex = 4 	# /usr/include/X11/X.h:237
Mod3MapIndex = 5 	# /usr/include/X11/X.h:238
Mod4MapIndex = 6 	# /usr/include/X11/X.h:239
Mod5MapIndex = 7 	# /usr/include/X11/X.h:240
Button1Mask = 256 	# /usr/include/X11/X.h:246
Button2Mask = 512 	# /usr/include/X11/X.h:247
Button3Mask = 1024 	# /usr/include/X11/X.h:248
Button4Mask = 2048 	# /usr/include/X11/X.h:249
Button5Mask = 4096 	# /usr/include/X11/X.h:250
AnyModifier = 32768 	# /usr/include/X11/X.h:252
Button1 = 1 	# /usr/include/X11/X.h:259
Button2 = 2 	# /usr/include/X11/X.h:260
Button3 = 3 	# /usr/include/X11/X.h:261
Button4 = 4 	# /usr/include/X11/X.h:262
Button5 = 5 	# /usr/include/X11/X.h:263
NotifyNormal = 0 	# /usr/include/X11/X.h:267
NotifyGrab = 1 	# /usr/include/X11/X.h:268
NotifyUngrab = 2 	# /usr/include/X11/X.h:269
NotifyWhileGrabbed = 3 	# /usr/include/X11/X.h:270
NotifyHint = 1 	# /usr/include/X11/X.h:272
NotifyAncestor = 0 	# /usr/include/X11/X.h:276
NotifyVirtual = 1 	# /usr/include/X11/X.h:277
NotifyInferior = 2 	# /usr/include/X11/X.h:278
NotifyNonlinear = 3 	# /usr/include/X11/X.h:279
NotifyNonlinearVirtual = 4 	# /usr/include/X11/X.h:280
NotifyPointer = 5 	# /usr/include/X11/X.h:281
NotifyPointerRoot = 6 	# /usr/include/X11/X.h:282
NotifyDetailNone = 7 	# /usr/include/X11/X.h:283
VisibilityUnobscured = 0 	# /usr/include/X11/X.h:287
VisibilityPartiallyObscured = 1 	# /usr/include/X11/X.h:288
VisibilityFullyObscured = 2 	# /usr/include/X11/X.h:289
PlaceOnTop = 0 	# /usr/include/X11/X.h:293
PlaceOnBottom = 1 	# /usr/include/X11/X.h:294
FamilyInternet = 0 	# /usr/include/X11/X.h:298
FamilyDECnet = 1 	# /usr/include/X11/X.h:299
FamilyChaos = 2 	# /usr/include/X11/X.h:300
FamilyInternet6 = 6 	# /usr/include/X11/X.h:301
FamilyServerInterpreted = 5 	# /usr/include/X11/X.h:304
PropertyNewValue = 0 	# /usr/include/X11/X.h:308
PropertyDelete = 1 	# /usr/include/X11/X.h:309
ColormapUninstalled = 0 	# /usr/include/X11/X.h:313
ColormapInstalled = 1 	# /usr/include/X11/X.h:314
GrabModeSync = 0 	# /usr/include/X11/X.h:318
GrabModeAsync = 1 	# /usr/include/X11/X.h:319
GrabSuccess = 0 	# /usr/include/X11/X.h:323
AlreadyGrabbed = 1 	# /usr/include/X11/X.h:324
GrabInvalidTime = 2 	# /usr/include/X11/X.h:325
GrabNotViewable = 3 	# /usr/include/X11/X.h:326
GrabFrozen = 4 	# /usr/include/X11/X.h:327
AsyncPointer = 0 	# /usr/include/X11/X.h:331
SyncPointer = 1 	# /usr/include/X11/X.h:332
ReplayPointer = 2 	# /usr/include/X11/X.h:333
AsyncKeyboard = 3 	# /usr/include/X11/X.h:334
SyncKeyboard = 4 	# /usr/include/X11/X.h:335
ReplayKeyboard = 5 	# /usr/include/X11/X.h:336
AsyncBoth = 6 	# /usr/include/X11/X.h:337
SyncBoth = 7 	# /usr/include/X11/X.h:338
RevertToParent = 2 	# /usr/include/X11/X.h:344
Success = 0 	# /usr/include/X11/X.h:350
BadRequest = 1 	# /usr/include/X11/X.h:351
BadValue = 2 	# /usr/include/X11/X.h:352
BadWindow = 3 	# /usr/include/X11/X.h:353
BadPixmap = 4 	# /usr/include/X11/X.h:354
BadAtom = 5 	# /usr/include/X11/X.h:355
BadCursor = 6 	# /usr/include/X11/X.h:356
BadFont = 7 	# /usr/include/X11/X.h:357
BadMatch = 8 	# /usr/include/X11/X.h:358
BadDrawable = 9 	# /usr/include/X11/X.h:359
BadAccess = 10 	# /usr/include/X11/X.h:360
BadAlloc = 11 	# /usr/include/X11/X.h:369
BadColor = 12 	# /usr/include/X11/X.h:370
BadGC = 13 	# /usr/include/X11/X.h:371
BadIDChoice = 14 	# /usr/include/X11/X.h:372
BadName = 15 	# /usr/include/X11/X.h:373
BadLength = 16 	# /usr/include/X11/X.h:374
BadImplementation = 17 	# /usr/include/X11/X.h:375
FirstExtensionError = 128 	# /usr/include/X11/X.h:377
LastExtensionError = 255 	# /usr/include/X11/X.h:378
InputOutput = 1 	# /usr/include/X11/X.h:387
InputOnly = 2 	# /usr/include/X11/X.h:388
CWBackPixmap = 1 	# /usr/include/X11/X.h:392
CWBackPixel = 2 	# /usr/include/X11/X.h:393
CWBorderPixmap = 4 	# /usr/include/X11/X.h:394
CWBorderPixel = 8 	# /usr/include/X11/X.h:395
CWBitGravity = 16 	# /usr/include/X11/X.h:396
CWWinGravity = 32 	# /usr/include/X11/X.h:397
CWBackingStore = 64 	# /usr/include/X11/X.h:398
CWBackingPlanes = 128 	# /usr/include/X11/X.h:399
CWBackingPixel = 256 	# /usr/include/X11/X.h:400
CWOverrideRedirect = 512 	# /usr/include/X11/X.h:401
CWSaveUnder = 1024 	# /usr/include/X11/X.h:402
CWEventMask = 2048 	# /usr/include/X11/X.h:403
CWDontPropagate = 4096 	# /usr/include/X11/X.h:404
CWColormap = 8192 	# /usr/include/X11/X.h:405
CWCursor = 16384 	# /usr/include/X11/X.h:406
CWX = 1 	# /usr/include/X11/X.h:410
CWY = 2 	# /usr/include/X11/X.h:411
CWWidth = 4 	# /usr/include/X11/X.h:412
CWHeight = 8 	# /usr/include/X11/X.h:413
CWBorderWidth = 16 	# /usr/include/X11/X.h:414
CWSibling = 32 	# /usr/include/X11/X.h:415
CWStackMode = 64 	# /usr/include/X11/X.h:416
ForgetGravity = 0 	# /usr/include/X11/X.h:421
NorthWestGravity = 1 	# /usr/include/X11/X.h:422
NorthGravity = 2 	# /usr/include/X11/X.h:423
NorthEastGravity = 3 	# /usr/include/X11/X.h:424
WestGravity = 4 	# /usr/include/X11/X.h:425
CenterGravity = 5 	# /usr/include/X11/X.h:426
EastGravity = 6 	# /usr/include/X11/X.h:427
SouthWestGravity = 7 	# /usr/include/X11/X.h:428
SouthGravity = 8 	# /usr/include/X11/X.h:429
SouthEastGravity = 9 	# /usr/include/X11/X.h:430
StaticGravity = 10 	# /usr/include/X11/X.h:431
UnmapGravity = 0 	# /usr/include/X11/X.h:435
NotUseful = 0 	# /usr/include/X11/X.h:439
WhenMapped = 1 	# /usr/include/X11/X.h:440
Always = 2 	# /usr/include/X11/X.h:441
IsUnmapped = 0 	# /usr/include/X11/X.h:445
IsUnviewable = 1 	# /usr/include/X11/X.h:446
IsViewable = 2 	# /usr/include/X11/X.h:447
SetModeInsert = 0 	# /usr/include/X11/X.h:451
SetModeDelete = 1 	# /usr/include/X11/X.h:452
DestroyAll = 0 	# /usr/include/X11/X.h:456
RetainPermanent = 1 	# /usr/include/X11/X.h:457
RetainTemporary = 2 	# /usr/include/X11/X.h:458
Above = 0 	# /usr/include/X11/X.h:462
Below = 1 	# /usr/include/X11/X.h:463
TopIf = 2 	# /usr/include/X11/X.h:464
BottomIf = 3 	# /usr/include/X11/X.h:465
Opposite = 4 	# /usr/include/X11/X.h:466
RaiseLowest = 0 	# /usr/include/X11/X.h:470
LowerHighest = 1 	# /usr/include/X11/X.h:471
PropModeReplace = 0 	# /usr/include/X11/X.h:475
PropModePrepend = 1 	# /usr/include/X11/X.h:476
PropModeAppend = 2 	# /usr/include/X11/X.h:477
GXclear = 0 	# /usr/include/X11/X.h:485
GXand = 1 	# /usr/include/X11/X.h:486
GXandReverse = 2 	# /usr/include/X11/X.h:487
GXcopy = 3 	# /usr/include/X11/X.h:488
GXandInverted = 4 	# /usr/include/X11/X.h:489
GXnoop = 5 	# /usr/include/X11/X.h:490
GXxor = 6 	# /usr/include/X11/X.h:491
GXor = 7 	# /usr/include/X11/X.h:492
GXnor = 8 	# /usr/include/X11/X.h:493
GXequiv = 9 	# /usr/include/X11/X.h:494
GXinvert = 10 	# /usr/include/X11/X.h:495
GXorReverse = 11 	# /usr/include/X11/X.h:496
GXcopyInverted = 12 	# /usr/include/X11/X.h:497
GXorInverted = 13 	# /usr/include/X11/X.h:498
GXnand = 14 	# /usr/include/X11/X.h:499
GXset = 15 	# /usr/include/X11/X.h:500
LineSolid = 0 	# /usr/include/X11/X.h:504
LineOnOffDash = 1 	# /usr/include/X11/X.h:505
LineDoubleDash = 2 	# /usr/include/X11/X.h:506
CapNotLast = 0 	# /usr/include/X11/X.h:510
CapButt = 1 	# /usr/include/X11/X.h:511
CapRound = 2 	# /usr/include/X11/X.h:512
CapProjecting = 3 	# /usr/include/X11/X.h:513
JoinMiter = 0 	# /usr/include/X11/X.h:517
JoinRound = 1 	# /usr/include/X11/X.h:518
JoinBevel = 2 	# /usr/include/X11/X.h:519
FillSolid = 0 	# /usr/include/X11/X.h:523
FillTiled = 1 	# /usr/include/X11/X.h:524
FillStippled = 2 	# /usr/include/X11/X.h:525
FillOpaqueStippled = 3 	# /usr/include/X11/X.h:526
EvenOddRule = 0 	# /usr/include/X11/X.h:530
WindingRule = 1 	# /usr/include/X11/X.h:531
ClipByChildren = 0 	# /usr/include/X11/X.h:535
IncludeInferiors = 1 	# /usr/include/X11/X.h:536
Unsorted = 0 	# /usr/include/X11/X.h:540
YSorted = 1 	# /usr/include/X11/X.h:541
YXSorted = 2 	# /usr/include/X11/X.h:542
YXBanded = 3 	# /usr/include/X11/X.h:543
CoordModeOrigin = 0 	# /usr/include/X11/X.h:547
CoordModePrevious = 1 	# /usr/include/X11/X.h:548
Complex = 0 	# /usr/include/X11/X.h:552
Nonconvex = 1 	# /usr/include/X11/X.h:553
Convex = 2 	# /usr/include/X11/X.h:554
ArcChord = 0 	# /usr/include/X11/X.h:558
ArcPieSlice = 1 	# /usr/include/X11/X.h:559
GCFunction = 1 	# /usr/include/X11/X.h:564
GCPlaneMask = 2 	# /usr/include/X11/X.h:565
GCForeground = 4 	# /usr/include/X11/X.h:566
GCBackground = 8 	# /usr/include/X11/X.h:567
GCLineWidth = 16 	# /usr/include/X11/X.h:568
GCLineStyle = 32 	# /usr/include/X11/X.h:569
GCCapStyle = 64 	# /usr/include/X11/X.h:570
GCJoinStyle = 128 	# /usr/include/X11/X.h:571
GCFillStyle = 256 	# /usr/include/X11/X.h:572
GCFillRule = 512 	# /usr/include/X11/X.h:573
GCTile = 1024 	# /usr/include/X11/X.h:574
GCStipple = 2048 	# /usr/include/X11/X.h:575
GCTileStipXOrigin = 4096 	# /usr/include/X11/X.h:576
GCTileStipYOrigin = 8192 	# /usr/include/X11/X.h:577
GCFont = 16384 	# /usr/include/X11/X.h:578
GCSubwindowMode = 32768 	# /usr/include/X11/X.h:579
GCGraphicsExposures = 65536 	# /usr/include/X11/X.h:580
GCClipXOrigin = 131072 	# /usr/include/X11/X.h:581
GCClipYOrigin = 262144 	# /usr/include/X11/X.h:582
GCClipMask = 524288 	# /usr/include/X11/X.h:583
GCDashOffset = 1048576 	# /usr/include/X11/X.h:584
GCDashList = 2097152 	# /usr/include/X11/X.h:585
GCArcMode = 4194304 	# /usr/include/X11/X.h:586
GCLastBit = 22 	# /usr/include/X11/X.h:588
FontLeftToRight = 0 	# /usr/include/X11/X.h:595
FontRightToLeft = 1 	# /usr/include/X11/X.h:596
FontChange = 255 	# /usr/include/X11/X.h:598
XYBitmap = 0 	# /usr/include/X11/X.h:606
XYPixmap = 1 	# /usr/include/X11/X.h:607
ZPixmap = 2 	# /usr/include/X11/X.h:608
AllocNone = 0 	# /usr/include/X11/X.h:616
AllocAll = 1 	# /usr/include/X11/X.h:617
DoRed = 1 	# /usr/include/X11/X.h:622
DoGreen = 2 	# /usr/include/X11/X.h:623
DoBlue = 4 	# /usr/include/X11/X.h:624
CursorShape = 0 	# /usr/include/X11/X.h:632
TileShape = 1 	# /usr/include/X11/X.h:633
StippleShape = 2 	# /usr/include/X11/X.h:634
AutoRepeatModeOff = 0 	# /usr/include/X11/X.h:640
AutoRepeatModeOn = 1 	# /usr/include/X11/X.h:641
AutoRepeatModeDefault = 2 	# /usr/include/X11/X.h:642
LedModeOff = 0 	# /usr/include/X11/X.h:644
LedModeOn = 1 	# /usr/include/X11/X.h:645
KBKeyClickPercent = 1 	# /usr/include/X11/X.h:649
KBBellPercent = 2 	# /usr/include/X11/X.h:650
KBBellPitch = 4 	# /usr/include/X11/X.h:651
KBBellDuration = 8 	# /usr/include/X11/X.h:652
KBLed = 16 	# /usr/include/X11/X.h:653
KBLedMode = 32 	# /usr/include/X11/X.h:654
KBKey = 64 	# /usr/include/X11/X.h:655
KBAutoRepeatMode = 128 	# /usr/include/X11/X.h:656
MappingSuccess = 0 	# /usr/include/X11/X.h:658
MappingBusy = 1 	# /usr/include/X11/X.h:659
MappingFailed = 2 	# /usr/include/X11/X.h:660
MappingModifier = 0 	# /usr/include/X11/X.h:662
MappingKeyboard = 1 	# /usr/include/X11/X.h:663
MappingPointer = 2 	# /usr/include/X11/X.h:664
DontPreferBlanking = 0 	# /usr/include/X11/X.h:670
PreferBlanking = 1 	# /usr/include/X11/X.h:671
DefaultBlanking = 2 	# /usr/include/X11/X.h:672
DisableScreenSaver = 0 	# /usr/include/X11/X.h:674
DisableScreenInterval = 0 	# /usr/include/X11/X.h:675
DontAllowExposures = 0 	# /usr/include/X11/X.h:677
AllowExposures = 1 	# /usr/include/X11/X.h:678
DefaultExposures = 2 	# /usr/include/X11/X.h:679
ScreenSaverReset = 0 	# /usr/include/X11/X.h:683
ScreenSaverActive = 1 	# /usr/include/X11/X.h:684
HostInsert = 0 	# /usr/include/X11/X.h:692
HostDelete = 1 	# /usr/include/X11/X.h:693
EnableAccess = 1 	# /usr/include/X11/X.h:697
DisableAccess = 0 	# /usr/include/X11/X.h:698
StaticGray = 0 	# /usr/include/X11/X.h:704
GrayScale = 1 	# /usr/include/X11/X.h:705
StaticColor = 2 	# /usr/include/X11/X.h:706
PseudoColor = 3 	# /usr/include/X11/X.h:707
TrueColor = 4 	# /usr/include/X11/X.h:708
DirectColor = 5 	# /usr/include/X11/X.h:709
LSBFirst = 0 	# /usr/include/X11/X.h:714
MSBFirst = 1 	# /usr/include/X11/X.h:715
# /usr/include/X11/Xlib.h:73
_Xmblen = _lib._Xmblen
_Xmblen.restype = c_int
_Xmblen.argtypes = [c_char_p, c_int]

X_HAVE_UTF8_STRING = 1 	# /usr/include/X11/Xlib.h:85
XPointer = c_char_p 	# /usr/include/X11/Xlib.h:87
Bool = c_int 	# /usr/include/X11/Xlib.h:89
Status = c_int 	# /usr/include/X11/Xlib.h:90
True_ = 1 	# /usr/include/X11/Xlib.h:91
False_ = 0 	# /usr/include/X11/Xlib.h:92
QueuedAlready = 0 	# /usr/include/X11/Xlib.h:94
QueuedAfterReading = 1 	# /usr/include/X11/Xlib.h:95
QueuedAfterFlush = 2 	# /usr/include/X11/Xlib.h:96
class struct__XExtData(Structure):
    __slots__ = [
        'number',
        'next',
        'free_private',
        'private_data',
    ]
struct__XExtData._fields_ = [
    ('number', c_int),
    ('next', POINTER(struct__XExtData)),
    ('free_private', POINTER(CFUNCTYPE(c_int, POINTER(struct__XExtData)))),
    ('private_data', XPointer),
]

XExtData = struct__XExtData 	# /usr/include/X11/Xlib.h:166
class struct_anon_15(Structure):
    __slots__ = [
        'extension',
        'major_opcode',
        'first_event',
        'first_error',
    ]
struct_anon_15._fields_ = [
    ('extension', c_int),
    ('major_opcode', c_int),
    ('first_event', c_int),
    ('first_error', c_int),
]

XExtCodes = struct_anon_15 	# /usr/include/X11/Xlib.h:176
class struct_anon_16(Structure):
    __slots__ = [
        'depth',
        'bits_per_pixel',
        'scanline_pad',
    ]
struct_anon_16._fields_ = [
    ('depth', c_int),
    ('bits_per_pixel', c_int),
    ('scanline_pad', c_int),
]

XPixmapFormatValues = struct_anon_16 	# /usr/include/X11/Xlib.h:186
class struct_anon_17(Structure):
    __slots__ = [
        'function',
        'plane_mask',
        'foreground',
        'background',
        'line_width',
        'line_style',
        'cap_style',
        'join_style',
        'fill_style',
        'fill_rule',
        'arc_mode',
        'tile',
        'stipple',
        'ts_x_origin',
        'ts_y_origin',
        'font',
        'subwindow_mode',
        'graphics_exposures',
        'clip_x_origin',
        'clip_y_origin',
        'clip_mask',
        'dash_offset',
        'dashes',
    ]
struct_anon_17._fields_ = [
    ('function', c_int),
    ('plane_mask', c_ulong),
    ('foreground', c_ulong),
    ('background', c_ulong),
    ('line_width', c_int),
    ('line_style', c_int),
    ('cap_style', c_int),
    ('join_style', c_int),
    ('fill_style', c_int),
    ('fill_rule', c_int),
    ('arc_mode', c_int),
    ('tile', Pixmap),
    ('stipple', Pixmap),
    ('ts_x_origin', c_int),
    ('ts_y_origin', c_int),
    ('font', Font),
    ('subwindow_mode', c_int),
    ('graphics_exposures', c_int),
    ('clip_x_origin', c_int),
    ('clip_y_origin', c_int),
    ('clip_mask', Pixmap),
    ('dash_offset', c_int),
    ('dashes', c_char),
]

XGCValues = struct_anon_17 	# /usr/include/X11/Xlib.h:218
class struct__XGC(Structure):
    __slots__ = [
    ]
struct__XGC._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XGC(Structure):
    __slots__ = [
    ]
struct__XGC._fields_ = [
    ('_opaque_struct', c_int)
]

GC = POINTER(struct__XGC) 	# /usr/include/X11/Xlib.h:233
class struct_anon_18(Structure):
    __slots__ = [
        'ext_data',
        'visualid',
        'class',
        'red_mask',
        'green_mask',
        'blue_mask',
        'bits_per_rgb',
        'map_entries',
    ]
struct_anon_18._fields_ = [
    ('ext_data', POINTER(XExtData)),
    ('visualid', VisualID),
    ('class', c_int),
    ('red_mask', c_ulong),
    ('green_mask', c_ulong),
    ('blue_mask', c_ulong),
    ('bits_per_rgb', c_int),
    ('map_entries', c_int),
]

Visual = struct_anon_18 	# /usr/include/X11/Xlib.h:249
class struct_anon_19(Structure):
    __slots__ = [
        'depth',
        'nvisuals',
        'visuals',
    ]
struct_anon_19._fields_ = [
    ('depth', c_int),
    ('nvisuals', c_int),
    ('visuals', POINTER(Visual)),
]

Depth = struct_anon_19 	# /usr/include/X11/Xlib.h:258
class struct_anon_20(Structure):
    __slots__ = [
        'ext_data',
        'display',
        'root',
        'width',
        'height',
        'mwidth',
        'mheight',
        'ndepths',
        'depths',
        'root_depth',
        'root_visual',
        'default_gc',
        'cmap',
        'white_pixel',
        'black_pixel',
        'max_maps',
        'min_maps',
        'backing_store',
        'save_unders',
        'root_input_mask',
    ]
class struct__XDisplay(Structure):
    __slots__ = [
    ]
struct__XDisplay._fields_ = [
    ('_opaque_struct', c_int)
]

struct_anon_20._fields_ = [
    ('ext_data', POINTER(XExtData)),
    ('display', POINTER(struct__XDisplay)),
    ('root', Window),
    ('width', c_int),
    ('height', c_int),
    ('mwidth', c_int),
    ('mheight', c_int),
    ('ndepths', c_int),
    ('depths', POINTER(Depth)),
    ('root_depth', c_int),
    ('root_visual', POINTER(Visual)),
    ('default_gc', GC),
    ('cmap', Colormap),
    ('white_pixel', c_ulong),
    ('black_pixel', c_ulong),
    ('max_maps', c_int),
    ('min_maps', c_int),
    ('backing_store', c_int),
    ('save_unders', c_int),
    ('root_input_mask', c_long),
]

Screen = struct_anon_20 	# /usr/include/X11/Xlib.h:286
class struct_anon_21(Structure):
    __slots__ = [
        'ext_data',
        'depth',
        'bits_per_pixel',
        'scanline_pad',
    ]
struct_anon_21._fields_ = [
    ('ext_data', POINTER(XExtData)),
    ('depth', c_int),
    ('bits_per_pixel', c_int),
    ('scanline_pad', c_int),
]

ScreenFormat = struct_anon_21 	# /usr/include/X11/Xlib.h:296
class struct_anon_22(Structure):
    __slots__ = [
        'background_pixmap',
        'background_pixel',
        'border_pixmap',
        'border_pixel',
        'bit_gravity',
        'win_gravity',
        'backing_store',
        'backing_planes',
        'backing_pixel',
        'save_under',
        'event_mask',
        'do_not_propagate_mask',
        'override_redirect',
        'colormap',
        'cursor',
    ]
struct_anon_22._fields_ = [
    ('background_pixmap', Pixmap),
    ('background_pixel', c_ulong),
    ('border_pixmap', Pixmap),
    ('border_pixel', c_ulong),
    ('bit_gravity', c_int),
    ('win_gravity', c_int),
    ('backing_store', c_int),
    ('backing_planes', c_ulong),
    ('backing_pixel', c_ulong),
    ('save_under', c_int),
    ('event_mask', c_long),
    ('do_not_propagate_mask', c_long),
    ('override_redirect', c_int),
    ('colormap', Colormap),
    ('cursor', Cursor),
]

XSetWindowAttributes = struct_anon_22 	# /usr/include/X11/Xlib.h:317
class struct_anon_23(Structure):
    __slots__ = [
        'x',
        'y',
        'width',
        'height',
        'border_width',
        'depth',
        'visual',
        'root',
        'class',
        'bit_gravity',
        'win_gravity',
        'backing_store',
        'backing_planes',
        'backing_pixel',
        'save_under',
        'colormap',
        'map_installed',
        'map_state',
        'all_event_masks',
        'your_event_mask',
        'do_not_propagate_mask',
        'override_redirect',
        'screen',
    ]
struct_anon_23._fields_ = [
    ('x', c_int),
    ('y', c_int),
    ('width', c_int),
    ('height', c_int),
    ('border_width', c_int),
    ('depth', c_int),
    ('visual', POINTER(Visual)),
    ('root', Window),
    ('class', c_int),
    ('bit_gravity', c_int),
    ('win_gravity', c_int),
    ('backing_store', c_int),
    ('backing_planes', c_ulong),
    ('backing_pixel', c_ulong),
    ('save_under', c_int),
    ('colormap', Colormap),
    ('map_installed', c_int),
    ('map_state', c_int),
    ('all_event_masks', c_long),
    ('your_event_mask', c_long),
    ('do_not_propagate_mask', c_long),
    ('override_redirect', c_int),
    ('screen', POINTER(Screen)),
]

XWindowAttributes = struct_anon_23 	# /usr/include/X11/Xlib.h:345
class struct_anon_24(Structure):
    __slots__ = [
        'family',
        'length',
        'address',
    ]
struct_anon_24._fields_ = [
    ('family', c_int),
    ('length', c_int),
    ('address', c_char_p),
]

XHostAddress = struct_anon_24 	# /usr/include/X11/Xlib.h:356
class struct_anon_25(Structure):
    __slots__ = [
        'typelength',
        'valuelength',
        'type',
        'value',
    ]
struct_anon_25._fields_ = [
    ('typelength', c_int),
    ('valuelength', c_int),
    ('type', c_char_p),
    ('value', c_char_p),
]

XServerInterpretedAddress = struct_anon_25 	# /usr/include/X11/Xlib.h:366
class struct__XImage(Structure):
    __slots__ = [
        'width',
        'height',
        'xoffset',
        'format',
        'data',
        'byte_order',
        'bitmap_unit',
        'bitmap_bit_order',
        'bitmap_pad',
        'depth',
        'bytes_per_line',
        'bits_per_pixel',
        'red_mask',
        'green_mask',
        'blue_mask',
        'obdata',
        'f',
    ]
class struct_funcs(Structure):
    __slots__ = [
        'create_image',
        'destroy_image',
        'get_pixel',
        'put_pixel',
        'sub_image',
        'add_pixel',
    ]
class struct__XDisplay(Structure):
    __slots__ = [
    ]
struct__XDisplay._fields_ = [
    ('_opaque_struct', c_int)
]

struct_funcs._fields_ = [
    ('create_image', POINTER(CFUNCTYPE(POINTER(struct__XImage), POINTER(struct__XDisplay), POINTER(Visual), c_uint, c_int, c_int, c_char_p, c_uint, c_uint, c_int, c_int))),
    ('destroy_image', POINTER(CFUNCTYPE(c_int, POINTER(struct__XImage)))),
    ('get_pixel', POINTER(CFUNCTYPE(c_ulong, POINTER(struct__XImage), c_int, c_int))),
    ('put_pixel', POINTER(CFUNCTYPE(c_int, POINTER(struct__XImage), c_int, c_int, c_ulong))),
    ('sub_image', POINTER(CFUNCTYPE(POINTER(struct__XImage), POINTER(struct__XImage), c_int, c_int, c_uint, c_uint))),
    ('add_pixel', POINTER(CFUNCTYPE(c_int, POINTER(struct__XImage), c_long))),
]

struct__XImage._fields_ = [
    ('width', c_int),
    ('height', c_int),
    ('xoffset', c_int),
    ('format', c_int),
    ('data', c_char_p),
    ('byte_order', c_int),
    ('bitmap_unit', c_int),
    ('bitmap_bit_order', c_int),
    ('bitmap_pad', c_int),
    ('depth', c_int),
    ('bytes_per_line', c_int),
    ('bits_per_pixel', c_int),
    ('red_mask', c_ulong),
    ('green_mask', c_ulong),
    ('blue_mask', c_ulong),
    ('obdata', XPointer),
    ('f', struct_funcs),
]

XImage = struct__XImage 	# /usr/include/X11/Xlib.h:405
class struct_anon_26(Structure):
    __slots__ = [
        'x',
        'y',
        'width',
        'height',
        'border_width',
        'sibling',
        'stack_mode',
    ]
struct_anon_26._fields_ = [
    ('x', c_int),
    ('y', c_int),
    ('width', c_int),
    ('height', c_int),
    ('border_width', c_int),
    ('sibling', Window),
    ('stack_mode', c_int),
]

XWindowChanges = struct_anon_26 	# /usr/include/X11/Xlib.h:416
class struct_anon_27(Structure):
    __slots__ = [
        'pixel',
        'red',
        'green',
        'blue',
        'flags',
        'pad',
    ]
struct_anon_27._fields_ = [
    ('pixel', c_ulong),
    ('red', c_ushort),
    ('green', c_ushort),
    ('blue', c_ushort),
    ('flags', c_char),
    ('pad', c_char),
]

XColor = struct_anon_27 	# /usr/include/X11/Xlib.h:426
class struct_anon_28(Structure):
    __slots__ = [
        'x1',
        'y1',
        'x2',
        'y2',
    ]
struct_anon_28._fields_ = [
    ('x1', c_short),
    ('y1', c_short),
    ('x2', c_short),
    ('y2', c_short),
]

XSegment = struct_anon_28 	# /usr/include/X11/Xlib.h:435
class struct_anon_29(Structure):
    __slots__ = [
        'x',
        'y',
    ]
struct_anon_29._fields_ = [
    ('x', c_short),
    ('y', c_short),
]

XPoint = struct_anon_29 	# /usr/include/X11/Xlib.h:439
class struct_anon_30(Structure):
    __slots__ = [
        'x',
        'y',
        'width',
        'height',
    ]
struct_anon_30._fields_ = [
    ('x', c_short),
    ('y', c_short),
    ('width', c_ushort),
    ('height', c_ushort),
]

XRectangle = struct_anon_30 	# /usr/include/X11/Xlib.h:444
class struct_anon_31(Structure):
    __slots__ = [
        'x',
        'y',
        'width',
        'height',
        'angle1',
        'angle2',
    ]
struct_anon_31._fields_ = [
    ('x', c_short),
    ('y', c_short),
    ('width', c_ushort),
    ('height', c_ushort),
    ('angle1', c_short),
    ('angle2', c_short),
]

XArc = struct_anon_31 	# /usr/include/X11/Xlib.h:450
class struct_anon_32(Structure):
    __slots__ = [
        'key_click_percent',
        'bell_percent',
        'bell_pitch',
        'bell_duration',
        'led',
        'led_mode',
        'key',
        'auto_repeat_mode',
    ]
struct_anon_32._fields_ = [
    ('key_click_percent', c_int),
    ('bell_percent', c_int),
    ('bell_pitch', c_int),
    ('bell_duration', c_int),
    ('led', c_int),
    ('led_mode', c_int),
    ('key', c_int),
    ('auto_repeat_mode', c_int),
]

XKeyboardControl = struct_anon_32 	# /usr/include/X11/Xlib.h:464
class struct_anon_33(Structure):
    __slots__ = [
        'key_click_percent',
        'bell_percent',
        'bell_pitch',
        'bell_duration',
        'led_mask',
        'global_auto_repeat',
        'auto_repeats',
    ]
struct_anon_33._fields_ = [
    ('key_click_percent', c_int),
    ('bell_percent', c_int),
    ('bell_pitch', c_uint),
    ('bell_duration', c_uint),
    ('led_mask', c_ulong),
    ('global_auto_repeat', c_int),
    ('auto_repeats', c_char * 32),
]

XKeyboardState = struct_anon_33 	# /usr/include/X11/Xlib.h:475
class struct_anon_34(Structure):
    __slots__ = [
        'time',
        'x',
        'y',
    ]
struct_anon_34._fields_ = [
    ('time', Time),
    ('x', c_short),
    ('y', c_short),
]

XTimeCoord = struct_anon_34 	# /usr/include/X11/Xlib.h:482
class struct_anon_35(Structure):
    __slots__ = [
        'max_keypermod',
        'modifiermap',
    ]
struct_anon_35._fields_ = [
    ('max_keypermod', c_int),
    ('modifiermap', POINTER(KeyCode)),
]

XModifierKeymap = struct_anon_35 	# /usr/include/X11/Xlib.h:489
class struct__XDisplay(Structure):
    __slots__ = [
    ]
struct__XDisplay._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XDisplay(Structure):
    __slots__ = [
    ]
struct__XDisplay._fields_ = [
    ('_opaque_struct', c_int)
]

Display = struct__XDisplay 	# /usr/include/X11/Xlib.h:498
class struct_anon_36(Structure):
    __slots__ = [
        'ext_data',
        'private1',
        'fd',
        'private2',
        'proto_major_version',
        'proto_minor_version',
        'vendor',
        'private3',
        'private4',
        'private5',
        'private6',
        'resource_alloc',
        'byte_order',
        'bitmap_unit',
        'bitmap_pad',
        'bitmap_bit_order',
        'nformats',
        'pixmap_format',
        'private8',
        'release',
        'private9',
        'private10',
        'qlen',
        'last_request_read',
        'request',
        'private11',
        'private12',
        'private13',
        'private14',
        'max_request_size',
        'db',
        'private15',
        'display_name',
        'default_screen',
        'nscreens',
        'screens',
        'motion_buffer',
        'private16',
        'min_keycode',
        'max_keycode',
        'private17',
        'private18',
        'private19',
        'xdefaults',
    ]
class struct__XPrivate(Structure):
    __slots__ = [
    ]
struct__XPrivate._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XDisplay(Structure):
    __slots__ = [
    ]
struct__XDisplay._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XPrivate(Structure):
    __slots__ = [
    ]
struct__XPrivate._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XPrivate(Structure):
    __slots__ = [
    ]
struct__XPrivate._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XrmHashBucketRec(Structure):
    __slots__ = [
    ]
struct__XrmHashBucketRec._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XDisplay(Structure):
    __slots__ = [
    ]
struct__XDisplay._fields_ = [
    ('_opaque_struct', c_int)
]

struct_anon_36._fields_ = [
    ('ext_data', POINTER(XExtData)),
    ('private1', POINTER(struct__XPrivate)),
    ('fd', c_int),
    ('private2', c_int),
    ('proto_major_version', c_int),
    ('proto_minor_version', c_int),
    ('vendor', c_char_p),
    ('private3', XID),
    ('private4', XID),
    ('private5', XID),
    ('private6', c_int),
    ('resource_alloc', POINTER(CFUNCTYPE(XID, POINTER(struct__XDisplay)))),
    ('byte_order', c_int),
    ('bitmap_unit', c_int),
    ('bitmap_pad', c_int),
    ('bitmap_bit_order', c_int),
    ('nformats', c_int),
    ('pixmap_format', POINTER(ScreenFormat)),
    ('private8', c_int),
    ('release', c_int),
    ('private9', POINTER(struct__XPrivate)),
    ('private10', POINTER(struct__XPrivate)),
    ('qlen', c_int),
    ('last_request_read', c_ulong),
    ('request', c_ulong),
    ('private11', XPointer),
    ('private12', XPointer),
    ('private13', XPointer),
    ('private14', XPointer),
    ('max_request_size', c_uint),
    ('db', POINTER(struct__XrmHashBucketRec)),
    ('private15', POINTER(CFUNCTYPE(c_int, POINTER(struct__XDisplay)))),
    ('display_name', c_char_p),
    ('default_screen', c_int),
    ('nscreens', c_int),
    ('screens', POINTER(Screen)),
    ('motion_buffer', c_ulong),
    ('private16', c_ulong),
    ('min_keycode', c_int),
    ('max_keycode', c_int),
    ('private17', XPointer),
    ('private18', XPointer),
    ('private19', c_int),
    ('xdefaults', c_char_p),
]

_XPrivDisplay = POINTER(struct_anon_36) 	# /usr/include/X11/Xlib.h:561
class struct_anon_37(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'root',
        'subwindow',
        'time',
        'x',
        'y',
        'x_root',
        'y_root',
        'state',
        'keycode',
        'same_screen',
    ]
struct_anon_37._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('root', Window),
    ('subwindow', Window),
    ('time', Time),
    ('x', c_int),
    ('y', c_int),
    ('x_root', c_int),
    ('y_root', c_int),
    ('state', c_uint),
    ('keycode', c_uint),
    ('same_screen', c_int),
]

XKeyEvent = struct_anon_37 	# /usr/include/X11/Xlib.h:582
XKeyPressedEvent = XKeyEvent 	# /usr/include/X11/Xlib.h:583
XKeyReleasedEvent = XKeyEvent 	# /usr/include/X11/Xlib.h:584
class struct_anon_38(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'root',
        'subwindow',
        'time',
        'x',
        'y',
        'x_root',
        'y_root',
        'state',
        'button',
        'same_screen',
    ]
struct_anon_38._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('root', Window),
    ('subwindow', Window),
    ('time', Time),
    ('x', c_int),
    ('y', c_int),
    ('x_root', c_int),
    ('y_root', c_int),
    ('state', c_uint),
    ('button', c_uint),
    ('same_screen', c_int),
]

XButtonEvent = struct_anon_38 	# /usr/include/X11/Xlib.h:600
XButtonPressedEvent = XButtonEvent 	# /usr/include/X11/Xlib.h:601
XButtonReleasedEvent = XButtonEvent 	# /usr/include/X11/Xlib.h:602
class struct_anon_39(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'root',
        'subwindow',
        'time',
        'x',
        'y',
        'x_root',
        'y_root',
        'state',
        'is_hint',
        'same_screen',
    ]
struct_anon_39._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('root', Window),
    ('subwindow', Window),
    ('time', Time),
    ('x', c_int),
    ('y', c_int),
    ('x_root', c_int),
    ('y_root', c_int),
    ('state', c_uint),
    ('is_hint', c_char),
    ('same_screen', c_int),
]

XMotionEvent = struct_anon_39 	# /usr/include/X11/Xlib.h:618
XPointerMovedEvent = XMotionEvent 	# /usr/include/X11/Xlib.h:619
class struct_anon_40(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'root',
        'subwindow',
        'time',
        'x',
        'y',
        'x_root',
        'y_root',
        'mode',
        'detail',
        'same_screen',
        'focus',
        'state',
    ]
struct_anon_40._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('root', Window),
    ('subwindow', Window),
    ('time', Time),
    ('x', c_int),
    ('y', c_int),
    ('x_root', c_int),
    ('y_root', c_int),
    ('mode', c_int),
    ('detail', c_int),
    ('same_screen', c_int),
    ('focus', c_int),
    ('state', c_uint),
]

XCrossingEvent = struct_anon_40 	# /usr/include/X11/Xlib.h:641
XEnterWindowEvent = XCrossingEvent 	# /usr/include/X11/Xlib.h:642
XLeaveWindowEvent = XCrossingEvent 	# /usr/include/X11/Xlib.h:643
class struct_anon_41(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'mode',
        'detail',
    ]
struct_anon_41._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('mode', c_int),
    ('detail', c_int),
]

XFocusChangeEvent = struct_anon_41 	# /usr/include/X11/Xlib.h:659
XFocusInEvent = XFocusChangeEvent 	# /usr/include/X11/Xlib.h:660
XFocusOutEvent = XFocusChangeEvent 	# /usr/include/X11/Xlib.h:661
class struct_anon_42(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'key_vector',
    ]
struct_anon_42._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('key_vector', c_char * 32),
]

XKeymapEvent = struct_anon_42 	# /usr/include/X11/Xlib.h:671
class struct_anon_43(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'x',
        'y',
        'width',
        'height',
        'count',
    ]
struct_anon_43._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('x', c_int),
    ('y', c_int),
    ('width', c_int),
    ('height', c_int),
    ('count', c_int),
]

XExposeEvent = struct_anon_43 	# /usr/include/X11/Xlib.h:682
class struct_anon_44(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'drawable',
        'x',
        'y',
        'width',
        'height',
        'count',
        'major_code',
        'minor_code',
    ]
struct_anon_44._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('drawable', Drawable),
    ('x', c_int),
    ('y', c_int),
    ('width', c_int),
    ('height', c_int),
    ('count', c_int),
    ('major_code', c_int),
    ('minor_code', c_int),
]

XGraphicsExposeEvent = struct_anon_44 	# /usr/include/X11/Xlib.h:695
class struct_anon_45(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'drawable',
        'major_code',
        'minor_code',
    ]
struct_anon_45._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('drawable', Drawable),
    ('major_code', c_int),
    ('minor_code', c_int),
]

XNoExposeEvent = struct_anon_45 	# /usr/include/X11/Xlib.h:705
class struct_anon_46(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'state',
    ]
struct_anon_46._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('state', c_int),
]

XVisibilityEvent = struct_anon_46 	# /usr/include/X11/Xlib.h:714
class struct_anon_47(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'parent',
        'window',
        'x',
        'y',
        'width',
        'height',
        'border_width',
        'override_redirect',
    ]
struct_anon_47._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('parent', Window),
    ('window', Window),
    ('x', c_int),
    ('y', c_int),
    ('width', c_int),
    ('height', c_int),
    ('border_width', c_int),
    ('override_redirect', c_int),
]

XCreateWindowEvent = struct_anon_47 	# /usr/include/X11/Xlib.h:727
class struct_anon_48(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'event',
        'window',
    ]
struct_anon_48._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('event', Window),
    ('window', Window),
]

XDestroyWindowEvent = struct_anon_48 	# /usr/include/X11/Xlib.h:736
class struct_anon_49(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'event',
        'window',
        'from_configure',
    ]
struct_anon_49._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('event', Window),
    ('window', Window),
    ('from_configure', c_int),
]

XUnmapEvent = struct_anon_49 	# /usr/include/X11/Xlib.h:746
class struct_anon_50(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'event',
        'window',
        'override_redirect',
    ]
struct_anon_50._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('event', Window),
    ('window', Window),
    ('override_redirect', c_int),
]

XMapEvent = struct_anon_50 	# /usr/include/X11/Xlib.h:756
class struct_anon_51(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'parent',
        'window',
    ]
struct_anon_51._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('parent', Window),
    ('window', Window),
]

XMapRequestEvent = struct_anon_51 	# /usr/include/X11/Xlib.h:765
class struct_anon_52(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'event',
        'window',
        'parent',
        'x',
        'y',
        'override_redirect',
    ]
struct_anon_52._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('event', Window),
    ('window', Window),
    ('parent', Window),
    ('x', c_int),
    ('y', c_int),
    ('override_redirect', c_int),
]

XReparentEvent = struct_anon_52 	# /usr/include/X11/Xlib.h:777
class struct_anon_53(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'event',
        'window',
        'x',
        'y',
        'width',
        'height',
        'border_width',
        'above',
        'override_redirect',
    ]
struct_anon_53._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('event', Window),
    ('window', Window),
    ('x', c_int),
    ('y', c_int),
    ('width', c_int),
    ('height', c_int),
    ('border_width', c_int),
    ('above', Window),
    ('override_redirect', c_int),
]

XConfigureEvent = struct_anon_53 	# /usr/include/X11/Xlib.h:791
class struct_anon_54(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'event',
        'window',
        'x',
        'y',
    ]
struct_anon_54._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('event', Window),
    ('window', Window),
    ('x', c_int),
    ('y', c_int),
]

XGravityEvent = struct_anon_54 	# /usr/include/X11/Xlib.h:801
class struct_anon_55(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'width',
        'height',
    ]
struct_anon_55._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('width', c_int),
    ('height', c_int),
]

XResizeRequestEvent = struct_anon_55 	# /usr/include/X11/Xlib.h:810
class struct_anon_56(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'parent',
        'window',
        'x',
        'y',
        'width',
        'height',
        'border_width',
        'above',
        'detail',
        'value_mask',
    ]
struct_anon_56._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('parent', Window),
    ('window', Window),
    ('x', c_int),
    ('y', c_int),
    ('width', c_int),
    ('height', c_int),
    ('border_width', c_int),
    ('above', Window),
    ('detail', c_int),
    ('value_mask', c_ulong),
]

XConfigureRequestEvent = struct_anon_56 	# /usr/include/X11/Xlib.h:825
class struct_anon_57(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'event',
        'window',
        'place',
    ]
struct_anon_57._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('event', Window),
    ('window', Window),
    ('place', c_int),
]

XCirculateEvent = struct_anon_57 	# /usr/include/X11/Xlib.h:835
class struct_anon_58(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'parent',
        'window',
        'place',
    ]
struct_anon_58._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('parent', Window),
    ('window', Window),
    ('place', c_int),
]

XCirculateRequestEvent = struct_anon_58 	# /usr/include/X11/Xlib.h:845
class struct_anon_59(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'atom',
        'time',
        'state',
    ]
struct_anon_59._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('atom', Atom),
    ('time', Time),
    ('state', c_int),
]

XPropertyEvent = struct_anon_59 	# /usr/include/X11/Xlib.h:856
class struct_anon_60(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'selection',
        'time',
    ]
struct_anon_60._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('selection', Atom),
    ('time', Time),
]

XSelectionClearEvent = struct_anon_60 	# /usr/include/X11/Xlib.h:866
class struct_anon_61(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'owner',
        'requestor',
        'selection',
        'target',
        'property',
        'time',
    ]
struct_anon_61._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('owner', Window),
    ('requestor', Window),
    ('selection', Atom),
    ('target', Atom),
    ('property', Atom),
    ('time', Time),
]

XSelectionRequestEvent = struct_anon_61 	# /usr/include/X11/Xlib.h:879
class struct_anon_62(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'requestor',
        'selection',
        'target',
        'property',
        'time',
    ]
struct_anon_62._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('requestor', Window),
    ('selection', Atom),
    ('target', Atom),
    ('property', Atom),
    ('time', Time),
]

XSelectionEvent = struct_anon_62 	# /usr/include/X11/Xlib.h:891
class struct_anon_63(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'colormap',
        'new',
        'state',
    ]
struct_anon_63._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('colormap', Colormap),
    ('new', c_int),
    ('state', c_int),
]

XColormapEvent = struct_anon_63 	# /usr/include/X11/Xlib.h:906
class struct_anon_64(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'message_type',
        'format',
        'data',
    ]
class struct_anon_65(Union):
    __slots__ = [
        'b',
        's',
        'l',
    ]
struct_anon_65._fields_ = [
    ('b', c_char * 20),
    ('s', c_short * 10),
    ('l', c_long * 5),
]

struct_anon_64._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('message_type', Atom),
    ('format', c_int),
    ('data', struct_anon_65),
]

XClientMessageEvent = struct_anon_64 	# /usr/include/X11/Xlib.h:921
class struct_anon_66(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'request',
        'first_keycode',
        'count',
    ]
struct_anon_66._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('request', c_int),
    ('first_keycode', c_int),
    ('count', c_int),
]

XMappingEvent = struct_anon_66 	# /usr/include/X11/Xlib.h:933
class struct_anon_67(Structure):
    __slots__ = [
        'type',
        'display',
        'resourceid',
        'serial',
        'error_code',
        'request_code',
        'minor_code',
    ]
struct_anon_67._fields_ = [
    ('type', c_int),
    ('display', POINTER(Display)),
    ('resourceid', XID),
    ('serial', c_ulong),
    ('error_code', c_ubyte),
    ('request_code', c_ubyte),
    ('minor_code', c_ubyte),
]

XErrorEvent = struct_anon_67 	# /usr/include/X11/Xlib.h:943
class struct_anon_68(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
    ]
struct_anon_68._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
]

XAnyEvent = struct_anon_68 	# /usr/include/X11/Xlib.h:951
class struct_anon_69(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'extension',
        'evtype',
    ]
struct_anon_69._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('extension', c_int),
    ('evtype', c_int),
]

XGenericEvent = struct_anon_69 	# /usr/include/X11/Xlib.h:967
class struct_anon_70(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'extension',
        'evtype',
        'cookie',
        'data',
    ]
struct_anon_70._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('extension', c_int),
    ('evtype', c_int),
    ('cookie', c_uint),
    ('data', POINTER(None)),
]

XGenericEventCookie = struct_anon_70 	# /usr/include/X11/Xlib.h:978
class struct__XEvent(Union):
    __slots__ = [
        'type',
        'xany',
        'xkey',
        'xbutton',
        'xmotion',
        'xcrossing',
        'xfocus',
        'xexpose',
        'xgraphicsexpose',
        'xnoexpose',
        'xvisibility',
        'xcreatewindow',
        'xdestroywindow',
        'xunmap',
        'xmap',
        'xmaprequest',
        'xreparent',
        'xconfigure',
        'xgravity',
        'xresizerequest',
        'xconfigurerequest',
        'xcirculate',
        'xcirculaterequest',
        'xproperty',
        'xselectionclear',
        'xselectionrequest',
        'xselection',
        'xcolormap',
        'xclient',
        'xmapping',
        'xerror',
        'xkeymap',
        'xgeneric',
        'xcookie',
        'pad',
    ]
struct__XEvent._fields_ = [
    ('type', c_int),
    ('xany', XAnyEvent),
    ('xkey', XKeyEvent),
    ('xbutton', XButtonEvent),
    ('xmotion', XMotionEvent),
    ('xcrossing', XCrossingEvent),
    ('xfocus', XFocusChangeEvent),
    ('xexpose', XExposeEvent),
    ('xgraphicsexpose', XGraphicsExposeEvent),
    ('xnoexpose', XNoExposeEvent),
    ('xvisibility', XVisibilityEvent),
    ('xcreatewindow', XCreateWindowEvent),
    ('xdestroywindow', XDestroyWindowEvent),
    ('xunmap', XUnmapEvent),
    ('xmap', XMapEvent),
    ('xmaprequest', XMapRequestEvent),
    ('xreparent', XReparentEvent),
    ('xconfigure', XConfigureEvent),
    ('xgravity', XGravityEvent),
    ('xresizerequest', XResizeRequestEvent),
    ('xconfigurerequest', XConfigureRequestEvent),
    ('xcirculate', XCirculateEvent),
    ('xcirculaterequest', XCirculateRequestEvent),
    ('xproperty', XPropertyEvent),
    ('xselectionclear', XSelectionClearEvent),
    ('xselectionrequest', XSelectionRequestEvent),
    ('xselection', XSelectionEvent),
    ('xcolormap', XColormapEvent),
    ('xclient', XClientMessageEvent),
    ('xmapping', XMappingEvent),
    ('xerror', XErrorEvent),
    ('xkeymap', XKeymapEvent),
    ('xgeneric', XGenericEvent),
    ('xcookie', XGenericEventCookie),
    ('pad', c_long * 24),
]

XEvent = struct__XEvent 	# /usr/include/X11/Xlib.h:1020
class struct_anon_71(Structure):
    __slots__ = [
        'lbearing',
        'rbearing',
        'width',
        'ascent',
        'descent',
        'attributes',
    ]
struct_anon_71._fields_ = [
    ('lbearing', c_short),
    ('rbearing', c_short),
    ('width', c_short),
    ('ascent', c_short),
    ('descent', c_short),
    ('attributes', c_ushort),
]

XCharStruct = struct_anon_71 	# /usr/include/X11/Xlib.h:1035
class struct_anon_72(Structure):
    __slots__ = [
        'name',
        'card32',
    ]
struct_anon_72._fields_ = [
    ('name', Atom),
    ('card32', c_ulong),
]

XFontProp = struct_anon_72 	# /usr/include/X11/Xlib.h:1044
class struct_anon_73(Structure):
    __slots__ = [
        'ext_data',
        'fid',
        'direction',
        'min_char_or_byte2',
        'max_char_or_byte2',
        'min_byte1',
        'max_byte1',
        'all_chars_exist',
        'default_char',
        'n_properties',
        'properties',
        'min_bounds',
        'max_bounds',
        'per_char',
        'ascent',
        'descent',
    ]
struct_anon_73._fields_ = [
    ('ext_data', POINTER(XExtData)),
    ('fid', Font),
    ('direction', c_uint),
    ('min_char_or_byte2', c_uint),
    ('max_char_or_byte2', c_uint),
    ('min_byte1', c_uint),
    ('max_byte1', c_uint),
    ('all_chars_exist', c_int),
    ('default_char', c_uint),
    ('n_properties', c_int),
    ('properties', POINTER(XFontProp)),
    ('min_bounds', XCharStruct),
    ('max_bounds', XCharStruct),
    ('per_char', POINTER(XCharStruct)),
    ('ascent', c_int),
    ('descent', c_int),
]

XFontStruct = struct_anon_73 	# /usr/include/X11/Xlib.h:1063
class struct_anon_74(Structure):
    __slots__ = [
        'chars',
        'nchars',
        'delta',
        'font',
    ]
struct_anon_74._fields_ = [
    ('chars', c_char_p),
    ('nchars', c_int),
    ('delta', c_int),
    ('font', Font),
]

XTextItem = struct_anon_74 	# /usr/include/X11/Xlib.h:1073
class struct_anon_75(Structure):
    __slots__ = [
        'byte1',
        'byte2',
    ]
struct_anon_75._fields_ = [
    ('byte1', c_ubyte),
    ('byte2', c_ubyte),
]

XChar2b = struct_anon_75 	# /usr/include/X11/Xlib.h:1078
class struct_anon_76(Structure):
    __slots__ = [
        'chars',
        'nchars',
        'delta',
        'font',
    ]
struct_anon_76._fields_ = [
    ('chars', POINTER(XChar2b)),
    ('nchars', c_int),
    ('delta', c_int),
    ('font', Font),
]

XTextItem16 = struct_anon_76 	# /usr/include/X11/Xlib.h:1085
class struct_anon_77(Union):
    __slots__ = [
        'display',
        'gc',
        'visual',
        'screen',
        'pixmap_format',
        'font',
    ]
struct_anon_77._fields_ = [
    ('display', POINTER(Display)),
    ('gc', GC),
    ('visual', POINTER(Visual)),
    ('screen', POINTER(Screen)),
    ('pixmap_format', POINTER(ScreenFormat)),
    ('font', POINTER(XFontStruct)),
]

XEDataObject = struct_anon_77 	# /usr/include/X11/Xlib.h:1093
class struct_anon_78(Structure):
    __slots__ = [
        'max_ink_extent',
        'max_logical_extent',
    ]
struct_anon_78._fields_ = [
    ('max_ink_extent', XRectangle),
    ('max_logical_extent', XRectangle),
]

XFontSetExtents = struct_anon_78 	# /usr/include/X11/Xlib.h:1098
class struct__XOM(Structure):
    __slots__ = [
    ]
struct__XOM._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XOM(Structure):
    __slots__ = [
    ]
struct__XOM._fields_ = [
    ('_opaque_struct', c_int)
]

XOM = POINTER(struct__XOM) 	# /usr/include/X11/Xlib.h:1104
class struct__XOC(Structure):
    __slots__ = [
    ]
struct__XOC._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XOC(Structure):
    __slots__ = [
    ]
struct__XOC._fields_ = [
    ('_opaque_struct', c_int)
]

XOC = POINTER(struct__XOC) 	# /usr/include/X11/Xlib.h:1105
class struct__XOC(Structure):
    __slots__ = [
    ]
struct__XOC._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XOC(Structure):
    __slots__ = [
    ]
struct__XOC._fields_ = [
    ('_opaque_struct', c_int)
]

XFontSet = POINTER(struct__XOC) 	# /usr/include/X11/Xlib.h:1105
class struct_anon_79(Structure):
    __slots__ = [
        'chars',
        'nchars',
        'delta',
        'font_set',
    ]
struct_anon_79._fields_ = [
    ('chars', c_char_p),
    ('nchars', c_int),
    ('delta', c_int),
    ('font_set', XFontSet),
]

XmbTextItem = struct_anon_79 	# /usr/include/X11/Xlib.h:1112
class struct_anon_80(Structure):
    __slots__ = [
        'chars',
        'nchars',
        'delta',
        'font_set',
    ]
struct_anon_80._fields_ = [
    ('chars', c_wchar_p),
    ('nchars', c_int),
    ('delta', c_int),
    ('font_set', XFontSet),
]

XwcTextItem = struct_anon_80 	# /usr/include/X11/Xlib.h:1119
class struct_anon_81(Structure):
    __slots__ = [
        'charset_count',
        'charset_list',
    ]
struct_anon_81._fields_ = [
    ('charset_count', c_int),
    ('charset_list', POINTER(c_char_p)),
]

XOMCharSetList = struct_anon_81 	# /usr/include/X11/Xlib.h:1135
enum_anon_82 = c_int
XOMOrientation_LTR_TTB = 0
XOMOrientation_RTL_TTB = 1
XOMOrientation_TTB_LTR = 2
XOMOrientation_TTB_RTL = 3
XOMOrientation_Context = 4
XOrientation = enum_anon_82 	# /usr/include/X11/Xlib.h:1143
class struct_anon_83(Structure):
    __slots__ = [
        'num_orientation',
        'orientation',
    ]
struct_anon_83._fields_ = [
    ('num_orientation', c_int),
    ('orientation', POINTER(XOrientation)),
]

XOMOrientation = struct_anon_83 	# /usr/include/X11/Xlib.h:1148
class struct_anon_84(Structure):
    __slots__ = [
        'num_font',
        'font_struct_list',
        'font_name_list',
    ]
struct_anon_84._fields_ = [
    ('num_font', c_int),
    ('font_struct_list', POINTER(POINTER(XFontStruct))),
    ('font_name_list', POINTER(c_char_p)),
]

XOMFontInfo = struct_anon_84 	# /usr/include/X11/Xlib.h:1154
class struct__XIM(Structure):
    __slots__ = [
    ]
struct__XIM._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XIM(Structure):
    __slots__ = [
    ]
struct__XIM._fields_ = [
    ('_opaque_struct', c_int)
]

XIM = POINTER(struct__XIM) 	# /usr/include/X11/Xlib.h:1156
class struct__XIC(Structure):
    __slots__ = [
    ]
struct__XIC._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XIC(Structure):
    __slots__ = [
    ]
struct__XIC._fields_ = [
    ('_opaque_struct', c_int)
]

XIC = POINTER(struct__XIC) 	# /usr/include/X11/Xlib.h:1157
XIMProc = CFUNCTYPE(None, XIM, XPointer, XPointer) 	# /usr/include/X11/Xlib.h:1159
XICProc = CFUNCTYPE(c_int, XIC, XPointer, XPointer) 	# /usr/include/X11/Xlib.h:1165
XIDProc = CFUNCTYPE(None, POINTER(Display), XPointer, XPointer) 	# /usr/include/X11/Xlib.h:1171
XIMStyle = c_ulong 	# /usr/include/X11/Xlib.h:1177
class struct_anon_85(Structure):
    __slots__ = [
        'count_styles',
        'supported_styles',
    ]
struct_anon_85._fields_ = [
    ('count_styles', c_ushort),
    ('supported_styles', POINTER(XIMStyle)),
]

XIMStyles = struct_anon_85 	# /usr/include/X11/Xlib.h:1182
XIMPreeditArea = 1 	# /usr/include/X11/Xlib.h:1184
XIMPreeditCallbacks = 2 	# /usr/include/X11/Xlib.h:1185
XIMPreeditPosition = 4 	# /usr/include/X11/Xlib.h:1186
XIMPreeditNothing = 8 	# /usr/include/X11/Xlib.h:1187
XIMPreeditNone = 16 	# /usr/include/X11/Xlib.h:1188
XIMStatusArea = 256 	# /usr/include/X11/Xlib.h:1189
XIMStatusCallbacks = 512 	# /usr/include/X11/Xlib.h:1190
XIMStatusNothing = 1024 	# /usr/include/X11/Xlib.h:1191
XIMStatusNone = 2048 	# /usr/include/X11/Xlib.h:1192
XBufferOverflow = -1 	# /usr/include/X11/Xlib.h:1238
XLookupNone = 1 	# /usr/include/X11/Xlib.h:1239
XLookupChars = 2 	# /usr/include/X11/Xlib.h:1240
XLookupKeySym = 3 	# /usr/include/X11/Xlib.h:1241
XLookupBoth = 4 	# /usr/include/X11/Xlib.h:1242
XVaNestedList = POINTER(None) 	# /usr/include/X11/Xlib.h:1244
class struct_anon_86(Structure):
    __slots__ = [
        'client_data',
        'callback',
    ]
struct_anon_86._fields_ = [
    ('client_data', XPointer),
    ('callback', XIMProc),
]

XIMCallback = struct_anon_86 	# /usr/include/X11/Xlib.h:1249
class struct_anon_87(Structure):
    __slots__ = [
        'client_data',
        'callback',
    ]
struct_anon_87._fields_ = [
    ('client_data', XPointer),
    ('callback', XICProc),
]

XICCallback = struct_anon_87 	# /usr/include/X11/Xlib.h:1254
XIMFeedback = c_ulong 	# /usr/include/X11/Xlib.h:1256
XIMReverse = 1 	# /usr/include/X11/Xlib.h:1258
XIMUnderline = 2 	# /usr/include/X11/Xlib.h:1259
XIMHighlight = 4 	# /usr/include/X11/Xlib.h:1260
XIMPrimary = 32 	# /usr/include/X11/Xlib.h:1261
XIMSecondary = 64 	# /usr/include/X11/Xlib.h:1262
XIMTertiary = 128 	# /usr/include/X11/Xlib.h:1263
XIMVisibleToForward = 256 	# /usr/include/X11/Xlib.h:1264
XIMVisibleToBackword = 512 	# /usr/include/X11/Xlib.h:1265
XIMVisibleToCenter = 1024 	# /usr/include/X11/Xlib.h:1266
class struct__XIMText(Structure):
    __slots__ = [
        'length',
        'feedback',
        'encoding_is_wchar',
        'string',
    ]
class struct_anon_88(Union):
    __slots__ = [
        'multi_byte',
        'wide_char',
    ]
struct_anon_88._fields_ = [
    ('multi_byte', c_char_p),
    ('wide_char', c_wchar_p),
]

struct__XIMText._fields_ = [
    ('length', c_ushort),
    ('feedback', POINTER(XIMFeedback)),
    ('encoding_is_wchar', c_int),
    ('string', struct_anon_88),
]

XIMText = struct__XIMText 	# /usr/include/X11/Xlib.h:1276
XIMPreeditState = c_ulong 	# /usr/include/X11/Xlib.h:1278
XIMPreeditUnKnown = 0 	# /usr/include/X11/Xlib.h:1280
XIMPreeditEnable = 1 	# /usr/include/X11/Xlib.h:1281
XIMPreeditDisable = 2 	# /usr/include/X11/Xlib.h:1282
class struct__XIMPreeditStateNotifyCallbackStruct(Structure):
    __slots__ = [
        'state',
    ]
struct__XIMPreeditStateNotifyCallbackStruct._fields_ = [
    ('state', XIMPreeditState),
]

XIMPreeditStateNotifyCallbackStruct = struct__XIMPreeditStateNotifyCallbackStruct 	# /usr/include/X11/Xlib.h:1286
XIMResetState = c_ulong 	# /usr/include/X11/Xlib.h:1288
XIMInitialState = 1 	# /usr/include/X11/Xlib.h:1290
XIMPreserveState = 2 	# /usr/include/X11/Xlib.h:1291
XIMStringConversionFeedback = c_ulong 	# /usr/include/X11/Xlib.h:1293
XIMStringConversionLeftEdge = 1 	# /usr/include/X11/Xlib.h:1295
XIMStringConversionRightEdge = 2 	# /usr/include/X11/Xlib.h:1296
XIMStringConversionTopEdge = 4 	# /usr/include/X11/Xlib.h:1297
XIMStringConversionBottomEdge = 8 	# /usr/include/X11/Xlib.h:1298
XIMStringConversionConcealed = 16 	# /usr/include/X11/Xlib.h:1299
XIMStringConversionWrapped = 32 	# /usr/include/X11/Xlib.h:1300
class struct__XIMStringConversionText(Structure):
    __slots__ = [
        'length',
        'feedback',
        'encoding_is_wchar',
        'string',
    ]
class struct_anon_89(Union):
    __slots__ = [
        'mbs',
        'wcs',
    ]
struct_anon_89._fields_ = [
    ('mbs', c_char_p),
    ('wcs', c_wchar_p),
]

struct__XIMStringConversionText._fields_ = [
    ('length', c_ushort),
    ('feedback', POINTER(XIMStringConversionFeedback)),
    ('encoding_is_wchar', c_int),
    ('string', struct_anon_89),
]

XIMStringConversionText = struct__XIMStringConversionText 	# /usr/include/X11/Xlib.h:1310
XIMStringConversionPosition = c_ushort 	# /usr/include/X11/Xlib.h:1312
XIMStringConversionType = c_ushort 	# /usr/include/X11/Xlib.h:1314
XIMStringConversionBuffer = 1 	# /usr/include/X11/Xlib.h:1316
XIMStringConversionLine = 2 	# /usr/include/X11/Xlib.h:1317
XIMStringConversionWord = 3 	# /usr/include/X11/Xlib.h:1318
XIMStringConversionChar = 4 	# /usr/include/X11/Xlib.h:1319
XIMStringConversionOperation = c_ushort 	# /usr/include/X11/Xlib.h:1321
XIMStringConversionSubstitution = 1 	# /usr/include/X11/Xlib.h:1323
XIMStringConversionRetrieval = 2 	# /usr/include/X11/Xlib.h:1324
enum_anon_90 = c_int
XIMForwardChar = 0
XIMBackwardChar = 1
XIMForwardWord = 2
XIMBackwardWord = 3
XIMCaretUp = 4
XIMCaretDown = 5
XIMNextLine = 6
XIMPreviousLine = 7
XIMLineStart = 8
XIMLineEnd = 9
XIMAbsolutePosition = 10
XIMDontChange = 11
XIMCaretDirection = enum_anon_90 	# /usr/include/X11/Xlib.h:1334
class struct__XIMStringConversionCallbackStruct(Structure):
    __slots__ = [
        'position',
        'direction',
        'operation',
        'factor',
        'text',
    ]
struct__XIMStringConversionCallbackStruct._fields_ = [
    ('position', XIMStringConversionPosition),
    ('direction', XIMCaretDirection),
    ('operation', XIMStringConversionOperation),
    ('factor', c_ushort),
    ('text', POINTER(XIMStringConversionText)),
]

XIMStringConversionCallbackStruct = struct__XIMStringConversionCallbackStruct 	# /usr/include/X11/Xlib.h:1342
class struct__XIMPreeditDrawCallbackStruct(Structure):
    __slots__ = [
        'caret',
        'chg_first',
        'chg_length',
        'text',
    ]
struct__XIMPreeditDrawCallbackStruct._fields_ = [
    ('caret', c_int),
    ('chg_first', c_int),
    ('chg_length', c_int),
    ('text', POINTER(XIMText)),
]

XIMPreeditDrawCallbackStruct = struct__XIMPreeditDrawCallbackStruct 	# /usr/include/X11/Xlib.h:1349
enum_anon_91 = c_int
XIMIsInvisible = 0
XIMIsPrimary = 1
XIMIsSecondary = 2
XIMCaretStyle = enum_anon_91 	# /usr/include/X11/Xlib.h:1355
class struct__XIMPreeditCaretCallbackStruct(Structure):
    __slots__ = [
        'position',
        'direction',
        'style',
    ]
struct__XIMPreeditCaretCallbackStruct._fields_ = [
    ('position', c_int),
    ('direction', XIMCaretDirection),
    ('style', XIMCaretStyle),
]

XIMPreeditCaretCallbackStruct = struct__XIMPreeditCaretCallbackStruct 	# /usr/include/X11/Xlib.h:1361
enum_anon_92 = c_int
XIMTextType = 0
XIMBitmapType = 1
XIMStatusDataType = enum_anon_92 	# /usr/include/X11/Xlib.h:1366
class struct__XIMStatusDrawCallbackStruct(Structure):
    __slots__ = [
        'type',
        'data',
    ]
class struct_anon_93(Union):
    __slots__ = [
        'text',
        'bitmap',
    ]
struct_anon_93._fields_ = [
    ('text', POINTER(XIMText)),
    ('bitmap', Pixmap),
]

struct__XIMStatusDrawCallbackStruct._fields_ = [
    ('type', XIMStatusDataType),
    ('data', struct_anon_93),
]

XIMStatusDrawCallbackStruct = struct__XIMStatusDrawCallbackStruct 	# /usr/include/X11/Xlib.h:1374
class struct__XIMHotKeyTrigger(Structure):
    __slots__ = [
        'keysym',
        'modifier',
        'modifier_mask',
    ]
struct__XIMHotKeyTrigger._fields_ = [
    ('keysym', KeySym),
    ('modifier', c_int),
    ('modifier_mask', c_int),
]

XIMHotKeyTrigger = struct__XIMHotKeyTrigger 	# /usr/include/X11/Xlib.h:1380
class struct__XIMHotKeyTriggers(Structure):
    __slots__ = [
        'num_hot_key',
        'key',
    ]
struct__XIMHotKeyTriggers._fields_ = [
    ('num_hot_key', c_int),
    ('key', POINTER(XIMHotKeyTrigger)),
]

XIMHotKeyTriggers = struct__XIMHotKeyTriggers 	# /usr/include/X11/Xlib.h:1385
XIMHotKeyState = c_ulong 	# /usr/include/X11/Xlib.h:1387
XIMHotKeyStateON = 1 	# /usr/include/X11/Xlib.h:1389
XIMHotKeyStateOFF = 2 	# /usr/include/X11/Xlib.h:1390
class struct_anon_94(Structure):
    __slots__ = [
        'count_values',
        'supported_values',
    ]
struct_anon_94._fields_ = [
    ('count_values', c_ushort),
    ('supported_values', POINTER(c_char_p)),
]

XIMValuesList = struct_anon_94 	# /usr/include/X11/Xlib.h:1395
# /usr/include/X11/Xlib.h:1405
XLoadQueryFont = _lib.XLoadQueryFont
XLoadQueryFont.restype = POINTER(XFontStruct)
XLoadQueryFont.argtypes = [POINTER(Display), c_char_p]

# /usr/include/X11/Xlib.h:1410
XQueryFont = _lib.XQueryFont
XQueryFont.restype = POINTER(XFontStruct)
XQueryFont.argtypes = [POINTER(Display), XID]

# /usr/include/X11/Xlib.h:1416
XGetMotionEvents = _lib.XGetMotionEvents
XGetMotionEvents.restype = POINTER(XTimeCoord)
XGetMotionEvents.argtypes = [POINTER(Display), Window, Time, Time, POINTER(c_int)]

# /usr/include/X11/Xlib.h:1424
XDeleteModifiermapEntry = _lib.XDeleteModifiermapEntry
XDeleteModifiermapEntry.restype = POINTER(XModifierKeymap)
XDeleteModifiermapEntry.argtypes = [POINTER(XModifierKeymap), KeyCode, c_int]

# /usr/include/X11/Xlib.h:1434
XGetModifierMapping = _lib.XGetModifierMapping
XGetModifierMapping.restype = POINTER(XModifierKeymap)
XGetModifierMapping.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:1438
XInsertModifiermapEntry = _lib.XInsertModifiermapEntry
XInsertModifiermapEntry.restype = POINTER(XModifierKeymap)
XInsertModifiermapEntry.argtypes = [POINTER(XModifierKeymap), KeyCode, c_int]

# /usr/include/X11/Xlib.h:1448
XNewModifiermap = _lib.XNewModifiermap
XNewModifiermap.restype = POINTER(XModifierKeymap)
XNewModifiermap.argtypes = [c_int]

# /usr/include/X11/Xlib.h:1452
XCreateImage = _lib.XCreateImage
XCreateImage.restype = POINTER(XImage)
XCreateImage.argtypes = [POINTER(Display), POINTER(Visual), c_uint, c_int, c_int, c_char_p, c_uint, c_uint, c_int, c_int]

# /usr/include/X11/Xlib.h:1464
XInitImage = _lib.XInitImage
XInitImage.restype = c_int
XInitImage.argtypes = [POINTER(XImage)]

# /usr/include/X11/Xlib.h:1467
XGetImage = _lib.XGetImage
XGetImage.restype = POINTER(XImage)
XGetImage.argtypes = [POINTER(Display), Drawable, c_int, c_int, c_uint, c_uint, c_ulong, c_int]

# /usr/include/X11/Xlib.h:1477
XGetSubImage = _lib.XGetSubImage
XGetSubImage.restype = POINTER(XImage)
XGetSubImage.argtypes = [POINTER(Display), Drawable, c_int, c_int, c_uint, c_uint, c_ulong, c_int, POINTER(XImage), c_int, c_int]

# /usr/include/X11/Xlib.h:1494
XOpenDisplay = _lib.XOpenDisplay
XOpenDisplay.restype = POINTER(Display)
XOpenDisplay.argtypes = [c_char_p]

# /usr/include/X11/Xlib.h:1498
XrmInitialize = _lib.XrmInitialize
XrmInitialize.restype = None
XrmInitialize.argtypes = []

# /usr/include/X11/Xlib.h:1502
XFetchBytes = _lib.XFetchBytes
XFetchBytes.restype = c_char_p
XFetchBytes.argtypes = [POINTER(Display), POINTER(c_int)]

# /usr/include/X11/Xlib.h:1506
XFetchBuffer = _lib.XFetchBuffer
XFetchBuffer.restype = c_char_p
XFetchBuffer.argtypes = [POINTER(Display), POINTER(c_int), c_int]

# /usr/include/X11/Xlib.h:1511
XGetAtomName = _lib.XGetAtomName
XGetAtomName.restype = c_char_p
XGetAtomName.argtypes = [POINTER(Display), Atom]

# /usr/include/X11/Xlib.h:1515
XGetAtomNames = _lib.XGetAtomNames
XGetAtomNames.restype = c_int
XGetAtomNames.argtypes = [POINTER(Display), POINTER(Atom), c_int, POINTER(c_char_p)]

# /usr/include/X11/Xlib.h:1521
XGetDefault = _lib.XGetDefault
XGetDefault.restype = c_char_p
XGetDefault.argtypes = [POINTER(Display), c_char_p, c_char_p]

# /usr/include/X11/Xlib.h:1526
XDisplayName = _lib.XDisplayName
XDisplayName.restype = c_char_p
XDisplayName.argtypes = [c_char_p]

# /usr/include/X11/Xlib.h:1529
XKeysymToString = _lib.XKeysymToString
XKeysymToString.restype = c_char_p
XKeysymToString.argtypes = [KeySym]

# /usr/include/X11/Xlib.h:1533
XSynchronize = _lib.XSynchronize
XSynchronize.restype = POINTER(CFUNCTYPE(c_int, POINTER(Display)))
XSynchronize.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:1539
XSetAfterFunction = _lib.XSetAfterFunction
XSetAfterFunction.restype = POINTER(CFUNCTYPE(c_int, POINTER(Display)))
XSetAfterFunction.argtypes = [POINTER(Display), CFUNCTYPE(c_int, POINTER(Display))]

# /usr/include/X11/Xlib.h:1547
XInternAtom = _lib.XInternAtom
XInternAtom.restype = Atom
XInternAtom.argtypes = [POINTER(Display), c_char_p, c_int]

# /usr/include/X11/Xlib.h:1552
XInternAtoms = _lib.XInternAtoms
XInternAtoms.restype = c_int
XInternAtoms.argtypes = [POINTER(Display), POINTER(c_char_p), c_int, c_int, POINTER(Atom)]

# /usr/include/X11/Xlib.h:1559
XCopyColormapAndFree = _lib.XCopyColormapAndFree
XCopyColormapAndFree.restype = Colormap
XCopyColormapAndFree.argtypes = [POINTER(Display), Colormap]

# /usr/include/X11/Xlib.h:1563
XCreateColormap = _lib.XCreateColormap
XCreateColormap.restype = Colormap
XCreateColormap.argtypes = [POINTER(Display), Window, POINTER(Visual), c_int]

# /usr/include/X11/Xlib.h:1569
XCreatePixmapCursor = _lib.XCreatePixmapCursor
XCreatePixmapCursor.restype = Cursor
XCreatePixmapCursor.argtypes = [POINTER(Display), Pixmap, Pixmap, POINTER(XColor), POINTER(XColor), c_uint, c_uint]

# /usr/include/X11/Xlib.h:1578
XCreateGlyphCursor = _lib.XCreateGlyphCursor
XCreateGlyphCursor.restype = Cursor
XCreateGlyphCursor.argtypes = [POINTER(Display), Font, Font, c_uint, c_uint, POINTER(XColor), POINTER(XColor)]

# /usr/include/X11/Xlib.h:1587
XCreateFontCursor = _lib.XCreateFontCursor
XCreateFontCursor.restype = Cursor
XCreateFontCursor.argtypes = [POINTER(Display), c_uint]

# /usr/include/X11/Xlib.h:1591
XLoadFont = _lib.XLoadFont
XLoadFont.restype = Font
XLoadFont.argtypes = [POINTER(Display), c_char_p]

# /usr/include/X11/Xlib.h:1595
XCreateGC = _lib.XCreateGC
XCreateGC.restype = GC
XCreateGC.argtypes = [POINTER(Display), Drawable, c_ulong, POINTER(XGCValues)]

# /usr/include/X11/Xlib.h:1601
XGContextFromGC = _lib.XGContextFromGC
XGContextFromGC.restype = GContext
XGContextFromGC.argtypes = [GC]

# /usr/include/X11/Xlib.h:1604
XFlushGC = _lib.XFlushGC
XFlushGC.restype = None
XFlushGC.argtypes = [POINTER(Display), GC]

# /usr/include/X11/Xlib.h:1608
XCreatePixmap = _lib.XCreatePixmap
XCreatePixmap.restype = Pixmap
XCreatePixmap.argtypes = [POINTER(Display), Drawable, c_uint, c_uint, c_uint]

# /usr/include/X11/Xlib.h:1615
XCreateBitmapFromData = _lib.XCreateBitmapFromData
XCreateBitmapFromData.restype = Pixmap
XCreateBitmapFromData.argtypes = [POINTER(Display), Drawable, c_char_p, c_uint, c_uint]

# /usr/include/X11/Xlib.h:1622
XCreatePixmapFromBitmapData = _lib.XCreatePixmapFromBitmapData
XCreatePixmapFromBitmapData.restype = Pixmap
XCreatePixmapFromBitmapData.argtypes = [POINTER(Display), Drawable, c_char_p, c_uint, c_uint, c_ulong, c_ulong, c_uint]

# /usr/include/X11/Xlib.h:1632
XCreateSimpleWindow = _lib.XCreateSimpleWindow
XCreateSimpleWindow.restype = Window
XCreateSimpleWindow.argtypes = [POINTER(Display), Window, c_int, c_int, c_uint, c_uint, c_uint, c_ulong, c_ulong]

# /usr/include/X11/Xlib.h:1643
XGetSelectionOwner = _lib.XGetSelectionOwner
XGetSelectionOwner.restype = Window
XGetSelectionOwner.argtypes = [POINTER(Display), Atom]

# /usr/include/X11/Xlib.h:1647
XCreateWindow = _lib.XCreateWindow
XCreateWindow.restype = Window
XCreateWindow.argtypes = [POINTER(Display), Window, c_int, c_int, c_uint, c_uint, c_uint, c_int, c_uint, POINTER(Visual), c_ulong, POINTER(XSetWindowAttributes)]

# /usr/include/X11/Xlib.h:1661
XListInstalledColormaps = _lib.XListInstalledColormaps
XListInstalledColormaps.restype = POINTER(Colormap)
XListInstalledColormaps.argtypes = [POINTER(Display), Window, POINTER(c_int)]

# /usr/include/X11/Xlib.h:1666
XListFonts = _lib.XListFonts
XListFonts.restype = POINTER(c_char_p)
XListFonts.argtypes = [POINTER(Display), c_char_p, c_int, POINTER(c_int)]

# /usr/include/X11/Xlib.h:1672
XListFontsWithInfo = _lib.XListFontsWithInfo
XListFontsWithInfo.restype = POINTER(c_char_p)
XListFontsWithInfo.argtypes = [POINTER(Display), c_char_p, c_int, POINTER(c_int), POINTER(POINTER(XFontStruct))]

# /usr/include/X11/Xlib.h:1679
XGetFontPath = _lib.XGetFontPath
XGetFontPath.restype = POINTER(c_char_p)
XGetFontPath.argtypes = [POINTER(Display), POINTER(c_int)]

# /usr/include/X11/Xlib.h:1683
XListExtensions = _lib.XListExtensions
XListExtensions.restype = POINTER(c_char_p)
XListExtensions.argtypes = [POINTER(Display), POINTER(c_int)]

# /usr/include/X11/Xlib.h:1687
XListProperties = _lib.XListProperties
XListProperties.restype = POINTER(Atom)
XListProperties.argtypes = [POINTER(Display), Window, POINTER(c_int)]

# /usr/include/X11/Xlib.h:1692
XListHosts = _lib.XListHosts
XListHosts.restype = POINTER(XHostAddress)
XListHosts.argtypes = [POINTER(Display), POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/Xlib.h:1697
XKeycodeToKeysym = _lib.XKeycodeToKeysym
XKeycodeToKeysym.restype = KeySym
XKeycodeToKeysym.argtypes = [POINTER(Display), KeyCode, c_int]

# /usr/include/X11/Xlib.h:1706
XLookupKeysym = _lib.XLookupKeysym
XLookupKeysym.restype = KeySym
XLookupKeysym.argtypes = [POINTER(XKeyEvent), c_int]

# /usr/include/X11/Xlib.h:1710
XGetKeyboardMapping = _lib.XGetKeyboardMapping
XGetKeyboardMapping.restype = POINTER(KeySym)
XGetKeyboardMapping.argtypes = [POINTER(Display), KeyCode, c_int, POINTER(c_int)]

# /usr/include/X11/Xlib.h:1720
XStringToKeysym = _lib.XStringToKeysym
XStringToKeysym.restype = KeySym
XStringToKeysym.argtypes = [c_char_p]

# /usr/include/X11/Xlib.h:1723
XMaxRequestSize = _lib.XMaxRequestSize
XMaxRequestSize.restype = c_long
XMaxRequestSize.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:1726
XExtendedMaxRequestSize = _lib.XExtendedMaxRequestSize
XExtendedMaxRequestSize.restype = c_long
XExtendedMaxRequestSize.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:1729
XResourceManagerString = _lib.XResourceManagerString
XResourceManagerString.restype = c_char_p
XResourceManagerString.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:1732
XScreenResourceString = _lib.XScreenResourceString
XScreenResourceString.restype = c_char_p
XScreenResourceString.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:1735
XDisplayMotionBufferSize = _lib.XDisplayMotionBufferSize
XDisplayMotionBufferSize.restype = c_ulong
XDisplayMotionBufferSize.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:1738
XVisualIDFromVisual = _lib.XVisualIDFromVisual
XVisualIDFromVisual.restype = VisualID
XVisualIDFromVisual.argtypes = [POINTER(Visual)]

# /usr/include/X11/Xlib.h:1744
XInitThreads = _lib.XInitThreads
XInitThreads.restype = c_int
XInitThreads.argtypes = []

# /usr/include/X11/Xlib.h:1748
XLockDisplay = _lib.XLockDisplay
XLockDisplay.restype = None
XLockDisplay.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:1752
XUnlockDisplay = _lib.XUnlockDisplay
XUnlockDisplay.restype = None
XUnlockDisplay.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:1758
XInitExtension = _lib.XInitExtension
XInitExtension.restype = POINTER(XExtCodes)
XInitExtension.argtypes = [POINTER(Display), c_char_p]

# /usr/include/X11/Xlib.h:1763
XAddExtension = _lib.XAddExtension
XAddExtension.restype = POINTER(XExtCodes)
XAddExtension.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:1766
XFindOnExtensionList = _lib.XFindOnExtensionList
XFindOnExtensionList.restype = POINTER(XExtData)
XFindOnExtensionList.argtypes = [POINTER(POINTER(XExtData)), c_int]

# /usr/include/X11/Xlib.h:1770
XEHeadOfExtensionList = _lib.XEHeadOfExtensionList
XEHeadOfExtensionList.restype = POINTER(POINTER(XExtData))
XEHeadOfExtensionList.argtypes = [XEDataObject]

# /usr/include/X11/Xlib.h:1775
XRootWindow = _lib.XRootWindow
XRootWindow.restype = Window
XRootWindow.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:1779
XDefaultRootWindow = _lib.XDefaultRootWindow
XDefaultRootWindow.restype = Window
XDefaultRootWindow.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:1782
XRootWindowOfScreen = _lib.XRootWindowOfScreen
XRootWindowOfScreen.restype = Window
XRootWindowOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:1785
XDefaultVisual = _lib.XDefaultVisual
XDefaultVisual.restype = POINTER(Visual)
XDefaultVisual.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:1789
XDefaultVisualOfScreen = _lib.XDefaultVisualOfScreen
XDefaultVisualOfScreen.restype = POINTER(Visual)
XDefaultVisualOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:1792
XDefaultGC = _lib.XDefaultGC
XDefaultGC.restype = GC
XDefaultGC.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:1796
XDefaultGCOfScreen = _lib.XDefaultGCOfScreen
XDefaultGCOfScreen.restype = GC
XDefaultGCOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:1799
XBlackPixel = _lib.XBlackPixel
XBlackPixel.restype = c_ulong
XBlackPixel.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:1803
XWhitePixel = _lib.XWhitePixel
XWhitePixel.restype = c_ulong
XWhitePixel.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:1807
XAllPlanes = _lib.XAllPlanes
XAllPlanes.restype = c_ulong
XAllPlanes.argtypes = []

# /usr/include/X11/Xlib.h:1810
XBlackPixelOfScreen = _lib.XBlackPixelOfScreen
XBlackPixelOfScreen.restype = c_ulong
XBlackPixelOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:1813
XWhitePixelOfScreen = _lib.XWhitePixelOfScreen
XWhitePixelOfScreen.restype = c_ulong
XWhitePixelOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:1816
XNextRequest = _lib.XNextRequest
XNextRequest.restype = c_ulong
XNextRequest.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:1819
XLastKnownRequestProcessed = _lib.XLastKnownRequestProcessed
XLastKnownRequestProcessed.restype = c_ulong
XLastKnownRequestProcessed.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:1822
XServerVendor = _lib.XServerVendor
XServerVendor.restype = c_char_p
XServerVendor.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:1825
XDisplayString = _lib.XDisplayString
XDisplayString.restype = c_char_p
XDisplayString.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:1828
XDefaultColormap = _lib.XDefaultColormap
XDefaultColormap.restype = Colormap
XDefaultColormap.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:1832
XDefaultColormapOfScreen = _lib.XDefaultColormapOfScreen
XDefaultColormapOfScreen.restype = Colormap
XDefaultColormapOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:1835
XDisplayOfScreen = _lib.XDisplayOfScreen
XDisplayOfScreen.restype = POINTER(Display)
XDisplayOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:1838
XScreenOfDisplay = _lib.XScreenOfDisplay
XScreenOfDisplay.restype = POINTER(Screen)
XScreenOfDisplay.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:1842
XDefaultScreenOfDisplay = _lib.XDefaultScreenOfDisplay
XDefaultScreenOfDisplay.restype = POINTER(Screen)
XDefaultScreenOfDisplay.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:1845
XEventMaskOfScreen = _lib.XEventMaskOfScreen
XEventMaskOfScreen.restype = c_long
XEventMaskOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:1849
XScreenNumberOfScreen = _lib.XScreenNumberOfScreen
XScreenNumberOfScreen.restype = c_int
XScreenNumberOfScreen.argtypes = [POINTER(Screen)]

XErrorHandler = CFUNCTYPE(c_int, POINTER(Display), POINTER(XErrorEvent)) 	# /usr/include/X11/Xlib.h:1853
# /usr/include/X11/Xlib.h:1858
XSetErrorHandler = _lib.XSetErrorHandler
XSetErrorHandler.restype = XErrorHandler
XSetErrorHandler.argtypes = [XErrorHandler]

XIOErrorHandler = CFUNCTYPE(c_int, POINTER(Display)) 	# /usr/include/X11/Xlib.h:1863
# /usr/include/X11/Xlib.h:1867
XSetIOErrorHandler = _lib.XSetIOErrorHandler
XSetIOErrorHandler.restype = XIOErrorHandler
XSetIOErrorHandler.argtypes = [XIOErrorHandler]

# /usr/include/X11/Xlib.h:1872
XListPixmapFormats = _lib.XListPixmapFormats
XListPixmapFormats.restype = POINTER(XPixmapFormatValues)
XListPixmapFormats.argtypes = [POINTER(Display), POINTER(c_int)]

# /usr/include/X11/Xlib.h:1876
XListDepths = _lib.XListDepths
XListDepths.restype = POINTER(c_int)
XListDepths.argtypes = [POINTER(Display), c_int, POINTER(c_int)]

# /usr/include/X11/Xlib.h:1884
XReconfigureWMWindow = _lib.XReconfigureWMWindow
XReconfigureWMWindow.restype = c_int
XReconfigureWMWindow.argtypes = [POINTER(Display), Window, c_int, c_uint, POINTER(XWindowChanges)]

# /usr/include/X11/Xlib.h:1892
XGetWMProtocols = _lib.XGetWMProtocols
XGetWMProtocols.restype = c_int
XGetWMProtocols.argtypes = [POINTER(Display), Window, POINTER(POINTER(Atom)), POINTER(c_int)]

# /usr/include/X11/Xlib.h:1898
XSetWMProtocols = _lib.XSetWMProtocols
XSetWMProtocols.restype = c_int
XSetWMProtocols.argtypes = [POINTER(Display), Window, POINTER(Atom), c_int]

# /usr/include/X11/Xlib.h:1904
XIconifyWindow = _lib.XIconifyWindow
XIconifyWindow.restype = c_int
XIconifyWindow.argtypes = [POINTER(Display), Window, c_int]

# /usr/include/X11/Xlib.h:1909
XWithdrawWindow = _lib.XWithdrawWindow
XWithdrawWindow.restype = c_int
XWithdrawWindow.argtypes = [POINTER(Display), Window, c_int]

# /usr/include/X11/Xlib.h:1914
XGetCommand = _lib.XGetCommand
XGetCommand.restype = c_int
XGetCommand.argtypes = [POINTER(Display), Window, POINTER(POINTER(c_char_p)), POINTER(c_int)]

# /usr/include/X11/Xlib.h:1920
XGetWMColormapWindows = _lib.XGetWMColormapWindows
XGetWMColormapWindows.restype = c_int
XGetWMColormapWindows.argtypes = [POINTER(Display), Window, POINTER(POINTER(Window)), POINTER(c_int)]

# /usr/include/X11/Xlib.h:1926
XSetWMColormapWindows = _lib.XSetWMColormapWindows
XSetWMColormapWindows.restype = c_int
XSetWMColormapWindows.argtypes = [POINTER(Display), Window, POINTER(Window), c_int]

# /usr/include/X11/Xlib.h:1932
XFreeStringList = _lib.XFreeStringList
XFreeStringList.restype = None
XFreeStringList.argtypes = [POINTER(c_char_p)]

# /usr/include/X11/Xlib.h:1935
XSetTransientForHint = _lib.XSetTransientForHint
XSetTransientForHint.restype = c_int
XSetTransientForHint.argtypes = [POINTER(Display), Window, Window]

# /usr/include/X11/Xlib.h:1943
XActivateScreenSaver = _lib.XActivateScreenSaver
XActivateScreenSaver.restype = c_int
XActivateScreenSaver.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:1947
XAddHost = _lib.XAddHost
XAddHost.restype = c_int
XAddHost.argtypes = [POINTER(Display), POINTER(XHostAddress)]

# /usr/include/X11/Xlib.h:1952
XAddHosts = _lib.XAddHosts
XAddHosts.restype = c_int
XAddHosts.argtypes = [POINTER(Display), POINTER(XHostAddress), c_int]

# /usr/include/X11/Xlib.h:1958
XAddToExtensionList = _lib.XAddToExtensionList
XAddToExtensionList.restype = c_int
XAddToExtensionList.argtypes = [POINTER(POINTER(struct__XExtData)), POINTER(XExtData)]

# /usr/include/X11/Xlib.h:1963
XAddToSaveSet = _lib.XAddToSaveSet
XAddToSaveSet.restype = c_int
XAddToSaveSet.argtypes = [POINTER(Display), Window]

# /usr/include/X11/Xlib.h:1968
XAllocColor = _lib.XAllocColor
XAllocColor.restype = c_int
XAllocColor.argtypes = [POINTER(Display), Colormap, POINTER(XColor)]

# /usr/include/X11/Xlib.h:1974
XAllocColorCells = _lib.XAllocColorCells
XAllocColorCells.restype = c_int
XAllocColorCells.argtypes = [POINTER(Display), Colormap, c_int, POINTER(c_ulong), c_uint, POINTER(c_ulong), c_uint]

# /usr/include/X11/Xlib.h:1984
XAllocColorPlanes = _lib.XAllocColorPlanes
XAllocColorPlanes.restype = c_int
XAllocColorPlanes.argtypes = [POINTER(Display), Colormap, c_int, POINTER(c_ulong), c_int, c_int, c_int, c_int, POINTER(c_ulong), POINTER(c_ulong), POINTER(c_ulong)]

# /usr/include/X11/Xlib.h:1998
XAllocNamedColor = _lib.XAllocNamedColor
XAllocNamedColor.restype = c_int
XAllocNamedColor.argtypes = [POINTER(Display), Colormap, c_char_p, POINTER(XColor), POINTER(XColor)]

# /usr/include/X11/Xlib.h:2006
XAllowEvents = _lib.XAllowEvents
XAllowEvents.restype = c_int
XAllowEvents.argtypes = [POINTER(Display), c_int, Time]

# /usr/include/X11/Xlib.h:2012
XAutoRepeatOff = _lib.XAutoRepeatOff
XAutoRepeatOff.restype = c_int
XAutoRepeatOff.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2016
XAutoRepeatOn = _lib.XAutoRepeatOn
XAutoRepeatOn.restype = c_int
XAutoRepeatOn.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2020
XBell = _lib.XBell
XBell.restype = c_int
XBell.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:2025
XBitmapBitOrder = _lib.XBitmapBitOrder
XBitmapBitOrder.restype = c_int
XBitmapBitOrder.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2029
XBitmapPad = _lib.XBitmapPad
XBitmapPad.restype = c_int
XBitmapPad.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2033
XBitmapUnit = _lib.XBitmapUnit
XBitmapUnit.restype = c_int
XBitmapUnit.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2037
XCellsOfScreen = _lib.XCellsOfScreen
XCellsOfScreen.restype = c_int
XCellsOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:2041
XChangeActivePointerGrab = _lib.XChangeActivePointerGrab
XChangeActivePointerGrab.restype = c_int
XChangeActivePointerGrab.argtypes = [POINTER(Display), c_uint, Cursor, Time]

# /usr/include/X11/Xlib.h:2048
XChangeGC = _lib.XChangeGC
XChangeGC.restype = c_int
XChangeGC.argtypes = [POINTER(Display), GC, c_ulong, POINTER(XGCValues)]

# /usr/include/X11/Xlib.h:2055
XChangeKeyboardControl = _lib.XChangeKeyboardControl
XChangeKeyboardControl.restype = c_int
XChangeKeyboardControl.argtypes = [POINTER(Display), c_ulong, POINTER(XKeyboardControl)]

# /usr/include/X11/Xlib.h:2061
XChangeKeyboardMapping = _lib.XChangeKeyboardMapping
XChangeKeyboardMapping.restype = c_int
XChangeKeyboardMapping.argtypes = [POINTER(Display), c_int, c_int, POINTER(KeySym), c_int]

# /usr/include/X11/Xlib.h:2069
XChangePointerControl = _lib.XChangePointerControl
XChangePointerControl.restype = c_int
XChangePointerControl.argtypes = [POINTER(Display), c_int, c_int, c_int, c_int, c_int]

# /usr/include/X11/Xlib.h:2078
XChangeProperty = _lib.XChangeProperty
XChangeProperty.restype = c_int
XChangeProperty.argtypes = [POINTER(Display), Window, Atom, Atom, c_int, c_int, POINTER(c_ubyte), c_int]

# /usr/include/X11/Xlib.h:2089
XChangeSaveSet = _lib.XChangeSaveSet
XChangeSaveSet.restype = c_int
XChangeSaveSet.argtypes = [POINTER(Display), Window, c_int]

# /usr/include/X11/Xlib.h:2095
XChangeWindowAttributes = _lib.XChangeWindowAttributes
XChangeWindowAttributes.restype = c_int
XChangeWindowAttributes.argtypes = [POINTER(Display), Window, c_ulong, POINTER(XSetWindowAttributes)]

# /usr/include/X11/Xlib.h:2102
XCheckIfEvent = _lib.XCheckIfEvent
XCheckIfEvent.restype = c_int
XCheckIfEvent.argtypes = [POINTER(Display), POINTER(XEvent), CFUNCTYPE(c_int, POINTER(Display), POINTER(XEvent), XPointer), XPointer]

# /usr/include/X11/Xlib.h:2113
XCheckMaskEvent = _lib.XCheckMaskEvent
XCheckMaskEvent.restype = c_int
XCheckMaskEvent.argtypes = [POINTER(Display), c_long, POINTER(XEvent)]

# /usr/include/X11/Xlib.h:2119
XCheckTypedEvent = _lib.XCheckTypedEvent
XCheckTypedEvent.restype = c_int
XCheckTypedEvent.argtypes = [POINTER(Display), c_int, POINTER(XEvent)]

# /usr/include/X11/Xlib.h:2125
XCheckTypedWindowEvent = _lib.XCheckTypedWindowEvent
XCheckTypedWindowEvent.restype = c_int
XCheckTypedWindowEvent.argtypes = [POINTER(Display), Window, c_int, POINTER(XEvent)]

# /usr/include/X11/Xlib.h:2132
XCheckWindowEvent = _lib.XCheckWindowEvent
XCheckWindowEvent.restype = c_int
XCheckWindowEvent.argtypes = [POINTER(Display), Window, c_long, POINTER(XEvent)]

# /usr/include/X11/Xlib.h:2139
XCirculateSubwindows = _lib.XCirculateSubwindows
XCirculateSubwindows.restype = c_int
XCirculateSubwindows.argtypes = [POINTER(Display), Window, c_int]

# /usr/include/X11/Xlib.h:2145
XCirculateSubwindowsDown = _lib.XCirculateSubwindowsDown
XCirculateSubwindowsDown.restype = c_int
XCirculateSubwindowsDown.argtypes = [POINTER(Display), Window]

# /usr/include/X11/Xlib.h:2150
XCirculateSubwindowsUp = _lib.XCirculateSubwindowsUp
XCirculateSubwindowsUp.restype = c_int
XCirculateSubwindowsUp.argtypes = [POINTER(Display), Window]

# /usr/include/X11/Xlib.h:2155
XClearArea = _lib.XClearArea
XClearArea.restype = c_int
XClearArea.argtypes = [POINTER(Display), Window, c_int, c_int, c_uint, c_uint, c_int]

# /usr/include/X11/Xlib.h:2165
XClearWindow = _lib.XClearWindow
XClearWindow.restype = c_int
XClearWindow.argtypes = [POINTER(Display), Window]

# /usr/include/X11/Xlib.h:2170
XCloseDisplay = _lib.XCloseDisplay
XCloseDisplay.restype = c_int
XCloseDisplay.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2174
XConfigureWindow = _lib.XConfigureWindow
XConfigureWindow.restype = c_int
XConfigureWindow.argtypes = [POINTER(Display), Window, c_uint, POINTER(XWindowChanges)]

# /usr/include/X11/Xlib.h:2181
XConnectionNumber = _lib.XConnectionNumber
XConnectionNumber.restype = c_int
XConnectionNumber.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2185
XConvertSelection = _lib.XConvertSelection
XConvertSelection.restype = c_int
XConvertSelection.argtypes = [POINTER(Display), Atom, Atom, Atom, Window, Time]

# /usr/include/X11/Xlib.h:2194
XCopyArea = _lib.XCopyArea
XCopyArea.restype = c_int
XCopyArea.argtypes = [POINTER(Display), Drawable, Drawable, GC, c_int, c_int, c_uint, c_uint, c_int, c_int]

# /usr/include/X11/Xlib.h:2207
XCopyGC = _lib.XCopyGC
XCopyGC.restype = c_int
XCopyGC.argtypes = [POINTER(Display), GC, c_ulong, GC]

# /usr/include/X11/Xlib.h:2214
XCopyPlane = _lib.XCopyPlane
XCopyPlane.restype = c_int
XCopyPlane.argtypes = [POINTER(Display), Drawable, Drawable, GC, c_int, c_int, c_uint, c_uint, c_int, c_int, c_ulong]

# /usr/include/X11/Xlib.h:2228
XDefaultDepth = _lib.XDefaultDepth
XDefaultDepth.restype = c_int
XDefaultDepth.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:2233
XDefaultDepthOfScreen = _lib.XDefaultDepthOfScreen
XDefaultDepthOfScreen.restype = c_int
XDefaultDepthOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:2237
XDefaultScreen = _lib.XDefaultScreen
XDefaultScreen.restype = c_int
XDefaultScreen.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2241
XDefineCursor = _lib.XDefineCursor
XDefineCursor.restype = c_int
XDefineCursor.argtypes = [POINTER(Display), Window, Cursor]

# /usr/include/X11/Xlib.h:2247
XDeleteProperty = _lib.XDeleteProperty
XDeleteProperty.restype = c_int
XDeleteProperty.argtypes = [POINTER(Display), Window, Atom]

# /usr/include/X11/Xlib.h:2253
XDestroyWindow = _lib.XDestroyWindow
XDestroyWindow.restype = c_int
XDestroyWindow.argtypes = [POINTER(Display), Window]

# /usr/include/X11/Xlib.h:2258
XDestroySubwindows = _lib.XDestroySubwindows
XDestroySubwindows.restype = c_int
XDestroySubwindows.argtypes = [POINTER(Display), Window]

# /usr/include/X11/Xlib.h:2263
XDoesBackingStore = _lib.XDoesBackingStore
XDoesBackingStore.restype = c_int
XDoesBackingStore.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:2267
XDoesSaveUnders = _lib.XDoesSaveUnders
XDoesSaveUnders.restype = c_int
XDoesSaveUnders.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:2271
XDisableAccessControl = _lib.XDisableAccessControl
XDisableAccessControl.restype = c_int
XDisableAccessControl.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2276
XDisplayCells = _lib.XDisplayCells
XDisplayCells.restype = c_int
XDisplayCells.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:2281
XDisplayHeight = _lib.XDisplayHeight
XDisplayHeight.restype = c_int
XDisplayHeight.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:2286
XDisplayHeightMM = _lib.XDisplayHeightMM
XDisplayHeightMM.restype = c_int
XDisplayHeightMM.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:2291
XDisplayKeycodes = _lib.XDisplayKeycodes
XDisplayKeycodes.restype = c_int
XDisplayKeycodes.argtypes = [POINTER(Display), POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/Xlib.h:2297
XDisplayPlanes = _lib.XDisplayPlanes
XDisplayPlanes.restype = c_int
XDisplayPlanes.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:2302
XDisplayWidth = _lib.XDisplayWidth
XDisplayWidth.restype = c_int
XDisplayWidth.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:2307
XDisplayWidthMM = _lib.XDisplayWidthMM
XDisplayWidthMM.restype = c_int
XDisplayWidthMM.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:2312
XDrawArc = _lib.XDrawArc
XDrawArc.restype = c_int
XDrawArc.argtypes = [POINTER(Display), Drawable, GC, c_int, c_int, c_uint, c_uint, c_int, c_int]

# /usr/include/X11/Xlib.h:2324
XDrawArcs = _lib.XDrawArcs
XDrawArcs.restype = c_int
XDrawArcs.argtypes = [POINTER(Display), Drawable, GC, POINTER(XArc), c_int]

# /usr/include/X11/Xlib.h:2332
XDrawImageString = _lib.XDrawImageString
XDrawImageString.restype = c_int
XDrawImageString.argtypes = [POINTER(Display), Drawable, GC, c_int, c_int, c_char_p, c_int]

# /usr/include/X11/Xlib.h:2342
XDrawImageString16 = _lib.XDrawImageString16
XDrawImageString16.restype = c_int
XDrawImageString16.argtypes = [POINTER(Display), Drawable, GC, c_int, c_int, POINTER(XChar2b), c_int]

# /usr/include/X11/Xlib.h:2352
XDrawLine = _lib.XDrawLine
XDrawLine.restype = c_int
XDrawLine.argtypes = [POINTER(Display), Drawable, GC, c_int, c_int, c_int, c_int]

# /usr/include/X11/Xlib.h:2362
XDrawLines = _lib.XDrawLines
XDrawLines.restype = c_int
XDrawLines.argtypes = [POINTER(Display), Drawable, GC, POINTER(XPoint), c_int, c_int]

# /usr/include/X11/Xlib.h:2371
XDrawPoint = _lib.XDrawPoint
XDrawPoint.restype = c_int
XDrawPoint.argtypes = [POINTER(Display), Drawable, GC, c_int, c_int]

# /usr/include/X11/Xlib.h:2379
XDrawPoints = _lib.XDrawPoints
XDrawPoints.restype = c_int
XDrawPoints.argtypes = [POINTER(Display), Drawable, GC, POINTER(XPoint), c_int, c_int]

# /usr/include/X11/Xlib.h:2388
XDrawRectangle = _lib.XDrawRectangle
XDrawRectangle.restype = c_int
XDrawRectangle.argtypes = [POINTER(Display), Drawable, GC, c_int, c_int, c_uint, c_uint]

# /usr/include/X11/Xlib.h:2398
XDrawRectangles = _lib.XDrawRectangles
XDrawRectangles.restype = c_int
XDrawRectangles.argtypes = [POINTER(Display), Drawable, GC, POINTER(XRectangle), c_int]

# /usr/include/X11/Xlib.h:2406
XDrawSegments = _lib.XDrawSegments
XDrawSegments.restype = c_int
XDrawSegments.argtypes = [POINTER(Display), Drawable, GC, POINTER(XSegment), c_int]

# /usr/include/X11/Xlib.h:2414
XDrawString = _lib.XDrawString
XDrawString.restype = c_int
XDrawString.argtypes = [POINTER(Display), Drawable, GC, c_int, c_int, c_char_p, c_int]

# /usr/include/X11/Xlib.h:2424
XDrawString16 = _lib.XDrawString16
XDrawString16.restype = c_int
XDrawString16.argtypes = [POINTER(Display), Drawable, GC, c_int, c_int, POINTER(XChar2b), c_int]

# /usr/include/X11/Xlib.h:2434
XDrawText = _lib.XDrawText
XDrawText.restype = c_int
XDrawText.argtypes = [POINTER(Display), Drawable, GC, c_int, c_int, POINTER(XTextItem), c_int]

# /usr/include/X11/Xlib.h:2444
XDrawText16 = _lib.XDrawText16
XDrawText16.restype = c_int
XDrawText16.argtypes = [POINTER(Display), Drawable, GC, c_int, c_int, POINTER(XTextItem16), c_int]

# /usr/include/X11/Xlib.h:2454
XEnableAccessControl = _lib.XEnableAccessControl
XEnableAccessControl.restype = c_int
XEnableAccessControl.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2458
XEventsQueued = _lib.XEventsQueued
XEventsQueued.restype = c_int
XEventsQueued.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:2463
XFetchName = _lib.XFetchName
XFetchName.restype = c_int
XFetchName.argtypes = [POINTER(Display), Window, POINTER(c_char_p)]

# /usr/include/X11/Xlib.h:2469
XFillArc = _lib.XFillArc
XFillArc.restype = c_int
XFillArc.argtypes = [POINTER(Display), Drawable, GC, c_int, c_int, c_uint, c_uint, c_int, c_int]

# /usr/include/X11/Xlib.h:2481
XFillArcs = _lib.XFillArcs
XFillArcs.restype = c_int
XFillArcs.argtypes = [POINTER(Display), Drawable, GC, POINTER(XArc), c_int]

# /usr/include/X11/Xlib.h:2489
XFillPolygon = _lib.XFillPolygon
XFillPolygon.restype = c_int
XFillPolygon.argtypes = [POINTER(Display), Drawable, GC, POINTER(XPoint), c_int, c_int, c_int]

# /usr/include/X11/Xlib.h:2499
XFillRectangle = _lib.XFillRectangle
XFillRectangle.restype = c_int
XFillRectangle.argtypes = [POINTER(Display), Drawable, GC, c_int, c_int, c_uint, c_uint]

# /usr/include/X11/Xlib.h:2509
XFillRectangles = _lib.XFillRectangles
XFillRectangles.restype = c_int
XFillRectangles.argtypes = [POINTER(Display), Drawable, GC, POINTER(XRectangle), c_int]

# /usr/include/X11/Xlib.h:2517
XFlush = _lib.XFlush
XFlush.restype = c_int
XFlush.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2521
XForceScreenSaver = _lib.XForceScreenSaver
XForceScreenSaver.restype = c_int
XForceScreenSaver.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:2526
XFree = _lib.XFree
XFree.restype = c_int
XFree.argtypes = [POINTER(None)]

# /usr/include/X11/Xlib.h:2530
XFreeColormap = _lib.XFreeColormap
XFreeColormap.restype = c_int
XFreeColormap.argtypes = [POINTER(Display), Colormap]

# /usr/include/X11/Xlib.h:2535
XFreeColors = _lib.XFreeColors
XFreeColors.restype = c_int
XFreeColors.argtypes = [POINTER(Display), Colormap, POINTER(c_ulong), c_int, c_ulong]

# /usr/include/X11/Xlib.h:2543
XFreeCursor = _lib.XFreeCursor
XFreeCursor.restype = c_int
XFreeCursor.argtypes = [POINTER(Display), Cursor]

# /usr/include/X11/Xlib.h:2548
XFreeExtensionList = _lib.XFreeExtensionList
XFreeExtensionList.restype = c_int
XFreeExtensionList.argtypes = [POINTER(c_char_p)]

# /usr/include/X11/Xlib.h:2552
XFreeFont = _lib.XFreeFont
XFreeFont.restype = c_int
XFreeFont.argtypes = [POINTER(Display), POINTER(XFontStruct)]

# /usr/include/X11/Xlib.h:2557
XFreeFontInfo = _lib.XFreeFontInfo
XFreeFontInfo.restype = c_int
XFreeFontInfo.argtypes = [POINTER(c_char_p), POINTER(XFontStruct), c_int]

# /usr/include/X11/Xlib.h:2563
XFreeFontNames = _lib.XFreeFontNames
XFreeFontNames.restype = c_int
XFreeFontNames.argtypes = [POINTER(c_char_p)]

# /usr/include/X11/Xlib.h:2567
XFreeFontPath = _lib.XFreeFontPath
XFreeFontPath.restype = c_int
XFreeFontPath.argtypes = [POINTER(c_char_p)]

# /usr/include/X11/Xlib.h:2571
XFreeGC = _lib.XFreeGC
XFreeGC.restype = c_int
XFreeGC.argtypes = [POINTER(Display), GC]

# /usr/include/X11/Xlib.h:2576
XFreeModifiermap = _lib.XFreeModifiermap
XFreeModifiermap.restype = c_int
XFreeModifiermap.argtypes = [POINTER(XModifierKeymap)]

# /usr/include/X11/Xlib.h:2580
XFreePixmap = _lib.XFreePixmap
XFreePixmap.restype = c_int
XFreePixmap.argtypes = [POINTER(Display), Pixmap]

# /usr/include/X11/Xlib.h:2585
XGeometry = _lib.XGeometry
XGeometry.restype = c_int
XGeometry.argtypes = [POINTER(Display), c_int, c_char_p, c_char_p, c_uint, c_uint, c_uint, c_int, c_int, POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/Xlib.h:2601
XGetErrorDatabaseText = _lib.XGetErrorDatabaseText
XGetErrorDatabaseText.restype = c_int
XGetErrorDatabaseText.argtypes = [POINTER(Display), c_char_p, c_char_p, c_char_p, c_char_p, c_int]

# /usr/include/X11/Xlib.h:2610
XGetErrorText = _lib.XGetErrorText
XGetErrorText.restype = c_int
XGetErrorText.argtypes = [POINTER(Display), c_int, c_char_p, c_int]

# /usr/include/X11/Xlib.h:2617
XGetFontProperty = _lib.XGetFontProperty
XGetFontProperty.restype = c_int
XGetFontProperty.argtypes = [POINTER(XFontStruct), Atom, POINTER(c_ulong)]

# /usr/include/X11/Xlib.h:2623
XGetGCValues = _lib.XGetGCValues
XGetGCValues.restype = c_int
XGetGCValues.argtypes = [POINTER(Display), GC, c_ulong, POINTER(XGCValues)]

# /usr/include/X11/Xlib.h:2630
XGetGeometry = _lib.XGetGeometry
XGetGeometry.restype = c_int
XGetGeometry.argtypes = [POINTER(Display), Drawable, POINTER(Window), POINTER(c_int), POINTER(c_int), POINTER(c_uint), POINTER(c_uint), POINTER(c_uint), POINTER(c_uint)]

# /usr/include/X11/Xlib.h:2642
XGetIconName = _lib.XGetIconName
XGetIconName.restype = c_int
XGetIconName.argtypes = [POINTER(Display), Window, POINTER(c_char_p)]

# /usr/include/X11/Xlib.h:2648
XGetInputFocus = _lib.XGetInputFocus
XGetInputFocus.restype = c_int
XGetInputFocus.argtypes = [POINTER(Display), POINTER(Window), POINTER(c_int)]

# /usr/include/X11/Xlib.h:2654
XGetKeyboardControl = _lib.XGetKeyboardControl
XGetKeyboardControl.restype = c_int
XGetKeyboardControl.argtypes = [POINTER(Display), POINTER(XKeyboardState)]

# /usr/include/X11/Xlib.h:2659
XGetPointerControl = _lib.XGetPointerControl
XGetPointerControl.restype = c_int
XGetPointerControl.argtypes = [POINTER(Display), POINTER(c_int), POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/Xlib.h:2666
XGetPointerMapping = _lib.XGetPointerMapping
XGetPointerMapping.restype = c_int
XGetPointerMapping.argtypes = [POINTER(Display), POINTER(c_ubyte), c_int]

# /usr/include/X11/Xlib.h:2672
XGetScreenSaver = _lib.XGetScreenSaver
XGetScreenSaver.restype = c_int
XGetScreenSaver.argtypes = [POINTER(Display), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/Xlib.h:2680
XGetTransientForHint = _lib.XGetTransientForHint
XGetTransientForHint.restype = c_int
XGetTransientForHint.argtypes = [POINTER(Display), Window, POINTER(Window)]

# /usr/include/X11/Xlib.h:2686
XGetWindowProperty = _lib.XGetWindowProperty
XGetWindowProperty.restype = c_int
XGetWindowProperty.argtypes = [POINTER(Display), Window, Atom, c_long, c_long, c_int, Atom, POINTER(Atom), POINTER(c_int), POINTER(c_ulong), POINTER(c_ulong), POINTER(POINTER(c_ubyte))]

# /usr/include/X11/Xlib.h:2701
XGetWindowAttributes = _lib.XGetWindowAttributes
XGetWindowAttributes.restype = c_int
XGetWindowAttributes.argtypes = [POINTER(Display), Window, POINTER(XWindowAttributes)]

# /usr/include/X11/Xlib.h:2707
XGrabButton = _lib.XGrabButton
XGrabButton.restype = c_int
XGrabButton.argtypes = [POINTER(Display), c_uint, c_uint, Window, c_int, c_uint, c_int, c_int, Window, Cursor]

# /usr/include/X11/Xlib.h:2720
XGrabKey = _lib.XGrabKey
XGrabKey.restype = c_int
XGrabKey.argtypes = [POINTER(Display), c_int, c_uint, Window, c_int, c_int, c_int]

# /usr/include/X11/Xlib.h:2730
XGrabKeyboard = _lib.XGrabKeyboard
XGrabKeyboard.restype = c_int
XGrabKeyboard.argtypes = [POINTER(Display), Window, c_int, c_int, c_int, Time]

# /usr/include/X11/Xlib.h:2739
XGrabPointer = _lib.XGrabPointer
XGrabPointer.restype = c_int
XGrabPointer.argtypes = [POINTER(Display), Window, c_int, c_uint, c_int, c_int, Window, Cursor, Time]

# /usr/include/X11/Xlib.h:2751
XGrabServer = _lib.XGrabServer
XGrabServer.restype = c_int
XGrabServer.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2755
XHeightMMOfScreen = _lib.XHeightMMOfScreen
XHeightMMOfScreen.restype = c_int
XHeightMMOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:2759
XHeightOfScreen = _lib.XHeightOfScreen
XHeightOfScreen.restype = c_int
XHeightOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:2763
XIfEvent = _lib.XIfEvent
XIfEvent.restype = c_int
XIfEvent.argtypes = [POINTER(Display), POINTER(XEvent), CFUNCTYPE(c_int, POINTER(Display), POINTER(XEvent), XPointer), XPointer]

# /usr/include/X11/Xlib.h:2774
XImageByteOrder = _lib.XImageByteOrder
XImageByteOrder.restype = c_int
XImageByteOrder.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2778
XInstallColormap = _lib.XInstallColormap
XInstallColormap.restype = c_int
XInstallColormap.argtypes = [POINTER(Display), Colormap]

# /usr/include/X11/Xlib.h:2783
XKeysymToKeycode = _lib.XKeysymToKeycode
XKeysymToKeycode.restype = KeyCode
XKeysymToKeycode.argtypes = [POINTER(Display), KeySym]

# /usr/include/X11/Xlib.h:2788
XKillClient = _lib.XKillClient
XKillClient.restype = c_int
XKillClient.argtypes = [POINTER(Display), XID]

# /usr/include/X11/Xlib.h:2793
XLookupColor = _lib.XLookupColor
XLookupColor.restype = c_int
XLookupColor.argtypes = [POINTER(Display), Colormap, c_char_p, POINTER(XColor), POINTER(XColor)]

# /usr/include/X11/Xlib.h:2801
XLowerWindow = _lib.XLowerWindow
XLowerWindow.restype = c_int
XLowerWindow.argtypes = [POINTER(Display), Window]

# /usr/include/X11/Xlib.h:2806
XMapRaised = _lib.XMapRaised
XMapRaised.restype = c_int
XMapRaised.argtypes = [POINTER(Display), Window]

# /usr/include/X11/Xlib.h:2811
XMapSubwindows = _lib.XMapSubwindows
XMapSubwindows.restype = c_int
XMapSubwindows.argtypes = [POINTER(Display), Window]

# /usr/include/X11/Xlib.h:2816
XMapWindow = _lib.XMapWindow
XMapWindow.restype = c_int
XMapWindow.argtypes = [POINTER(Display), Window]

# /usr/include/X11/Xlib.h:2821
XMaskEvent = _lib.XMaskEvent
XMaskEvent.restype = c_int
XMaskEvent.argtypes = [POINTER(Display), c_long, POINTER(XEvent)]

# /usr/include/X11/Xlib.h:2827
XMaxCmapsOfScreen = _lib.XMaxCmapsOfScreen
XMaxCmapsOfScreen.restype = c_int
XMaxCmapsOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:2831
XMinCmapsOfScreen = _lib.XMinCmapsOfScreen
XMinCmapsOfScreen.restype = c_int
XMinCmapsOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:2835
XMoveResizeWindow = _lib.XMoveResizeWindow
XMoveResizeWindow.restype = c_int
XMoveResizeWindow.argtypes = [POINTER(Display), Window, c_int, c_int, c_uint, c_uint]

# /usr/include/X11/Xlib.h:2844
XMoveWindow = _lib.XMoveWindow
XMoveWindow.restype = c_int
XMoveWindow.argtypes = [POINTER(Display), Window, c_int, c_int]

# /usr/include/X11/Xlib.h:2851
XNextEvent = _lib.XNextEvent
XNextEvent.restype = c_int
XNextEvent.argtypes = [POINTER(Display), POINTER(XEvent)]

# /usr/include/X11/Xlib.h:2856
XNoOp = _lib.XNoOp
XNoOp.restype = c_int
XNoOp.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2860
XParseColor = _lib.XParseColor
XParseColor.restype = c_int
XParseColor.argtypes = [POINTER(Display), Colormap, c_char_p, POINTER(XColor)]

# /usr/include/X11/Xlib.h:2867
XParseGeometry = _lib.XParseGeometry
XParseGeometry.restype = c_int
XParseGeometry.argtypes = [c_char_p, POINTER(c_int), POINTER(c_int), POINTER(c_uint), POINTER(c_uint)]

# /usr/include/X11/Xlib.h:2875
XPeekEvent = _lib.XPeekEvent
XPeekEvent.restype = c_int
XPeekEvent.argtypes = [POINTER(Display), POINTER(XEvent)]

# /usr/include/X11/Xlib.h:2880
XPeekIfEvent = _lib.XPeekIfEvent
XPeekIfEvent.restype = c_int
XPeekIfEvent.argtypes = [POINTER(Display), POINTER(XEvent), CFUNCTYPE(c_int, POINTER(Display), POINTER(XEvent), XPointer), XPointer]

# /usr/include/X11/Xlib.h:2891
XPending = _lib.XPending
XPending.restype = c_int
XPending.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2895
XPlanesOfScreen = _lib.XPlanesOfScreen
XPlanesOfScreen.restype = c_int
XPlanesOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:2899
XProtocolRevision = _lib.XProtocolRevision
XProtocolRevision.restype = c_int
XProtocolRevision.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2903
XProtocolVersion = _lib.XProtocolVersion
XProtocolVersion.restype = c_int
XProtocolVersion.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2908
XPutBackEvent = _lib.XPutBackEvent
XPutBackEvent.restype = c_int
XPutBackEvent.argtypes = [POINTER(Display), POINTER(XEvent)]

# /usr/include/X11/Xlib.h:2913
XPutImage = _lib.XPutImage
XPutImage.restype = c_int
XPutImage.argtypes = [POINTER(Display), Drawable, GC, POINTER(XImage), c_int, c_int, c_int, c_int, c_uint, c_uint]

# /usr/include/X11/Xlib.h:2926
XQLength = _lib.XQLength
XQLength.restype = c_int
XQLength.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:2930
XQueryBestCursor = _lib.XQueryBestCursor
XQueryBestCursor.restype = c_int
XQueryBestCursor.argtypes = [POINTER(Display), Drawable, c_uint, c_uint, POINTER(c_uint), POINTER(c_uint)]

# /usr/include/X11/Xlib.h:2939
XQueryBestSize = _lib.XQueryBestSize
XQueryBestSize.restype = c_int
XQueryBestSize.argtypes = [POINTER(Display), c_int, Drawable, c_uint, c_uint, POINTER(c_uint), POINTER(c_uint)]

# /usr/include/X11/Xlib.h:2949
XQueryBestStipple = _lib.XQueryBestStipple
XQueryBestStipple.restype = c_int
XQueryBestStipple.argtypes = [POINTER(Display), Drawable, c_uint, c_uint, POINTER(c_uint), POINTER(c_uint)]

# /usr/include/X11/Xlib.h:2958
XQueryBestTile = _lib.XQueryBestTile
XQueryBestTile.restype = c_int
XQueryBestTile.argtypes = [POINTER(Display), Drawable, c_uint, c_uint, POINTER(c_uint), POINTER(c_uint)]

# /usr/include/X11/Xlib.h:2967
XQueryColor = _lib.XQueryColor
XQueryColor.restype = c_int
XQueryColor.argtypes = [POINTER(Display), Colormap, POINTER(XColor)]

# /usr/include/X11/Xlib.h:2973
XQueryColors = _lib.XQueryColors
XQueryColors.restype = c_int
XQueryColors.argtypes = [POINTER(Display), Colormap, POINTER(XColor), c_int]

# /usr/include/X11/Xlib.h:2980
XQueryExtension = _lib.XQueryExtension
XQueryExtension.restype = c_int
XQueryExtension.argtypes = [POINTER(Display), c_char_p, POINTER(c_int), POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/Xlib.h:2988
XQueryKeymap = _lib.XQueryKeymap
XQueryKeymap.restype = c_int
XQueryKeymap.argtypes = [POINTER(Display), c_char * 32]

# /usr/include/X11/Xlib.h:2993
XQueryPointer = _lib.XQueryPointer
XQueryPointer.restype = c_int
XQueryPointer.argtypes = [POINTER(Display), Window, POINTER(Window), POINTER(Window), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_uint)]

# /usr/include/X11/Xlib.h:3005
XQueryTextExtents = _lib.XQueryTextExtents
XQueryTextExtents.restype = c_int
XQueryTextExtents.argtypes = [POINTER(Display), XID, c_char_p, c_int, POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(XCharStruct)]

# /usr/include/X11/Xlib.h:3016
XQueryTextExtents16 = _lib.XQueryTextExtents16
XQueryTextExtents16.restype = c_int
XQueryTextExtents16.argtypes = [POINTER(Display), XID, POINTER(XChar2b), c_int, POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(XCharStruct)]

# /usr/include/X11/Xlib.h:3027
XQueryTree = _lib.XQueryTree
XQueryTree.restype = c_int
XQueryTree.argtypes = [POINTER(Display), Window, POINTER(Window), POINTER(Window), POINTER(POINTER(Window)), POINTER(c_uint)]

# /usr/include/X11/Xlib.h:3036
XRaiseWindow = _lib.XRaiseWindow
XRaiseWindow.restype = c_int
XRaiseWindow.argtypes = [POINTER(Display), Window]

# /usr/include/X11/Xlib.h:3041
XReadBitmapFile = _lib.XReadBitmapFile
XReadBitmapFile.restype = c_int
XReadBitmapFile.argtypes = [POINTER(Display), Drawable, c_char_p, POINTER(c_uint), POINTER(c_uint), POINTER(Pixmap), POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/Xlib.h:3052
XReadBitmapFileData = _lib.XReadBitmapFileData
XReadBitmapFileData.restype = c_int
XReadBitmapFileData.argtypes = [c_char_p, POINTER(c_uint), POINTER(c_uint), POINTER(POINTER(c_ubyte)), POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/Xlib.h:3061
XRebindKeysym = _lib.XRebindKeysym
XRebindKeysym.restype = c_int
XRebindKeysym.argtypes = [POINTER(Display), KeySym, POINTER(KeySym), c_int, POINTER(c_ubyte), c_int]

# /usr/include/X11/Xlib.h:3070
XRecolorCursor = _lib.XRecolorCursor
XRecolorCursor.restype = c_int
XRecolorCursor.argtypes = [POINTER(Display), Cursor, POINTER(XColor), POINTER(XColor)]

# /usr/include/X11/Xlib.h:3077
XRefreshKeyboardMapping = _lib.XRefreshKeyboardMapping
XRefreshKeyboardMapping.restype = c_int
XRefreshKeyboardMapping.argtypes = [POINTER(XMappingEvent)]

# /usr/include/X11/Xlib.h:3081
XRemoveFromSaveSet = _lib.XRemoveFromSaveSet
XRemoveFromSaveSet.restype = c_int
XRemoveFromSaveSet.argtypes = [POINTER(Display), Window]

# /usr/include/X11/Xlib.h:3086
XRemoveHost = _lib.XRemoveHost
XRemoveHost.restype = c_int
XRemoveHost.argtypes = [POINTER(Display), POINTER(XHostAddress)]

# /usr/include/X11/Xlib.h:3091
XRemoveHosts = _lib.XRemoveHosts
XRemoveHosts.restype = c_int
XRemoveHosts.argtypes = [POINTER(Display), POINTER(XHostAddress), c_int]

# /usr/include/X11/Xlib.h:3097
XReparentWindow = _lib.XReparentWindow
XReparentWindow.restype = c_int
XReparentWindow.argtypes = [POINTER(Display), Window, Window, c_int, c_int]

# /usr/include/X11/Xlib.h:3105
XResetScreenSaver = _lib.XResetScreenSaver
XResetScreenSaver.restype = c_int
XResetScreenSaver.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:3109
XResizeWindow = _lib.XResizeWindow
XResizeWindow.restype = c_int
XResizeWindow.argtypes = [POINTER(Display), Window, c_uint, c_uint]

# /usr/include/X11/Xlib.h:3116
XRestackWindows = _lib.XRestackWindows
XRestackWindows.restype = c_int
XRestackWindows.argtypes = [POINTER(Display), POINTER(Window), c_int]

# /usr/include/X11/Xlib.h:3122
XRotateBuffers = _lib.XRotateBuffers
XRotateBuffers.restype = c_int
XRotateBuffers.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:3127
XRotateWindowProperties = _lib.XRotateWindowProperties
XRotateWindowProperties.restype = c_int
XRotateWindowProperties.argtypes = [POINTER(Display), Window, POINTER(Atom), c_int, c_int]

# /usr/include/X11/Xlib.h:3135
XScreenCount = _lib.XScreenCount
XScreenCount.restype = c_int
XScreenCount.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:3139
XSelectInput = _lib.XSelectInput
XSelectInput.restype = c_int
XSelectInput.argtypes = [POINTER(Display), Window, c_long]

# /usr/include/X11/Xlib.h:3145
XSendEvent = _lib.XSendEvent
XSendEvent.restype = c_int
XSendEvent.argtypes = [POINTER(Display), Window, c_int, c_long, POINTER(XEvent)]

# /usr/include/X11/Xlib.h:3153
XSetAccessControl = _lib.XSetAccessControl
XSetAccessControl.restype = c_int
XSetAccessControl.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:3158
XSetArcMode = _lib.XSetArcMode
XSetArcMode.restype = c_int
XSetArcMode.argtypes = [POINTER(Display), GC, c_int]

# /usr/include/X11/Xlib.h:3164
XSetBackground = _lib.XSetBackground
XSetBackground.restype = c_int
XSetBackground.argtypes = [POINTER(Display), GC, c_ulong]

# /usr/include/X11/Xlib.h:3170
XSetClipMask = _lib.XSetClipMask
XSetClipMask.restype = c_int
XSetClipMask.argtypes = [POINTER(Display), GC, Pixmap]

# /usr/include/X11/Xlib.h:3176
XSetClipOrigin = _lib.XSetClipOrigin
XSetClipOrigin.restype = c_int
XSetClipOrigin.argtypes = [POINTER(Display), GC, c_int, c_int]

# /usr/include/X11/Xlib.h:3183
XSetClipRectangles = _lib.XSetClipRectangles
XSetClipRectangles.restype = c_int
XSetClipRectangles.argtypes = [POINTER(Display), GC, c_int, c_int, POINTER(XRectangle), c_int, c_int]

# /usr/include/X11/Xlib.h:3193
XSetCloseDownMode = _lib.XSetCloseDownMode
XSetCloseDownMode.restype = c_int
XSetCloseDownMode.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:3198
XSetCommand = _lib.XSetCommand
XSetCommand.restype = c_int
XSetCommand.argtypes = [POINTER(Display), Window, POINTER(c_char_p), c_int]

# /usr/include/X11/Xlib.h:3205
XSetDashes = _lib.XSetDashes
XSetDashes.restype = c_int
XSetDashes.argtypes = [POINTER(Display), GC, c_int, c_char_p, c_int]

# /usr/include/X11/Xlib.h:3213
XSetFillRule = _lib.XSetFillRule
XSetFillRule.restype = c_int
XSetFillRule.argtypes = [POINTER(Display), GC, c_int]

# /usr/include/X11/Xlib.h:3219
XSetFillStyle = _lib.XSetFillStyle
XSetFillStyle.restype = c_int
XSetFillStyle.argtypes = [POINTER(Display), GC, c_int]

# /usr/include/X11/Xlib.h:3225
XSetFont = _lib.XSetFont
XSetFont.restype = c_int
XSetFont.argtypes = [POINTER(Display), GC, Font]

# /usr/include/X11/Xlib.h:3231
XSetFontPath = _lib.XSetFontPath
XSetFontPath.restype = c_int
XSetFontPath.argtypes = [POINTER(Display), POINTER(c_char_p), c_int]

# /usr/include/X11/Xlib.h:3237
XSetForeground = _lib.XSetForeground
XSetForeground.restype = c_int
XSetForeground.argtypes = [POINTER(Display), GC, c_ulong]

# /usr/include/X11/Xlib.h:3243
XSetFunction = _lib.XSetFunction
XSetFunction.restype = c_int
XSetFunction.argtypes = [POINTER(Display), GC, c_int]

# /usr/include/X11/Xlib.h:3249
XSetGraphicsExposures = _lib.XSetGraphicsExposures
XSetGraphicsExposures.restype = c_int
XSetGraphicsExposures.argtypes = [POINTER(Display), GC, c_int]

# /usr/include/X11/Xlib.h:3255
XSetIconName = _lib.XSetIconName
XSetIconName.restype = c_int
XSetIconName.argtypes = [POINTER(Display), Window, c_char_p]

# /usr/include/X11/Xlib.h:3261
XSetInputFocus = _lib.XSetInputFocus
XSetInputFocus.restype = c_int
XSetInputFocus.argtypes = [POINTER(Display), Window, c_int, Time]

# /usr/include/X11/Xlib.h:3268
XSetLineAttributes = _lib.XSetLineAttributes
XSetLineAttributes.restype = c_int
XSetLineAttributes.argtypes = [POINTER(Display), GC, c_uint, c_int, c_int, c_int]

# /usr/include/X11/Xlib.h:3277
XSetModifierMapping = _lib.XSetModifierMapping
XSetModifierMapping.restype = c_int
XSetModifierMapping.argtypes = [POINTER(Display), POINTER(XModifierKeymap)]

# /usr/include/X11/Xlib.h:3282
XSetPlaneMask = _lib.XSetPlaneMask
XSetPlaneMask.restype = c_int
XSetPlaneMask.argtypes = [POINTER(Display), GC, c_ulong]

# /usr/include/X11/Xlib.h:3288
XSetPointerMapping = _lib.XSetPointerMapping
XSetPointerMapping.restype = c_int
XSetPointerMapping.argtypes = [POINTER(Display), POINTER(c_ubyte), c_int]

# /usr/include/X11/Xlib.h:3294
XSetScreenSaver = _lib.XSetScreenSaver
XSetScreenSaver.restype = c_int
XSetScreenSaver.argtypes = [POINTER(Display), c_int, c_int, c_int, c_int]

# /usr/include/X11/Xlib.h:3302
XSetSelectionOwner = _lib.XSetSelectionOwner
XSetSelectionOwner.restype = c_int
XSetSelectionOwner.argtypes = [POINTER(Display), Atom, Window, Time]

# /usr/include/X11/Xlib.h:3309
XSetState = _lib.XSetState
XSetState.restype = c_int
XSetState.argtypes = [POINTER(Display), GC, c_ulong, c_ulong, c_int, c_ulong]

# /usr/include/X11/Xlib.h:3318
XSetStipple = _lib.XSetStipple
XSetStipple.restype = c_int
XSetStipple.argtypes = [POINTER(Display), GC, Pixmap]

# /usr/include/X11/Xlib.h:3324
XSetSubwindowMode = _lib.XSetSubwindowMode
XSetSubwindowMode.restype = c_int
XSetSubwindowMode.argtypes = [POINTER(Display), GC, c_int]

# /usr/include/X11/Xlib.h:3330
XSetTSOrigin = _lib.XSetTSOrigin
XSetTSOrigin.restype = c_int
XSetTSOrigin.argtypes = [POINTER(Display), GC, c_int, c_int]

# /usr/include/X11/Xlib.h:3337
XSetTile = _lib.XSetTile
XSetTile.restype = c_int
XSetTile.argtypes = [POINTER(Display), GC, Pixmap]

# /usr/include/X11/Xlib.h:3343
XSetWindowBackground = _lib.XSetWindowBackground
XSetWindowBackground.restype = c_int
XSetWindowBackground.argtypes = [POINTER(Display), Window, c_ulong]

# /usr/include/X11/Xlib.h:3349
XSetWindowBackgroundPixmap = _lib.XSetWindowBackgroundPixmap
XSetWindowBackgroundPixmap.restype = c_int
XSetWindowBackgroundPixmap.argtypes = [POINTER(Display), Window, Pixmap]

# /usr/include/X11/Xlib.h:3355
XSetWindowBorder = _lib.XSetWindowBorder
XSetWindowBorder.restype = c_int
XSetWindowBorder.argtypes = [POINTER(Display), Window, c_ulong]

# /usr/include/X11/Xlib.h:3361
XSetWindowBorderPixmap = _lib.XSetWindowBorderPixmap
XSetWindowBorderPixmap.restype = c_int
XSetWindowBorderPixmap.argtypes = [POINTER(Display), Window, Pixmap]

# /usr/include/X11/Xlib.h:3367
XSetWindowBorderWidth = _lib.XSetWindowBorderWidth
XSetWindowBorderWidth.restype = c_int
XSetWindowBorderWidth.argtypes = [POINTER(Display), Window, c_uint]

# /usr/include/X11/Xlib.h:3373
XSetWindowColormap = _lib.XSetWindowColormap
XSetWindowColormap.restype = c_int
XSetWindowColormap.argtypes = [POINTER(Display), Window, Colormap]

# /usr/include/X11/Xlib.h:3379
XStoreBuffer = _lib.XStoreBuffer
XStoreBuffer.restype = c_int
XStoreBuffer.argtypes = [POINTER(Display), c_char_p, c_int, c_int]

# /usr/include/X11/Xlib.h:3386
XStoreBytes = _lib.XStoreBytes
XStoreBytes.restype = c_int
XStoreBytes.argtypes = [POINTER(Display), c_char_p, c_int]

# /usr/include/X11/Xlib.h:3392
XStoreColor = _lib.XStoreColor
XStoreColor.restype = c_int
XStoreColor.argtypes = [POINTER(Display), Colormap, POINTER(XColor)]

# /usr/include/X11/Xlib.h:3398
XStoreColors = _lib.XStoreColors
XStoreColors.restype = c_int
XStoreColors.argtypes = [POINTER(Display), Colormap, POINTER(XColor), c_int]

# /usr/include/X11/Xlib.h:3405
XStoreName = _lib.XStoreName
XStoreName.restype = c_int
XStoreName.argtypes = [POINTER(Display), Window, c_char_p]

# /usr/include/X11/Xlib.h:3411
XStoreNamedColor = _lib.XStoreNamedColor
XStoreNamedColor.restype = c_int
XStoreNamedColor.argtypes = [POINTER(Display), Colormap, c_char_p, c_ulong, c_int]

# /usr/include/X11/Xlib.h:3419
XSync = _lib.XSync
XSync.restype = c_int
XSync.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:3424
XTextExtents = _lib.XTextExtents
XTextExtents.restype = c_int
XTextExtents.argtypes = [POINTER(XFontStruct), c_char_p, c_int, POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(XCharStruct)]

# /usr/include/X11/Xlib.h:3434
XTextExtents16 = _lib.XTextExtents16
XTextExtents16.restype = c_int
XTextExtents16.argtypes = [POINTER(XFontStruct), POINTER(XChar2b), c_int, POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(XCharStruct)]

# /usr/include/X11/Xlib.h:3444
XTextWidth = _lib.XTextWidth
XTextWidth.restype = c_int
XTextWidth.argtypes = [POINTER(XFontStruct), c_char_p, c_int]

# /usr/include/X11/Xlib.h:3450
XTextWidth16 = _lib.XTextWidth16
XTextWidth16.restype = c_int
XTextWidth16.argtypes = [POINTER(XFontStruct), POINTER(XChar2b), c_int]

# /usr/include/X11/Xlib.h:3456
XTranslateCoordinates = _lib.XTranslateCoordinates
XTranslateCoordinates.restype = c_int
XTranslateCoordinates.argtypes = [POINTER(Display), Window, Window, c_int, c_int, POINTER(c_int), POINTER(c_int), POINTER(Window)]

# /usr/include/X11/Xlib.h:3467
XUndefineCursor = _lib.XUndefineCursor
XUndefineCursor.restype = c_int
XUndefineCursor.argtypes = [POINTER(Display), Window]

# /usr/include/X11/Xlib.h:3472
XUngrabButton = _lib.XUngrabButton
XUngrabButton.restype = c_int
XUngrabButton.argtypes = [POINTER(Display), c_uint, c_uint, Window]

# /usr/include/X11/Xlib.h:3479
XUngrabKey = _lib.XUngrabKey
XUngrabKey.restype = c_int
XUngrabKey.argtypes = [POINTER(Display), c_int, c_uint, Window]

# /usr/include/X11/Xlib.h:3486
XUngrabKeyboard = _lib.XUngrabKeyboard
XUngrabKeyboard.restype = c_int
XUngrabKeyboard.argtypes = [POINTER(Display), Time]

# /usr/include/X11/Xlib.h:3491
XUngrabPointer = _lib.XUngrabPointer
XUngrabPointer.restype = c_int
XUngrabPointer.argtypes = [POINTER(Display), Time]

# /usr/include/X11/Xlib.h:3496
XUngrabServer = _lib.XUngrabServer
XUngrabServer.restype = c_int
XUngrabServer.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:3500
XUninstallColormap = _lib.XUninstallColormap
XUninstallColormap.restype = c_int
XUninstallColormap.argtypes = [POINTER(Display), Colormap]

# /usr/include/X11/Xlib.h:3505
XUnloadFont = _lib.XUnloadFont
XUnloadFont.restype = c_int
XUnloadFont.argtypes = [POINTER(Display), Font]

# /usr/include/X11/Xlib.h:3510
XUnmapSubwindows = _lib.XUnmapSubwindows
XUnmapSubwindows.restype = c_int
XUnmapSubwindows.argtypes = [POINTER(Display), Window]

# /usr/include/X11/Xlib.h:3515
XUnmapWindow = _lib.XUnmapWindow
XUnmapWindow.restype = c_int
XUnmapWindow.argtypes = [POINTER(Display), Window]

# /usr/include/X11/Xlib.h:3520
XVendorRelease = _lib.XVendorRelease
XVendorRelease.restype = c_int
XVendorRelease.argtypes = [POINTER(Display)]

# /usr/include/X11/Xlib.h:3524
XWarpPointer = _lib.XWarpPointer
XWarpPointer.restype = c_int
XWarpPointer.argtypes = [POINTER(Display), Window, Window, c_int, c_int, c_uint, c_uint, c_int, c_int]

# /usr/include/X11/Xlib.h:3536
XWidthMMOfScreen = _lib.XWidthMMOfScreen
XWidthMMOfScreen.restype = c_int
XWidthMMOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:3540
XWidthOfScreen = _lib.XWidthOfScreen
XWidthOfScreen.restype = c_int
XWidthOfScreen.argtypes = [POINTER(Screen)]

# /usr/include/X11/Xlib.h:3544
XWindowEvent = _lib.XWindowEvent
XWindowEvent.restype = c_int
XWindowEvent.argtypes = [POINTER(Display), Window, c_long, POINTER(XEvent)]

# /usr/include/X11/Xlib.h:3551
XWriteBitmapFile = _lib.XWriteBitmapFile
XWriteBitmapFile.restype = c_int
XWriteBitmapFile.argtypes = [POINTER(Display), c_char_p, Pixmap, c_uint, c_uint, c_int, c_int]

# /usr/include/X11/Xlib.h:3561
XSupportsLocale = _lib.XSupportsLocale
XSupportsLocale.restype = c_int
XSupportsLocale.argtypes = []

# /usr/include/X11/Xlib.h:3563
XSetLocaleModifiers = _lib.XSetLocaleModifiers
XSetLocaleModifiers.restype = c_char_p
XSetLocaleModifiers.argtypes = [c_char_p]

class struct__XrmHashBucketRec(Structure):
    __slots__ = [
    ]
struct__XrmHashBucketRec._fields_ = [
    ('_opaque_struct', c_int)
]

# /usr/include/X11/Xlib.h:3567
XOpenOM = _lib.XOpenOM
XOpenOM.restype = XOM
XOpenOM.argtypes = [POINTER(Display), POINTER(struct__XrmHashBucketRec), c_char_p, c_char_p]

# /usr/include/X11/Xlib.h:3574
XCloseOM = _lib.XCloseOM
XCloseOM.restype = c_int
XCloseOM.argtypes = [XOM]

# /usr/include/X11/Xlib.h:3578
XSetOMValues = _lib.XSetOMValues
XSetOMValues.restype = c_char_p
XSetOMValues.argtypes = [XOM]

# /usr/include/X11/Xlib.h:3583
XGetOMValues = _lib.XGetOMValues
XGetOMValues.restype = c_char_p
XGetOMValues.argtypes = [XOM]

# /usr/include/X11/Xlib.h:3588
XDisplayOfOM = _lib.XDisplayOfOM
XDisplayOfOM.restype = POINTER(Display)
XDisplayOfOM.argtypes = [XOM]

# /usr/include/X11/Xlib.h:3592
XLocaleOfOM = _lib.XLocaleOfOM
XLocaleOfOM.restype = c_char_p
XLocaleOfOM.argtypes = [XOM]

# /usr/include/X11/Xlib.h:3596
XCreateOC = _lib.XCreateOC
XCreateOC.restype = XOC
XCreateOC.argtypes = [XOM]

# /usr/include/X11/Xlib.h:3601
XDestroyOC = _lib.XDestroyOC
XDestroyOC.restype = None
XDestroyOC.argtypes = [XOC]

# /usr/include/X11/Xlib.h:3605
XOMOfOC = _lib.XOMOfOC
XOMOfOC.restype = XOM
XOMOfOC.argtypes = [XOC]

# /usr/include/X11/Xlib.h:3609
XSetOCValues = _lib.XSetOCValues
XSetOCValues.restype = c_char_p
XSetOCValues.argtypes = [XOC]

# /usr/include/X11/Xlib.h:3614
XGetOCValues = _lib.XGetOCValues
XGetOCValues.restype = c_char_p
XGetOCValues.argtypes = [XOC]

# /usr/include/X11/Xlib.h:3619
XCreateFontSet = _lib.XCreateFontSet
XCreateFontSet.restype = XFontSet
XCreateFontSet.argtypes = [POINTER(Display), c_char_p, POINTER(POINTER(c_char_p)), POINTER(c_int), POINTER(c_char_p)]

# /usr/include/X11/Xlib.h:3627
XFreeFontSet = _lib.XFreeFontSet
XFreeFontSet.restype = None
XFreeFontSet.argtypes = [POINTER(Display), XFontSet]

# /usr/include/X11/Xlib.h:3632
XFontsOfFontSet = _lib.XFontsOfFontSet
XFontsOfFontSet.restype = c_int
XFontsOfFontSet.argtypes = [XFontSet, POINTER(POINTER(POINTER(XFontStruct))), POINTER(POINTER(c_char_p))]

# /usr/include/X11/Xlib.h:3638
XBaseFontNameListOfFontSet = _lib.XBaseFontNameListOfFontSet
XBaseFontNameListOfFontSet.restype = c_char_p
XBaseFontNameListOfFontSet.argtypes = [XFontSet]

# /usr/include/X11/Xlib.h:3642
XLocaleOfFontSet = _lib.XLocaleOfFontSet
XLocaleOfFontSet.restype = c_char_p
XLocaleOfFontSet.argtypes = [XFontSet]

# /usr/include/X11/Xlib.h:3646
XContextDependentDrawing = _lib.XContextDependentDrawing
XContextDependentDrawing.restype = c_int
XContextDependentDrawing.argtypes = [XFontSet]

# /usr/include/X11/Xlib.h:3650
XDirectionalDependentDrawing = _lib.XDirectionalDependentDrawing
XDirectionalDependentDrawing.restype = c_int
XDirectionalDependentDrawing.argtypes = [XFontSet]

# /usr/include/X11/Xlib.h:3654
XContextualDrawing = _lib.XContextualDrawing
XContextualDrawing.restype = c_int
XContextualDrawing.argtypes = [XFontSet]

# /usr/include/X11/Xlib.h:3658
XExtentsOfFontSet = _lib.XExtentsOfFontSet
XExtentsOfFontSet.restype = POINTER(XFontSetExtents)
XExtentsOfFontSet.argtypes = [XFontSet]

# /usr/include/X11/Xlib.h:3662
XmbTextEscapement = _lib.XmbTextEscapement
XmbTextEscapement.restype = c_int
XmbTextEscapement.argtypes = [XFontSet, c_char_p, c_int]

# /usr/include/X11/Xlib.h:3668
XwcTextEscapement = _lib.XwcTextEscapement
XwcTextEscapement.restype = c_int
XwcTextEscapement.argtypes = [XFontSet, c_wchar_p, c_int]

# /usr/include/X11/Xlib.h:3674
Xutf8TextEscapement = _lib.Xutf8TextEscapement
Xutf8TextEscapement.restype = c_int
Xutf8TextEscapement.argtypes = [XFontSet, c_char_p, c_int]

# /usr/include/X11/Xlib.h:3680
XmbTextExtents = _lib.XmbTextExtents
XmbTextExtents.restype = c_int
XmbTextExtents.argtypes = [XFontSet, c_char_p, c_int, POINTER(XRectangle), POINTER(XRectangle)]

# /usr/include/X11/Xlib.h:3688
XwcTextExtents = _lib.XwcTextExtents
XwcTextExtents.restype = c_int
XwcTextExtents.argtypes = [XFontSet, c_wchar_p, c_int, POINTER(XRectangle), POINTER(XRectangle)]

# /usr/include/X11/Xlib.h:3696
Xutf8TextExtents = _lib.Xutf8TextExtents
Xutf8TextExtents.restype = c_int
Xutf8TextExtents.argtypes = [XFontSet, c_char_p, c_int, POINTER(XRectangle), POINTER(XRectangle)]

# /usr/include/X11/Xlib.h:3704
XmbTextPerCharExtents = _lib.XmbTextPerCharExtents
XmbTextPerCharExtents.restype = c_int
XmbTextPerCharExtents.argtypes = [XFontSet, c_char_p, c_int, POINTER(XRectangle), POINTER(XRectangle), c_int, POINTER(c_int), POINTER(XRectangle), POINTER(XRectangle)]

# /usr/include/X11/Xlib.h:3716
XwcTextPerCharExtents = _lib.XwcTextPerCharExtents
XwcTextPerCharExtents.restype = c_int
XwcTextPerCharExtents.argtypes = [XFontSet, c_wchar_p, c_int, POINTER(XRectangle), POINTER(XRectangle), c_int, POINTER(c_int), POINTER(XRectangle), POINTER(XRectangle)]

# /usr/include/X11/Xlib.h:3728
Xutf8TextPerCharExtents = _lib.Xutf8TextPerCharExtents
Xutf8TextPerCharExtents.restype = c_int
Xutf8TextPerCharExtents.argtypes = [XFontSet, c_char_p, c_int, POINTER(XRectangle), POINTER(XRectangle), c_int, POINTER(c_int), POINTER(XRectangle), POINTER(XRectangle)]

# /usr/include/X11/Xlib.h:3740
XmbDrawText = _lib.XmbDrawText
XmbDrawText.restype = None
XmbDrawText.argtypes = [POINTER(Display), Drawable, GC, c_int, c_int, POINTER(XmbTextItem), c_int]

# /usr/include/X11/Xlib.h:3750
XwcDrawText = _lib.XwcDrawText
XwcDrawText.restype = None
XwcDrawText.argtypes = [POINTER(Display), Drawable, GC, c_int, c_int, POINTER(XwcTextItem), c_int]

# /usr/include/X11/Xlib.h:3760
Xutf8DrawText = _lib.Xutf8DrawText
Xutf8DrawText.restype = None
Xutf8DrawText.argtypes = [POINTER(Display), Drawable, GC, c_int, c_int, POINTER(XmbTextItem), c_int]

# /usr/include/X11/Xlib.h:3770
XmbDrawString = _lib.XmbDrawString
XmbDrawString.restype = None
XmbDrawString.argtypes = [POINTER(Display), Drawable, XFontSet, GC, c_int, c_int, c_char_p, c_int]

# /usr/include/X11/Xlib.h:3781
XwcDrawString = _lib.XwcDrawString
XwcDrawString.restype = None
XwcDrawString.argtypes = [POINTER(Display), Drawable, XFontSet, GC, c_int, c_int, c_wchar_p, c_int]

# /usr/include/X11/Xlib.h:3792
Xutf8DrawString = _lib.Xutf8DrawString
Xutf8DrawString.restype = None
Xutf8DrawString.argtypes = [POINTER(Display), Drawable, XFontSet, GC, c_int, c_int, c_char_p, c_int]

# /usr/include/X11/Xlib.h:3803
XmbDrawImageString = _lib.XmbDrawImageString
XmbDrawImageString.restype = None
XmbDrawImageString.argtypes = [POINTER(Display), Drawable, XFontSet, GC, c_int, c_int, c_char_p, c_int]

# /usr/include/X11/Xlib.h:3814
XwcDrawImageString = _lib.XwcDrawImageString
XwcDrawImageString.restype = None
XwcDrawImageString.argtypes = [POINTER(Display), Drawable, XFontSet, GC, c_int, c_int, c_wchar_p, c_int]

# /usr/include/X11/Xlib.h:3825
Xutf8DrawImageString = _lib.Xutf8DrawImageString
Xutf8DrawImageString.restype = None
Xutf8DrawImageString.argtypes = [POINTER(Display), Drawable, XFontSet, GC, c_int, c_int, c_char_p, c_int]

class struct__XrmHashBucketRec(Structure):
    __slots__ = [
    ]
struct__XrmHashBucketRec._fields_ = [
    ('_opaque_struct', c_int)
]

# /usr/include/X11/Xlib.h:3836
XOpenIM = _lib.XOpenIM
XOpenIM.restype = XIM
XOpenIM.argtypes = [POINTER(Display), POINTER(struct__XrmHashBucketRec), c_char_p, c_char_p]

# /usr/include/X11/Xlib.h:3843
XCloseIM = _lib.XCloseIM
XCloseIM.restype = c_int
XCloseIM.argtypes = [XIM]

# /usr/include/X11/Xlib.h:3847
XGetIMValues = _lib.XGetIMValues
XGetIMValues.restype = c_char_p
XGetIMValues.argtypes = [XIM]

# /usr/include/X11/Xlib.h:3851
XSetIMValues = _lib.XSetIMValues
XSetIMValues.restype = c_char_p
XSetIMValues.argtypes = [XIM]

# /usr/include/X11/Xlib.h:3855
XDisplayOfIM = _lib.XDisplayOfIM
XDisplayOfIM.restype = POINTER(Display)
XDisplayOfIM.argtypes = [XIM]

# /usr/include/X11/Xlib.h:3859
XLocaleOfIM = _lib.XLocaleOfIM
XLocaleOfIM.restype = c_char_p
XLocaleOfIM.argtypes = [XIM]

# /usr/include/X11/Xlib.h:3863
XCreateIC = _lib.XCreateIC
XCreateIC.restype = XIC
XCreateIC.argtypes = [XIM]

# /usr/include/X11/Xlib.h:3867
XDestroyIC = _lib.XDestroyIC
XDestroyIC.restype = None
XDestroyIC.argtypes = [XIC]

# /usr/include/X11/Xlib.h:3871
XSetICFocus = _lib.XSetICFocus
XSetICFocus.restype = None
XSetICFocus.argtypes = [XIC]

# /usr/include/X11/Xlib.h:3875
XUnsetICFocus = _lib.XUnsetICFocus
XUnsetICFocus.restype = None
XUnsetICFocus.argtypes = [XIC]

# /usr/include/X11/Xlib.h:3879
XwcResetIC = _lib.XwcResetIC
XwcResetIC.restype = c_wchar_p
XwcResetIC.argtypes = [XIC]

# /usr/include/X11/Xlib.h:3883
XmbResetIC = _lib.XmbResetIC
XmbResetIC.restype = c_char_p
XmbResetIC.argtypes = [XIC]

# /usr/include/X11/Xlib.h:3887
Xutf8ResetIC = _lib.Xutf8ResetIC
Xutf8ResetIC.restype = c_char_p
Xutf8ResetIC.argtypes = [XIC]

# /usr/include/X11/Xlib.h:3891
XSetICValues = _lib.XSetICValues
XSetICValues.restype = c_char_p
XSetICValues.argtypes = [XIC]

# /usr/include/X11/Xlib.h:3895
XGetICValues = _lib.XGetICValues
XGetICValues.restype = c_char_p
XGetICValues.argtypes = [XIC]

# /usr/include/X11/Xlib.h:3899
XIMOfIC = _lib.XIMOfIC
XIMOfIC.restype = XIM
XIMOfIC.argtypes = [XIC]

# /usr/include/X11/Xlib.h:3903
XFilterEvent = _lib.XFilterEvent
XFilterEvent.restype = c_int
XFilterEvent.argtypes = [POINTER(XEvent), Window]

# /usr/include/X11/Xlib.h:3908
XmbLookupString = _lib.XmbLookupString
XmbLookupString.restype = c_int
XmbLookupString.argtypes = [XIC, POINTER(XKeyPressedEvent), c_char_p, c_int, POINTER(KeySym), POINTER(c_int)]

# /usr/include/X11/Xlib.h:3917
XwcLookupString = _lib.XwcLookupString
XwcLookupString.restype = c_int
XwcLookupString.argtypes = [XIC, POINTER(XKeyPressedEvent), c_wchar_p, c_int, POINTER(KeySym), POINTER(c_int)]

# /usr/include/X11/Xlib.h:3926
Xutf8LookupString = _lib.Xutf8LookupString
Xutf8LookupString.restype = c_int
Xutf8LookupString.argtypes = [XIC, POINTER(XKeyPressedEvent), c_char_p, c_int, POINTER(KeySym), POINTER(c_int)]

# /usr/include/X11/Xlib.h:3935
XVaCreateNestedList = _lib.XVaCreateNestedList
XVaCreateNestedList.restype = XVaNestedList
XVaCreateNestedList.argtypes = [c_int]

class struct__XrmHashBucketRec(Structure):
    __slots__ = [
    ]
struct__XrmHashBucketRec._fields_ = [
    ('_opaque_struct', c_int)
]

# /usr/include/X11/Xlib.h:3941
XRegisterIMInstantiateCallback = _lib.XRegisterIMInstantiateCallback
XRegisterIMInstantiateCallback.restype = c_int
XRegisterIMInstantiateCallback.argtypes = [POINTER(Display), POINTER(struct__XrmHashBucketRec), c_char_p, c_char_p, XIDProc, XPointer]

class struct__XrmHashBucketRec(Structure):
    __slots__ = [
    ]
struct__XrmHashBucketRec._fields_ = [
    ('_opaque_struct', c_int)
]

# /usr/include/X11/Xlib.h:3950
XUnregisterIMInstantiateCallback = _lib.XUnregisterIMInstantiateCallback
XUnregisterIMInstantiateCallback.restype = c_int
XUnregisterIMInstantiateCallback.argtypes = [POINTER(Display), POINTER(struct__XrmHashBucketRec), c_char_p, c_char_p, XIDProc, XPointer]

XConnectionWatchProc = CFUNCTYPE(None, POINTER(Display), XPointer, c_int, c_int, POINTER(XPointer)) 	# /usr/include/X11/Xlib.h:3959
# /usr/include/X11/Xlib.h:3968
XInternalConnectionNumbers = _lib.XInternalConnectionNumbers
XInternalConnectionNumbers.restype = c_int
XInternalConnectionNumbers.argtypes = [POINTER(Display), POINTER(POINTER(c_int)), POINTER(c_int)]

# /usr/include/X11/Xlib.h:3974
XProcessInternalConnection = _lib.XProcessInternalConnection
XProcessInternalConnection.restype = None
XProcessInternalConnection.argtypes = [POINTER(Display), c_int]

# /usr/include/X11/Xlib.h:3979
XAddConnectionWatch = _lib.XAddConnectionWatch
XAddConnectionWatch.restype = c_int
XAddConnectionWatch.argtypes = [POINTER(Display), XConnectionWatchProc, XPointer]

# /usr/include/X11/Xlib.h:3985
XRemoveConnectionWatch = _lib.XRemoveConnectionWatch
XRemoveConnectionWatch.restype = None
XRemoveConnectionWatch.argtypes = [POINTER(Display), XConnectionWatchProc, XPointer]

# /usr/include/X11/Xlib.h:3991
XSetAuthorization = _lib.XSetAuthorization
XSetAuthorization.restype = None
XSetAuthorization.argtypes = [c_char_p, c_int, c_char_p, c_int]

# /usr/include/X11/Xlib.h:3998
_Xmbtowc = _lib._Xmbtowc
_Xmbtowc.restype = c_int
_Xmbtowc.argtypes = [c_wchar_p, c_char_p, c_int]

# /usr/include/X11/Xlib.h:4009
_Xwctomb = _lib._Xwctomb
_Xwctomb.restype = c_int
_Xwctomb.argtypes = [c_char_p, c_wchar]

# /usr/include/X11/Xlib.h:4014
XGetEventData = _lib.XGetEventData
XGetEventData.restype = c_int
XGetEventData.argtypes = [POINTER(Display), POINTER(XGenericEventCookie)]

# /usr/include/X11/Xlib.h:4019
XFreeEventData = _lib.XFreeEventData
XFreeEventData.restype = None
XFreeEventData.argtypes = [POINTER(Display), POINTER(XGenericEventCookie)]

NoValue = 0 	# /usr/include/X11/Xutil.h:4805
XValue = 1 	# /usr/include/X11/Xutil.h:4806
YValue = 2 	# /usr/include/X11/Xutil.h:4807
WidthValue = 4 	# /usr/include/X11/Xutil.h:4808
HeightValue = 8 	# /usr/include/X11/Xutil.h:4809
AllValues = 15 	# /usr/include/X11/Xutil.h:4810
XNegative = 16 	# /usr/include/X11/Xutil.h:4811
YNegative = 32 	# /usr/include/X11/Xutil.h:4812
class struct_anon_95(Structure):
    __slots__ = [
        'flags',
        'x',
        'y',
        'width',
        'height',
        'min_width',
        'min_height',
        'max_width',
        'max_height',
        'width_inc',
        'height_inc',
        'min_aspect',
        'max_aspect',
        'base_width',
        'base_height',
        'win_gravity',
    ]
class struct_anon_96(Structure):
    __slots__ = [
        'x',
        'y',
    ]
struct_anon_96._fields_ = [
    ('x', c_int),
    ('y', c_int),
]

class struct_anon_97(Structure):
    __slots__ = [
        'x',
        'y',
    ]
struct_anon_97._fields_ = [
    ('x', c_int),
    ('y', c_int),
]

struct_anon_95._fields_ = [
    ('flags', c_long),
    ('x', c_int),
    ('y', c_int),
    ('width', c_int),
    ('height', c_int),
    ('min_width', c_int),
    ('min_height', c_int),
    ('max_width', c_int),
    ('max_height', c_int),
    ('width_inc', c_int),
    ('height_inc', c_int),
    ('min_aspect', struct_anon_96),
    ('max_aspect', struct_anon_97),
    ('base_width', c_int),
    ('base_height', c_int),
    ('win_gravity', c_int),
]

XSizeHints = struct_anon_95 	# /usr/include/X11/Xutil.h:4831
USPosition = 1 	# /usr/include/X11/Xutil.h:4839
USSize = 2 	# /usr/include/X11/Xutil.h:4840
PPosition = 4 	# /usr/include/X11/Xutil.h:4842
PSize = 8 	# /usr/include/X11/Xutil.h:4843
PMinSize = 16 	# /usr/include/X11/Xutil.h:4844
PMaxSize = 32 	# /usr/include/X11/Xutil.h:4845
PResizeInc = 64 	# /usr/include/X11/Xutil.h:4846
PAspect = 128 	# /usr/include/X11/Xutil.h:4847
PBaseSize = 256 	# /usr/include/X11/Xutil.h:4848
PWinGravity = 512 	# /usr/include/X11/Xutil.h:4849
PAllHints = 252 	# /usr/include/X11/Xutil.h:4852
class struct_anon_98(Structure):
    __slots__ = [
        'flags',
        'input',
        'initial_state',
        'icon_pixmap',
        'icon_window',
        'icon_x',
        'icon_y',
        'icon_mask',
        'window_group',
    ]
struct_anon_98._fields_ = [
    ('flags', c_long),
    ('input', c_int),
    ('initial_state', c_int),
    ('icon_pixmap', Pixmap),
    ('icon_window', Window),
    ('icon_x', c_int),
    ('icon_y', c_int),
    ('icon_mask', Pixmap),
    ('window_group', XID),
]

XWMHints = struct_anon_98 	# /usr/include/X11/Xutil.h:4867
InputHint = 1 	# /usr/include/X11/Xutil.h:4871
StateHint = 2 	# /usr/include/X11/Xutil.h:4872
IconPixmapHint = 4 	# /usr/include/X11/Xutil.h:4873
IconWindowHint = 8 	# /usr/include/X11/Xutil.h:4874
IconPositionHint = 16 	# /usr/include/X11/Xutil.h:4875
IconMaskHint = 32 	# /usr/include/X11/Xutil.h:4876
WindowGroupHint = 64 	# /usr/include/X11/Xutil.h:4877
AllHints = 127 	# /usr/include/X11/Xutil.h:4878
XUrgencyHint = 256 	# /usr/include/X11/Xutil.h:4880
WithdrawnState = 0 	# /usr/include/X11/Xutil.h:4883
NormalState = 1 	# /usr/include/X11/Xutil.h:4884
IconicState = 3 	# /usr/include/X11/Xutil.h:4885
DontCareState = 0 	# /usr/include/X11/Xutil.h:4890
ZoomState = 2 	# /usr/include/X11/Xutil.h:4891
InactiveState = 4 	# /usr/include/X11/Xutil.h:4892
class struct_anon_99(Structure):
    __slots__ = [
        'value',
        'encoding',
        'format',
        'nitems',
    ]
struct_anon_99._fields_ = [
    ('value', POINTER(c_ubyte)),
    ('encoding', Atom),
    ('format', c_int),
    ('nitems', c_ulong),
]

XTextProperty = struct_anon_99 	# /usr/include/X11/Xutil.h:4905
XNoMemory = -1 	# /usr/include/X11/Xutil.h:4907
XLocaleNotSupported = -2 	# /usr/include/X11/Xutil.h:4908
XConverterNotFound = -3 	# /usr/include/X11/Xutil.h:4909
enum_anon_100 = c_int
XStringStyle = 0
XCompoundTextStyle = 1
XTextStyle = 2
XStdICCTextStyle = 3
XUTF8StringStyle = 4
XICCEncodingStyle = enum_anon_100 	# /usr/include/X11/Xutil.h:4918
class struct_anon_101(Structure):
    __slots__ = [
        'min_width',
        'min_height',
        'max_width',
        'max_height',
        'width_inc',
        'height_inc',
    ]
struct_anon_101._fields_ = [
    ('min_width', c_int),
    ('min_height', c_int),
    ('max_width', c_int),
    ('max_height', c_int),
    ('width_inc', c_int),
    ('height_inc', c_int),
]

XIconSize = struct_anon_101 	# /usr/include/X11/Xutil.h:4924
class struct_anon_102(Structure):
    __slots__ = [
        'res_name',
        'res_class',
    ]
struct_anon_102._fields_ = [
    ('res_name', c_char_p),
    ('res_class', c_char_p),
]

XClassHint = struct_anon_102 	# /usr/include/X11/Xutil.h:4929
class struct__XComposeStatus(Structure):
    __slots__ = [
        'compose_ptr',
        'chars_matched',
    ]
struct__XComposeStatus._fields_ = [
    ('compose_ptr', XPointer),
    ('chars_matched', c_int),
]

XComposeStatus = struct__XComposeStatus 	# /usr/include/X11/Xutil.h:4971
class struct__XRegion(Structure):
    __slots__ = [
    ]
struct__XRegion._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XRegion(Structure):
    __slots__ = [
    ]
struct__XRegion._fields_ = [
    ('_opaque_struct', c_int)
]

Region = POINTER(struct__XRegion) 	# /usr/include/X11/Xutil.h:5010
RectangleOut = 0 	# /usr/include/X11/Xutil.h:5014
RectangleIn = 1 	# /usr/include/X11/Xutil.h:5015
RectanglePart = 2 	# /usr/include/X11/Xutil.h:5016
class struct_anon_103(Structure):
    __slots__ = [
        'visual',
        'visualid',
        'screen',
        'depth',
        'class',
        'red_mask',
        'green_mask',
        'blue_mask',
        'colormap_size',
        'bits_per_rgb',
    ]
struct_anon_103._fields_ = [
    ('visual', POINTER(Visual)),
    ('visualid', VisualID),
    ('screen', c_int),
    ('depth', c_int),
    ('class', c_int),
    ('red_mask', c_ulong),
    ('green_mask', c_ulong),
    ('blue_mask', c_ulong),
    ('colormap_size', c_int),
    ('bits_per_rgb', c_int),
]

XVisualInfo = struct_anon_103 	# /usr/include/X11/Xutil.h:5039
VisualNoMask = 0 	# /usr/include/X11/Xutil.h:5041
VisualIDMask = 1 	# /usr/include/X11/Xutil.h:5042
VisualScreenMask = 2 	# /usr/include/X11/Xutil.h:5043
VisualDepthMask = 4 	# /usr/include/X11/Xutil.h:5044
VisualClassMask = 8 	# /usr/include/X11/Xutil.h:5045
VisualRedMaskMask = 16 	# /usr/include/X11/Xutil.h:5046
VisualGreenMaskMask = 32 	# /usr/include/X11/Xutil.h:5047
VisualBlueMaskMask = 64 	# /usr/include/X11/Xutil.h:5048
VisualColormapSizeMask = 128 	# /usr/include/X11/Xutil.h:5049
VisualBitsPerRGBMask = 256 	# /usr/include/X11/Xutil.h:5050
VisualAllMask = 511 	# /usr/include/X11/Xutil.h:5051
class struct_anon_104(Structure):
    __slots__ = [
        'colormap',
        'red_max',
        'red_mult',
        'green_max',
        'green_mult',
        'blue_max',
        'blue_mult',
        'base_pixel',
        'visualid',
        'killid',
    ]
struct_anon_104._fields_ = [
    ('colormap', Colormap),
    ('red_max', c_ulong),
    ('red_mult', c_ulong),
    ('green_max', c_ulong),
    ('green_mult', c_ulong),
    ('blue_max', c_ulong),
    ('blue_mult', c_ulong),
    ('base_pixel', c_ulong),
    ('visualid', VisualID),
    ('killid', XID),
]

XStandardColormap = struct_anon_104 	# /usr/include/X11/Xutil.h:5068
BitmapSuccess = 0 	# /usr/include/X11/Xutil.h:5076
BitmapOpenFailed = 1 	# /usr/include/X11/Xutil.h:5077
BitmapFileInvalid = 2 	# /usr/include/X11/Xutil.h:5078
BitmapNoMemory = 3 	# /usr/include/X11/Xutil.h:5079
XCSUCCESS = 0 	# /usr/include/X11/Xutil.h:5090
XCNOMEM = 1 	# /usr/include/X11/Xutil.h:5091
XCNOENT = 2 	# /usr/include/X11/Xutil.h:5092
XContext = c_int 	# /usr/include/X11/Xutil.h:5094
# /usr/include/X11/Xutil.h:5103
XAllocClassHint = _lib.XAllocClassHint
XAllocClassHint.restype = POINTER(XClassHint)
XAllocClassHint.argtypes = []

# /usr/include/X11/Xutil.h:5107
XAllocIconSize = _lib.XAllocIconSize
XAllocIconSize.restype = POINTER(XIconSize)
XAllocIconSize.argtypes = []

# /usr/include/X11/Xutil.h:5111
XAllocSizeHints = _lib.XAllocSizeHints
XAllocSizeHints.restype = POINTER(XSizeHints)
XAllocSizeHints.argtypes = []

# /usr/include/X11/Xutil.h:5115
XAllocStandardColormap = _lib.XAllocStandardColormap
XAllocStandardColormap.restype = POINTER(XStandardColormap)
XAllocStandardColormap.argtypes = []

# /usr/include/X11/Xutil.h:5119
XAllocWMHints = _lib.XAllocWMHints
XAllocWMHints.restype = POINTER(XWMHints)
XAllocWMHints.argtypes = []

# /usr/include/X11/Xutil.h:5123
XClipBox = _lib.XClipBox
XClipBox.restype = c_int
XClipBox.argtypes = [Region, POINTER(XRectangle)]

# /usr/include/X11/Xutil.h:5128
XCreateRegion = _lib.XCreateRegion
XCreateRegion.restype = Region
XCreateRegion.argtypes = []

# /usr/include/X11/Xutil.h:5132
XDefaultString = _lib.XDefaultString
XDefaultString.restype = c_char_p
XDefaultString.argtypes = []

# /usr/include/X11/Xutil.h:5134
XDeleteContext = _lib.XDeleteContext
XDeleteContext.restype = c_int
XDeleteContext.argtypes = [POINTER(Display), XID, XContext]

# /usr/include/X11/Xutil.h:5140
XDestroyRegion = _lib.XDestroyRegion
XDestroyRegion.restype = c_int
XDestroyRegion.argtypes = [Region]

# /usr/include/X11/Xutil.h:5144
XEmptyRegion = _lib.XEmptyRegion
XEmptyRegion.restype = c_int
XEmptyRegion.argtypes = [Region]

# /usr/include/X11/Xutil.h:5148
XEqualRegion = _lib.XEqualRegion
XEqualRegion.restype = c_int
XEqualRegion.argtypes = [Region, Region]

# /usr/include/X11/Xutil.h:5153
XFindContext = _lib.XFindContext
XFindContext.restype = c_int
XFindContext.argtypes = [POINTER(Display), XID, XContext, POINTER(XPointer)]

# /usr/include/X11/Xutil.h:5160
XGetClassHint = _lib.XGetClassHint
XGetClassHint.restype = c_int
XGetClassHint.argtypes = [POINTER(Display), Window, POINTER(XClassHint)]

# /usr/include/X11/Xutil.h:5166
XGetIconSizes = _lib.XGetIconSizes
XGetIconSizes.restype = c_int
XGetIconSizes.argtypes = [POINTER(Display), Window, POINTER(POINTER(XIconSize)), POINTER(c_int)]

# /usr/include/X11/Xutil.h:5173
XGetNormalHints = _lib.XGetNormalHints
XGetNormalHints.restype = c_int
XGetNormalHints.argtypes = [POINTER(Display), Window, POINTER(XSizeHints)]

# /usr/include/X11/Xutil.h:5179
XGetRGBColormaps = _lib.XGetRGBColormaps
XGetRGBColormaps.restype = c_int
XGetRGBColormaps.argtypes = [POINTER(Display), Window, POINTER(POINTER(XStandardColormap)), POINTER(c_int), Atom]

# /usr/include/X11/Xutil.h:5187
XGetSizeHints = _lib.XGetSizeHints
XGetSizeHints.restype = c_int
XGetSizeHints.argtypes = [POINTER(Display), Window, POINTER(XSizeHints), Atom]

# /usr/include/X11/Xutil.h:5194
XGetStandardColormap = _lib.XGetStandardColormap
XGetStandardColormap.restype = c_int
XGetStandardColormap.argtypes = [POINTER(Display), Window, POINTER(XStandardColormap), Atom]

# /usr/include/X11/Xutil.h:5201
XGetTextProperty = _lib.XGetTextProperty
XGetTextProperty.restype = c_int
XGetTextProperty.argtypes = [POINTER(Display), Window, POINTER(XTextProperty), Atom]

# /usr/include/X11/Xutil.h:5208
XGetVisualInfo = _lib.XGetVisualInfo
XGetVisualInfo.restype = POINTER(XVisualInfo)
XGetVisualInfo.argtypes = [POINTER(Display), c_long, POINTER(XVisualInfo), POINTER(c_int)]

# /usr/include/X11/Xutil.h:5215
XGetWMClientMachine = _lib.XGetWMClientMachine
XGetWMClientMachine.restype = c_int
XGetWMClientMachine.argtypes = [POINTER(Display), Window, POINTER(XTextProperty)]

# /usr/include/X11/Xutil.h:5221
XGetWMHints = _lib.XGetWMHints
XGetWMHints.restype = POINTER(XWMHints)
XGetWMHints.argtypes = [POINTER(Display), Window]

# /usr/include/X11/Xutil.h:5226
XGetWMIconName = _lib.XGetWMIconName
XGetWMIconName.restype = c_int
XGetWMIconName.argtypes = [POINTER(Display), Window, POINTER(XTextProperty)]

# /usr/include/X11/Xutil.h:5232
XGetWMName = _lib.XGetWMName
XGetWMName.restype = c_int
XGetWMName.argtypes = [POINTER(Display), Window, POINTER(XTextProperty)]

# /usr/include/X11/Xutil.h:5238
XGetWMNormalHints = _lib.XGetWMNormalHints
XGetWMNormalHints.restype = c_int
XGetWMNormalHints.argtypes = [POINTER(Display), Window, POINTER(XSizeHints), POINTER(c_long)]

# /usr/include/X11/Xutil.h:5245
XGetWMSizeHints = _lib.XGetWMSizeHints
XGetWMSizeHints.restype = c_int
XGetWMSizeHints.argtypes = [POINTER(Display), Window, POINTER(XSizeHints), POINTER(c_long), Atom]

# /usr/include/X11/Xutil.h:5253
XGetZoomHints = _lib.XGetZoomHints
XGetZoomHints.restype = c_int
XGetZoomHints.argtypes = [POINTER(Display), Window, POINTER(XSizeHints)]

# /usr/include/X11/Xutil.h:5259
XIntersectRegion = _lib.XIntersectRegion
XIntersectRegion.restype = c_int
XIntersectRegion.argtypes = [Region, Region, Region]

# /usr/include/X11/Xutil.h:5265
XConvertCase = _lib.XConvertCase
XConvertCase.restype = None
XConvertCase.argtypes = [KeySym, POINTER(KeySym), POINTER(KeySym)]

# /usr/include/X11/Xutil.h:5271
XLookupString = _lib.XLookupString
XLookupString.restype = c_int
XLookupString.argtypes = [POINTER(XKeyEvent), c_char_p, c_int, POINTER(KeySym), POINTER(XComposeStatus)]

# /usr/include/X11/Xutil.h:5279
XMatchVisualInfo = _lib.XMatchVisualInfo
XMatchVisualInfo.restype = c_int
XMatchVisualInfo.argtypes = [POINTER(Display), c_int, c_int, c_int, POINTER(XVisualInfo)]

# /usr/include/X11/Xutil.h:5287
XOffsetRegion = _lib.XOffsetRegion
XOffsetRegion.restype = c_int
XOffsetRegion.argtypes = [Region, c_int, c_int]

# /usr/include/X11/Xutil.h:5293
XPointInRegion = _lib.XPointInRegion
XPointInRegion.restype = c_int
XPointInRegion.argtypes = [Region, c_int, c_int]

# /usr/include/X11/Xutil.h:5299
XPolygonRegion = _lib.XPolygonRegion
XPolygonRegion.restype = Region
XPolygonRegion.argtypes = [POINTER(XPoint), c_int, c_int]

# /usr/include/X11/Xutil.h:5305
XRectInRegion = _lib.XRectInRegion
XRectInRegion.restype = c_int
XRectInRegion.argtypes = [Region, c_int, c_int, c_uint, c_uint]

# /usr/include/X11/Xutil.h:5313
XSaveContext = _lib.XSaveContext
XSaveContext.restype = c_int
XSaveContext.argtypes = [POINTER(Display), XID, XContext, c_char_p]

# /usr/include/X11/Xutil.h:5320
XSetClassHint = _lib.XSetClassHint
XSetClassHint.restype = c_int
XSetClassHint.argtypes = [POINTER(Display), Window, POINTER(XClassHint)]

# /usr/include/X11/Xutil.h:5326
XSetIconSizes = _lib.XSetIconSizes
XSetIconSizes.restype = c_int
XSetIconSizes.argtypes = [POINTER(Display), Window, POINTER(XIconSize), c_int]

# /usr/include/X11/Xutil.h:5333
XSetNormalHints = _lib.XSetNormalHints
XSetNormalHints.restype = c_int
XSetNormalHints.argtypes = [POINTER(Display), Window, POINTER(XSizeHints)]

# /usr/include/X11/Xutil.h:5339
XSetRGBColormaps = _lib.XSetRGBColormaps
XSetRGBColormaps.restype = None
XSetRGBColormaps.argtypes = [POINTER(Display), Window, POINTER(XStandardColormap), c_int, Atom]

# /usr/include/X11/Xutil.h:5347
XSetSizeHints = _lib.XSetSizeHints
XSetSizeHints.restype = c_int
XSetSizeHints.argtypes = [POINTER(Display), Window, POINTER(XSizeHints), Atom]

# /usr/include/X11/Xutil.h:5354
XSetStandardProperties = _lib.XSetStandardProperties
XSetStandardProperties.restype = c_int
XSetStandardProperties.argtypes = [POINTER(Display), Window, c_char_p, c_char_p, Pixmap, POINTER(c_char_p), c_int, POINTER(XSizeHints)]

# /usr/include/X11/Xutil.h:5365
XSetTextProperty = _lib.XSetTextProperty
XSetTextProperty.restype = None
XSetTextProperty.argtypes = [POINTER(Display), Window, POINTER(XTextProperty), Atom]

# /usr/include/X11/Xutil.h:5372
XSetWMClientMachine = _lib.XSetWMClientMachine
XSetWMClientMachine.restype = None
XSetWMClientMachine.argtypes = [POINTER(Display), Window, POINTER(XTextProperty)]

# /usr/include/X11/Xutil.h:5378
XSetWMHints = _lib.XSetWMHints
XSetWMHints.restype = c_int
XSetWMHints.argtypes = [POINTER(Display), Window, POINTER(XWMHints)]

# /usr/include/X11/Xutil.h:5384
XSetWMIconName = _lib.XSetWMIconName
XSetWMIconName.restype = None
XSetWMIconName.argtypes = [POINTER(Display), Window, POINTER(XTextProperty)]

# /usr/include/X11/Xutil.h:5390
XSetWMName = _lib.XSetWMName
XSetWMName.restype = None
XSetWMName.argtypes = [POINTER(Display), Window, POINTER(XTextProperty)]

# /usr/include/X11/Xutil.h:5396
XSetWMNormalHints = _lib.XSetWMNormalHints
XSetWMNormalHints.restype = None
XSetWMNormalHints.argtypes = [POINTER(Display), Window, POINTER(XSizeHints)]

# /usr/include/X11/Xutil.h:5402
XSetWMProperties = _lib.XSetWMProperties
XSetWMProperties.restype = None
XSetWMProperties.argtypes = [POINTER(Display), Window, POINTER(XTextProperty), POINTER(XTextProperty), POINTER(c_char_p), c_int, POINTER(XSizeHints), POINTER(XWMHints), POINTER(XClassHint)]

# /usr/include/X11/Xutil.h:5414
XmbSetWMProperties = _lib.XmbSetWMProperties
XmbSetWMProperties.restype = None
XmbSetWMProperties.argtypes = [POINTER(Display), Window, c_char_p, c_char_p, POINTER(c_char_p), c_int, POINTER(XSizeHints), POINTER(XWMHints), POINTER(XClassHint)]

# /usr/include/X11/Xutil.h:5426
Xutf8SetWMProperties = _lib.Xutf8SetWMProperties
Xutf8SetWMProperties.restype = None
Xutf8SetWMProperties.argtypes = [POINTER(Display), Window, c_char_p, c_char_p, POINTER(c_char_p), c_int, POINTER(XSizeHints), POINTER(XWMHints), POINTER(XClassHint)]

# /usr/include/X11/Xutil.h:5438
XSetWMSizeHints = _lib.XSetWMSizeHints
XSetWMSizeHints.restype = None
XSetWMSizeHints.argtypes = [POINTER(Display), Window, POINTER(XSizeHints), Atom]

# /usr/include/X11/Xutil.h:5445
XSetRegion = _lib.XSetRegion
XSetRegion.restype = c_int
XSetRegion.argtypes = [POINTER(Display), GC, Region]

# /usr/include/X11/Xutil.h:5451
XSetStandardColormap = _lib.XSetStandardColormap
XSetStandardColormap.restype = None
XSetStandardColormap.argtypes = [POINTER(Display), Window, POINTER(XStandardColormap), Atom]

# /usr/include/X11/Xutil.h:5458
XSetZoomHints = _lib.XSetZoomHints
XSetZoomHints.restype = c_int
XSetZoomHints.argtypes = [POINTER(Display), Window, POINTER(XSizeHints)]

# /usr/include/X11/Xutil.h:5464
XShrinkRegion = _lib.XShrinkRegion
XShrinkRegion.restype = c_int
XShrinkRegion.argtypes = [Region, c_int, c_int]

# /usr/include/X11/Xutil.h:5470
XStringListToTextProperty = _lib.XStringListToTextProperty
XStringListToTextProperty.restype = c_int
XStringListToTextProperty.argtypes = [POINTER(c_char_p), c_int, POINTER(XTextProperty)]

# /usr/include/X11/Xutil.h:5476
XSubtractRegion = _lib.XSubtractRegion
XSubtractRegion.restype = c_int
XSubtractRegion.argtypes = [Region, Region, Region]

# /usr/include/X11/Xutil.h:5482
XmbTextListToTextProperty = _lib.XmbTextListToTextProperty
XmbTextListToTextProperty.restype = c_int
XmbTextListToTextProperty.argtypes = [POINTER(Display), POINTER(c_char_p), c_int, XICCEncodingStyle, POINTER(XTextProperty)]

# /usr/include/X11/Xutil.h:5490
XwcTextListToTextProperty = _lib.XwcTextListToTextProperty
XwcTextListToTextProperty.restype = c_int
XwcTextListToTextProperty.argtypes = [POINTER(Display), POINTER(c_wchar_p), c_int, XICCEncodingStyle, POINTER(XTextProperty)]

# /usr/include/X11/Xutil.h:5498
Xutf8TextListToTextProperty = _lib.Xutf8TextListToTextProperty
Xutf8TextListToTextProperty.restype = c_int
Xutf8TextListToTextProperty.argtypes = [POINTER(Display), POINTER(c_char_p), c_int, XICCEncodingStyle, POINTER(XTextProperty)]

# /usr/include/X11/Xutil.h:5506
XwcFreeStringList = _lib.XwcFreeStringList
XwcFreeStringList.restype = None
XwcFreeStringList.argtypes = [POINTER(c_wchar_p)]

# /usr/include/X11/Xutil.h:5510
XTextPropertyToStringList = _lib.XTextPropertyToStringList
XTextPropertyToStringList.restype = c_int
XTextPropertyToStringList.argtypes = [POINTER(XTextProperty), POINTER(POINTER(c_char_p)), POINTER(c_int)]

# /usr/include/X11/Xutil.h:5516
XmbTextPropertyToTextList = _lib.XmbTextPropertyToTextList
XmbTextPropertyToTextList.restype = c_int
XmbTextPropertyToTextList.argtypes = [POINTER(Display), POINTER(XTextProperty), POINTER(POINTER(c_char_p)), POINTER(c_int)]

# /usr/include/X11/Xutil.h:5523
XwcTextPropertyToTextList = _lib.XwcTextPropertyToTextList
XwcTextPropertyToTextList.restype = c_int
XwcTextPropertyToTextList.argtypes = [POINTER(Display), POINTER(XTextProperty), POINTER(POINTER(c_wchar_p)), POINTER(c_int)]

# /usr/include/X11/Xutil.h:5530
Xutf8TextPropertyToTextList = _lib.Xutf8TextPropertyToTextList
Xutf8TextPropertyToTextList.restype = c_int
Xutf8TextPropertyToTextList.argtypes = [POINTER(Display), POINTER(XTextProperty), POINTER(POINTER(c_char_p)), POINTER(c_int)]

# /usr/include/X11/Xutil.h:5537
XUnionRectWithRegion = _lib.XUnionRectWithRegion
XUnionRectWithRegion.restype = c_int
XUnionRectWithRegion.argtypes = [POINTER(XRectangle), Region, Region]

# /usr/include/X11/Xutil.h:5543
XUnionRegion = _lib.XUnionRegion
XUnionRegion.restype = c_int
XUnionRegion.argtypes = [Region, Region, Region]

# /usr/include/X11/Xutil.h:5549
XWMGeometry = _lib.XWMGeometry
XWMGeometry.restype = c_int
XWMGeometry.argtypes = [POINTER(Display), c_int, c_char_p, c_char_p, c_uint, POINTER(XSizeHints), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/Xutil.h:5563
XXorRegion = _lib.XXorRegion
XXorRegion.restype = c_int
XXorRegion.argtypes = [Region, Region, Region]


__all__ = ['XlibSpecificationRelease', 'X_PROTOCOL', 'X_PROTOCOL_REVISION',
'XID', 'Mask', 'Atom', 'VisualID', 'Time', 'Window', 'Drawable', 'Font',
'Pixmap', 'Cursor', 'Colormap', 'GContext', 'KeySym', 'KeyCode', 'None_',
'ParentRelative', 'CopyFromParent', 'PointerWindow', 'InputFocus',
'PointerRoot', 'AnyPropertyType', 'AnyKey', 'AnyButton', 'AllTemporary',
'CurrentTime', 'NoSymbol', 'NoEventMask', 'KeyPressMask', 'KeyReleaseMask',
'ButtonPressMask', 'ButtonReleaseMask', 'EnterWindowMask', 'LeaveWindowMask',
'PointerMotionMask', 'PointerMotionHintMask', 'Button1MotionMask',
'Button2MotionMask', 'Button3MotionMask', 'Button4MotionMask',
'Button5MotionMask', 'ButtonMotionMask', 'KeymapStateMask', 'ExposureMask',
'VisibilityChangeMask', 'StructureNotifyMask', 'ResizeRedirectMask',
'SubstructureNotifyMask', 'SubstructureRedirectMask', 'FocusChangeMask',
'PropertyChangeMask', 'ColormapChangeMask', 'OwnerGrabButtonMask', 'KeyPress',
'KeyRelease', 'ButtonPress', 'ButtonRelease', 'MotionNotify', 'EnterNotify',
'LeaveNotify', 'FocusIn', 'FocusOut', 'KeymapNotify', 'Expose',
'GraphicsExpose', 'NoExpose', 'VisibilityNotify', 'CreateNotify',
'DestroyNotify', 'UnmapNotify', 'MapNotify', 'MapRequest', 'ReparentNotify',
'ConfigureNotify', 'ConfigureRequest', 'GravityNotify', 'ResizeRequest',
'CirculateNotify', 'CirculateRequest', 'PropertyNotify', 'SelectionClear',
'SelectionRequest', 'SelectionNotify', 'ColormapNotify', 'ClientMessage',
'MappingNotify', 'GenericEvent', 'LASTEvent', 'ShiftMask', 'LockMask',
'ControlMask', 'Mod1Mask', 'Mod2Mask', 'Mod3Mask', 'Mod4Mask', 'Mod5Mask',
'ShiftMapIndex', 'LockMapIndex', 'ControlMapIndex', 'Mod1MapIndex',
'Mod2MapIndex', 'Mod3MapIndex', 'Mod4MapIndex', 'Mod5MapIndex', 'Button1Mask',
'Button2Mask', 'Button3Mask', 'Button4Mask', 'Button5Mask', 'AnyModifier',
'Button1', 'Button2', 'Button3', 'Button4', 'Button5', 'NotifyNormal',
'NotifyGrab', 'NotifyUngrab', 'NotifyWhileGrabbed', 'NotifyHint',
'NotifyAncestor', 'NotifyVirtual', 'NotifyInferior', 'NotifyNonlinear',
'NotifyNonlinearVirtual', 'NotifyPointer', 'NotifyPointerRoot',
'NotifyDetailNone', 'VisibilityUnobscured', 'VisibilityPartiallyObscured',
'VisibilityFullyObscured', 'PlaceOnTop', 'PlaceOnBottom', 'FamilyInternet',
'FamilyDECnet', 'FamilyChaos', 'FamilyInternet6', 'FamilyServerInterpreted',
'PropertyNewValue', 'PropertyDelete', 'ColormapUninstalled',
'ColormapInstalled', 'GrabModeSync', 'GrabModeAsync', 'GrabSuccess',
'AlreadyGrabbed', 'GrabInvalidTime', 'GrabNotViewable', 'GrabFrozen',
'AsyncPointer', 'SyncPointer', 'ReplayPointer', 'AsyncKeyboard',
'SyncKeyboard', 'ReplayKeyboard', 'AsyncBoth', 'SyncBoth', 'RevertToParent',
'Success', 'BadRequest', 'BadValue', 'BadWindow', 'BadPixmap', 'BadAtom',
'BadCursor', 'BadFont', 'BadMatch', 'BadDrawable', 'BadAccess', 'BadAlloc',
'BadColor', 'BadGC', 'BadIDChoice', 'BadName', 'BadLength',
'BadImplementation', 'FirstExtensionError', 'LastExtensionError',
'InputOutput', 'InputOnly', 'CWBackPixmap', 'CWBackPixel', 'CWBorderPixmap',
'CWBorderPixel', 'CWBitGravity', 'CWWinGravity', 'CWBackingStore',
'CWBackingPlanes', 'CWBackingPixel', 'CWOverrideRedirect', 'CWSaveUnder',
'CWEventMask', 'CWDontPropagate', 'CWColormap', 'CWCursor', 'CWX', 'CWY',
'CWWidth', 'CWHeight', 'CWBorderWidth', 'CWSibling', 'CWStackMode',
'ForgetGravity', 'NorthWestGravity', 'NorthGravity', 'NorthEastGravity',
'WestGravity', 'CenterGravity', 'EastGravity', 'SouthWestGravity',
'SouthGravity', 'SouthEastGravity', 'StaticGravity', 'UnmapGravity',
'NotUseful', 'WhenMapped', 'Always', 'IsUnmapped', 'IsUnviewable',
'IsViewable', 'SetModeInsert', 'SetModeDelete', 'DestroyAll',
'RetainPermanent', 'RetainTemporary', 'Above', 'Below', 'TopIf', 'BottomIf',
'Opposite', 'RaiseLowest', 'LowerHighest', 'PropModeReplace',
'PropModePrepend', 'PropModeAppend', 'GXclear', 'GXand', 'GXandReverse',
'GXcopy', 'GXandInverted', 'GXnoop', 'GXxor', 'GXor', 'GXnor', 'GXequiv',
'GXinvert', 'GXorReverse', 'GXcopyInverted', 'GXorInverted', 'GXnand',
'GXset', 'LineSolid', 'LineOnOffDash', 'LineDoubleDash', 'CapNotLast',
'CapButt', 'CapRound', 'CapProjecting', 'JoinMiter', 'JoinRound', 'JoinBevel',
'FillSolid', 'FillTiled', 'FillStippled', 'FillOpaqueStippled', 'EvenOddRule',
'WindingRule', 'ClipByChildren', 'IncludeInferiors', 'Unsorted', 'YSorted',
'YXSorted', 'YXBanded', 'CoordModeOrigin', 'CoordModePrevious', 'Complex',
'Nonconvex', 'Convex', 'ArcChord', 'ArcPieSlice', 'GCFunction', 'GCPlaneMask',
'GCForeground', 'GCBackground', 'GCLineWidth', 'GCLineStyle', 'GCCapStyle',
'GCJoinStyle', 'GCFillStyle', 'GCFillRule', 'GCTile', 'GCStipple',
'GCTileStipXOrigin', 'GCTileStipYOrigin', 'GCFont', 'GCSubwindowMode',
'GCGraphicsExposures', 'GCClipXOrigin', 'GCClipYOrigin', 'GCClipMask',
'GCDashOffset', 'GCDashList', 'GCArcMode', 'GCLastBit', 'FontLeftToRight',
'FontRightToLeft', 'FontChange', 'XYBitmap', 'XYPixmap', 'ZPixmap',
'AllocNone', 'AllocAll', 'DoRed', 'DoGreen', 'DoBlue', 'CursorShape',
'TileShape', 'StippleShape', 'AutoRepeatModeOff', 'AutoRepeatModeOn',
'AutoRepeatModeDefault', 'LedModeOff', 'LedModeOn', 'KBKeyClickPercent',
'KBBellPercent', 'KBBellPitch', 'KBBellDuration', 'KBLed', 'KBLedMode',
'KBKey', 'KBAutoRepeatMode', 'MappingSuccess', 'MappingBusy', 'MappingFailed',
'MappingModifier', 'MappingKeyboard', 'MappingPointer', 'DontPreferBlanking',
'PreferBlanking', 'DefaultBlanking', 'DisableScreenSaver',
'DisableScreenInterval', 'DontAllowExposures', 'AllowExposures',
'DefaultExposures', 'ScreenSaverReset', 'ScreenSaverActive', 'HostInsert',
'HostDelete', 'EnableAccess', 'DisableAccess', 'StaticGray', 'GrayScale',
'StaticColor', 'PseudoColor', 'TrueColor', 'DirectColor', 'LSBFirst',
'MSBFirst', '_Xmblen', 'X_HAVE_UTF8_STRING', 'XPointer', 'Bool', 'Status',
'True_', 'False_', 'QueuedAlready', 'QueuedAfterReading', 'QueuedAfterFlush',
'XExtData', 'XExtCodes', 'XPixmapFormatValues', 'XGCValues', 'GC', 'Visual',
'Depth', 'Screen', 'ScreenFormat', 'XSetWindowAttributes',
'XWindowAttributes', 'XHostAddress', 'XServerInterpretedAddress', 'XImage',
'XWindowChanges', 'XColor', 'XSegment', 'XPoint', 'XRectangle', 'XArc',
'XKeyboardControl', 'XKeyboardState', 'XTimeCoord', 'XModifierKeymap',
'Display', '_XPrivDisplay', 'XKeyEvent', 'XKeyPressedEvent',
'XKeyReleasedEvent', 'XButtonEvent', 'XButtonPressedEvent',
'XButtonReleasedEvent', 'XMotionEvent', 'XPointerMovedEvent',
'XCrossingEvent', 'XEnterWindowEvent', 'XLeaveWindowEvent',
'XFocusChangeEvent', 'XFocusInEvent', 'XFocusOutEvent', 'XKeymapEvent',
'XExposeEvent', 'XGraphicsExposeEvent', 'XNoExposeEvent', 'XVisibilityEvent',
'XCreateWindowEvent', 'XDestroyWindowEvent', 'XUnmapEvent', 'XMapEvent',
'XMapRequestEvent', 'XReparentEvent', 'XConfigureEvent', 'XGravityEvent',
'XResizeRequestEvent', 'XConfigureRequestEvent', 'XCirculateEvent',
'XCirculateRequestEvent', 'XPropertyEvent', 'XSelectionClearEvent',
'XSelectionRequestEvent', 'XSelectionEvent', 'XColormapEvent',
'XClientMessageEvent', 'XMappingEvent', 'XErrorEvent', 'XAnyEvent',
'XGenericEvent', 'XGenericEventCookie', 'XEvent', 'XCharStruct', 'XFontProp',
'XFontStruct', 'XTextItem', 'XChar2b', 'XTextItem16', 'XEDataObject',
'XFontSetExtents', 'XOM', 'XOC', 'XFontSet', 'XmbTextItem', 'XwcTextItem',
'XOMCharSetList', 'XOrientation', 'XOMOrientation_LTR_TTB',
'XOMOrientation_RTL_TTB', 'XOMOrientation_TTB_LTR', 'XOMOrientation_TTB_RTL',
'XOMOrientation_Context', 'XOMOrientation', 'XOMFontInfo', 'XIM', 'XIC',
'XIMProc', 'XICProc', 'XIDProc', 'XIMStyle', 'XIMStyles', 'XIMPreeditArea',
'XIMPreeditCallbacks', 'XIMPreeditPosition', 'XIMPreeditNothing',
'XIMPreeditNone', 'XIMStatusArea', 'XIMStatusCallbacks', 'XIMStatusNothing',
'XIMStatusNone', 'XBufferOverflow', 'XLookupNone', 'XLookupChars',
'XLookupKeySym', 'XLookupBoth', 'XVaNestedList', 'XIMCallback', 'XICCallback',
'XIMFeedback', 'XIMReverse', 'XIMUnderline', 'XIMHighlight', 'XIMPrimary',
'XIMSecondary', 'XIMTertiary', 'XIMVisibleToForward', 'XIMVisibleToBackword',
'XIMVisibleToCenter', 'XIMText', 'XIMPreeditState', 'XIMPreeditUnKnown',
'XIMPreeditEnable', 'XIMPreeditDisable',
'XIMPreeditStateNotifyCallbackStruct', 'XIMResetState', 'XIMInitialState',
'XIMPreserveState', 'XIMStringConversionFeedback',
'XIMStringConversionLeftEdge', 'XIMStringConversionRightEdge',
'XIMStringConversionTopEdge', 'XIMStringConversionBottomEdge',
'XIMStringConversionConcealed', 'XIMStringConversionWrapped',
'XIMStringConversionText', 'XIMStringConversionPosition',
'XIMStringConversionType', 'XIMStringConversionBuffer',
'XIMStringConversionLine', 'XIMStringConversionWord',
'XIMStringConversionChar', 'XIMStringConversionOperation',
'XIMStringConversionSubstitution', 'XIMStringConversionRetrieval',
'XIMCaretDirection', 'XIMForwardChar', 'XIMBackwardChar', 'XIMForwardWord',
'XIMBackwardWord', 'XIMCaretUp', 'XIMCaretDown', 'XIMNextLine',
'XIMPreviousLine', 'XIMLineStart', 'XIMLineEnd', 'XIMAbsolutePosition',
'XIMDontChange', 'XIMStringConversionCallbackStruct',
'XIMPreeditDrawCallbackStruct', 'XIMCaretStyle', 'XIMIsInvisible',
'XIMIsPrimary', 'XIMIsSecondary', 'XIMPreeditCaretCallbackStruct',
'XIMStatusDataType', 'XIMTextType', 'XIMBitmapType',
'XIMStatusDrawCallbackStruct', 'XIMHotKeyTrigger', 'XIMHotKeyTriggers',
'XIMHotKeyState', 'XIMHotKeyStateON', 'XIMHotKeyStateOFF', 'XIMValuesList',
'XLoadQueryFont', 'XQueryFont', 'XGetMotionEvents', 'XDeleteModifiermapEntry',
'XGetModifierMapping', 'XInsertModifiermapEntry', 'XNewModifiermap',
'XCreateImage', 'XInitImage', 'XGetImage', 'XGetSubImage', 'XOpenDisplay',
'XrmInitialize', 'XFetchBytes', 'XFetchBuffer', 'XGetAtomName',
'XGetAtomNames', 'XGetDefault', 'XDisplayName', 'XKeysymToString',
'XSynchronize', 'XSetAfterFunction', 'XInternAtom', 'XInternAtoms',
'XCopyColormapAndFree', 'XCreateColormap', 'XCreatePixmapCursor',
'XCreateGlyphCursor', 'XCreateFontCursor', 'XLoadFont', 'XCreateGC',
'XGContextFromGC', 'XFlushGC', 'XCreatePixmap', 'XCreateBitmapFromData',
'XCreatePixmapFromBitmapData', 'XCreateSimpleWindow', 'XGetSelectionOwner',
'XCreateWindow', 'XListInstalledColormaps', 'XListFonts',
'XListFontsWithInfo', 'XGetFontPath', 'XListExtensions', 'XListProperties',
'XListHosts', 'XKeycodeToKeysym', 'XLookupKeysym', 'XGetKeyboardMapping',
'XStringToKeysym', 'XMaxRequestSize', 'XExtendedMaxRequestSize',
'XResourceManagerString', 'XScreenResourceString', 'XDisplayMotionBufferSize',
'XVisualIDFromVisual', 'XInitThreads', 'XLockDisplay', 'XUnlockDisplay',
'XInitExtension', 'XAddExtension', 'XFindOnExtensionList',
'XEHeadOfExtensionList', 'XRootWindow', 'XDefaultRootWindow',
'XRootWindowOfScreen', 'XDefaultVisual', 'XDefaultVisualOfScreen',
'XDefaultGC', 'XDefaultGCOfScreen', 'XBlackPixel', 'XWhitePixel',
'XAllPlanes', 'XBlackPixelOfScreen', 'XWhitePixelOfScreen', 'XNextRequest',
'XLastKnownRequestProcessed', 'XServerVendor', 'XDisplayString',
'XDefaultColormap', 'XDefaultColormapOfScreen', 'XDisplayOfScreen',
'XScreenOfDisplay', 'XDefaultScreenOfDisplay', 'XEventMaskOfScreen',
'XScreenNumberOfScreen', 'XErrorHandler', 'XSetErrorHandler',
'XIOErrorHandler', 'XSetIOErrorHandler', 'XListPixmapFormats', 'XListDepths',
'XReconfigureWMWindow', 'XGetWMProtocols', 'XSetWMProtocols',
'XIconifyWindow', 'XWithdrawWindow', 'XGetCommand', 'XGetWMColormapWindows',
'XSetWMColormapWindows', 'XFreeStringList', 'XSetTransientForHint',
'XActivateScreenSaver', 'XAddHost', 'XAddHosts', 'XAddToExtensionList',
'XAddToSaveSet', 'XAllocColor', 'XAllocColorCells', 'XAllocColorPlanes',
'XAllocNamedColor', 'XAllowEvents', 'XAutoRepeatOff', 'XAutoRepeatOn',
'XBell', 'XBitmapBitOrder', 'XBitmapPad', 'XBitmapUnit', 'XCellsOfScreen',
'XChangeActivePointerGrab', 'XChangeGC', 'XChangeKeyboardControl',
'XChangeKeyboardMapping', 'XChangePointerControl', 'XChangeProperty',
'XChangeSaveSet', 'XChangeWindowAttributes', 'XCheckIfEvent',
'XCheckMaskEvent', 'XCheckTypedEvent', 'XCheckTypedWindowEvent',
'XCheckWindowEvent', 'XCirculateSubwindows', 'XCirculateSubwindowsDown',
'XCirculateSubwindowsUp', 'XClearArea', 'XClearWindow', 'XCloseDisplay',
'XConfigureWindow', 'XConnectionNumber', 'XConvertSelection', 'XCopyArea',
'XCopyGC', 'XCopyPlane', 'XDefaultDepth', 'XDefaultDepthOfScreen',
'XDefaultScreen', 'XDefineCursor', 'XDeleteProperty', 'XDestroyWindow',
'XDestroySubwindows', 'XDoesBackingStore', 'XDoesSaveUnders',
'XDisableAccessControl', 'XDisplayCells', 'XDisplayHeight',
'XDisplayHeightMM', 'XDisplayKeycodes', 'XDisplayPlanes', 'XDisplayWidth',
'XDisplayWidthMM', 'XDrawArc', 'XDrawArcs', 'XDrawImageString',
'XDrawImageString16', 'XDrawLine', 'XDrawLines', 'XDrawPoint', 'XDrawPoints',
'XDrawRectangle', 'XDrawRectangles', 'XDrawSegments', 'XDrawString',
'XDrawString16', 'XDrawText', 'XDrawText16', 'XEnableAccessControl',
'XEventsQueued', 'XFetchName', 'XFillArc', 'XFillArcs', 'XFillPolygon',
'XFillRectangle', 'XFillRectangles', 'XFlush', 'XForceScreenSaver', 'XFree',
'XFreeColormap', 'XFreeColors', 'XFreeCursor', 'XFreeExtensionList',
'XFreeFont', 'XFreeFontInfo', 'XFreeFontNames', 'XFreeFontPath', 'XFreeGC',
'XFreeModifiermap', 'XFreePixmap', 'XGeometry', 'XGetErrorDatabaseText',
'XGetErrorText', 'XGetFontProperty', 'XGetGCValues', 'XGetGeometry',
'XGetIconName', 'XGetInputFocus', 'XGetKeyboardControl', 'XGetPointerControl',
'XGetPointerMapping', 'XGetScreenSaver', 'XGetTransientForHint',
'XGetWindowProperty', 'XGetWindowAttributes', 'XGrabButton', 'XGrabKey',
'XGrabKeyboard', 'XGrabPointer', 'XGrabServer', 'XHeightMMOfScreen',
'XHeightOfScreen', 'XIfEvent', 'XImageByteOrder', 'XInstallColormap',
'XKeysymToKeycode', 'XKillClient', 'XLookupColor', 'XLowerWindow',
'XMapRaised', 'XMapSubwindows', 'XMapWindow', 'XMaskEvent',
'XMaxCmapsOfScreen', 'XMinCmapsOfScreen', 'XMoveResizeWindow', 'XMoveWindow',
'XNextEvent', 'XNoOp', 'XParseColor', 'XParseGeometry', 'XPeekEvent',
'XPeekIfEvent', 'XPending', 'XPlanesOfScreen', 'XProtocolRevision',
'XProtocolVersion', 'XPutBackEvent', 'XPutImage', 'XQLength',
'XQueryBestCursor', 'XQueryBestSize', 'XQueryBestStipple', 'XQueryBestTile',
'XQueryColor', 'XQueryColors', 'XQueryExtension', 'XQueryKeymap',
'XQueryPointer', 'XQueryTextExtents', 'XQueryTextExtents16', 'XQueryTree',
'XRaiseWindow', 'XReadBitmapFile', 'XReadBitmapFileData', 'XRebindKeysym',
'XRecolorCursor', 'XRefreshKeyboardMapping', 'XRemoveFromSaveSet',
'XRemoveHost', 'XRemoveHosts', 'XReparentWindow', 'XResetScreenSaver',
'XResizeWindow', 'XRestackWindows', 'XRotateBuffers',
'XRotateWindowProperties', 'XScreenCount', 'XSelectInput', 'XSendEvent',
'XSetAccessControl', 'XSetArcMode', 'XSetBackground', 'XSetClipMask',
'XSetClipOrigin', 'XSetClipRectangles', 'XSetCloseDownMode', 'XSetCommand',
'XSetDashes', 'XSetFillRule', 'XSetFillStyle', 'XSetFont', 'XSetFontPath',
'XSetForeground', 'XSetFunction', 'XSetGraphicsExposures', 'XSetIconName',
'XSetInputFocus', 'XSetLineAttributes', 'XSetModifierMapping',
'XSetPlaneMask', 'XSetPointerMapping', 'XSetScreenSaver',
'XSetSelectionOwner', 'XSetState', 'XSetStipple', 'XSetSubwindowMode',
'XSetTSOrigin', 'XSetTile', 'XSetWindowBackground',
'XSetWindowBackgroundPixmap', 'XSetWindowBorder', 'XSetWindowBorderPixmap',
'XSetWindowBorderWidth', 'XSetWindowColormap', 'XStoreBuffer', 'XStoreBytes',
'XStoreColor', 'XStoreColors', 'XStoreName', 'XStoreNamedColor', 'XSync',
'XTextExtents', 'XTextExtents16', 'XTextWidth', 'XTextWidth16',
'XTranslateCoordinates', 'XUndefineCursor', 'XUngrabButton', 'XUngrabKey',
'XUngrabKeyboard', 'XUngrabPointer', 'XUngrabServer', 'XUninstallColormap',
'XUnloadFont', 'XUnmapSubwindows', 'XUnmapWindow', 'XVendorRelease',
'XWarpPointer', 'XWidthMMOfScreen', 'XWidthOfScreen', 'XWindowEvent',
'XWriteBitmapFile', 'XSupportsLocale', 'XSetLocaleModifiers', 'XOpenOM',
'XCloseOM', 'XSetOMValues', 'XGetOMValues', 'XDisplayOfOM', 'XLocaleOfOM',
'XCreateOC', 'XDestroyOC', 'XOMOfOC', 'XSetOCValues', 'XGetOCValues',
'XCreateFontSet', 'XFreeFontSet', 'XFontsOfFontSet',
'XBaseFontNameListOfFontSet', 'XLocaleOfFontSet', 'XContextDependentDrawing',
'XDirectionalDependentDrawing', 'XContextualDrawing', 'XExtentsOfFontSet',
'XmbTextEscapement', 'XwcTextEscapement', 'Xutf8TextEscapement',
'XmbTextExtents', 'XwcTextExtents', 'Xutf8TextExtents',
'XmbTextPerCharExtents', 'XwcTextPerCharExtents', 'Xutf8TextPerCharExtents',
'XmbDrawText', 'XwcDrawText', 'Xutf8DrawText', 'XmbDrawString',
'XwcDrawString', 'Xutf8DrawString', 'XmbDrawImageString',
'XwcDrawImageString', 'Xutf8DrawImageString', 'XOpenIM', 'XCloseIM',
'XGetIMValues', 'XSetIMValues', 'XDisplayOfIM', 'XLocaleOfIM', 'XCreateIC',
'XDestroyIC', 'XSetICFocus', 'XUnsetICFocus', 'XwcResetIC', 'XmbResetIC',
'Xutf8ResetIC', 'XSetICValues', 'XGetICValues', 'XIMOfIC', 'XFilterEvent',
'XmbLookupString', 'XwcLookupString', 'Xutf8LookupString',
'XVaCreateNestedList', 'XRegisterIMInstantiateCallback',
'XUnregisterIMInstantiateCallback', 'XConnectionWatchProc',
'XInternalConnectionNumbers', 'XProcessInternalConnection',
'XAddConnectionWatch', 'XRemoveConnectionWatch', 'XSetAuthorization',
'_Xmbtowc', '_Xwctomb', 'XGetEventData', 'XFreeEventData', 'NoValue',
'XValue', 'YValue', 'WidthValue', 'HeightValue', 'AllValues', 'XNegative',
'YNegative', 'XSizeHints', 'USPosition', 'USSize', 'PPosition', 'PSize',
'PMinSize', 'PMaxSize', 'PResizeInc', 'PAspect', 'PBaseSize', 'PWinGravity',
'PAllHints', 'XWMHints', 'InputHint', 'StateHint', 'IconPixmapHint',
'IconWindowHint', 'IconPositionHint', 'IconMaskHint', 'WindowGroupHint',
'AllHints', 'XUrgencyHint', 'WithdrawnState', 'NormalState', 'IconicState',
'DontCareState', 'ZoomState', 'InactiveState', 'XTextProperty', 'XNoMemory',
'XLocaleNotSupported', 'XConverterNotFound', 'XICCEncodingStyle',
'XStringStyle', 'XCompoundTextStyle', 'XTextStyle', 'XStdICCTextStyle',
'XUTF8StringStyle', 'XIconSize', 'XClassHint', 'XComposeStatus', 'Region',
'RectangleOut', 'RectangleIn', 'RectanglePart', 'XVisualInfo', 'VisualNoMask',
'VisualIDMask', 'VisualScreenMask', 'VisualDepthMask', 'VisualClassMask',
'VisualRedMaskMask', 'VisualGreenMaskMask', 'VisualBlueMaskMask',
'VisualColormapSizeMask', 'VisualBitsPerRGBMask', 'VisualAllMask',
'XStandardColormap', 'BitmapSuccess', 'BitmapOpenFailed', 'BitmapFileInvalid',
'BitmapNoMemory', 'XCSUCCESS', 'XCNOMEM', 'XCNOENT', 'XContext',
'XAllocClassHint', 'XAllocIconSize', 'XAllocSizeHints',
'XAllocStandardColormap', 'XAllocWMHints', 'XClipBox', 'XCreateRegion',
'XDefaultString', 'XDeleteContext', 'XDestroyRegion', 'XEmptyRegion',
'XEqualRegion', 'XFindContext', 'XGetClassHint', 'XGetIconSizes',
'XGetNormalHints', 'XGetRGBColormaps', 'XGetSizeHints',
'XGetStandardColormap', 'XGetTextProperty', 'XGetVisualInfo',
'XGetWMClientMachine', 'XGetWMHints', 'XGetWMIconName', 'XGetWMName',
'XGetWMNormalHints', 'XGetWMSizeHints', 'XGetZoomHints', 'XIntersectRegion',
'XConvertCase', 'XLookupString', 'XMatchVisualInfo', 'XOffsetRegion',
'XPointInRegion', 'XPolygonRegion', 'XRectInRegion', 'XSaveContext',
'XSetClassHint', 'XSetIconSizes', 'XSetNormalHints', 'XSetRGBColormaps',
'XSetSizeHints', 'XSetStandardProperties', 'XSetTextProperty',
'XSetWMClientMachine', 'XSetWMHints', 'XSetWMIconName', 'XSetWMName',
'XSetWMNormalHints', 'XSetWMProperties', 'XmbSetWMProperties',
'Xutf8SetWMProperties', 'XSetWMSizeHints', 'XSetRegion',
'XSetStandardColormap', 'XSetZoomHints', 'XShrinkRegion',
'XStringListToTextProperty', 'XSubtractRegion', 'XmbTextListToTextProperty',
'XwcTextListToTextProperty', 'Xutf8TextListToTextProperty',
'XwcFreeStringList', 'XTextPropertyToStringList', 'XmbTextPropertyToTextList',
'XwcTextPropertyToTextList', 'Xutf8TextPropertyToTextList',
'XUnionRectWithRegion', 'XUnionRegion', 'XWMGeometry', 'XXorRegion']
