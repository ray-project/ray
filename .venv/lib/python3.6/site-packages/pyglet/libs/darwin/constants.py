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

'''
'''

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

# CFString.h
kCFStringEncodingMacRoman = 0
kCFStringEncodingWindowsLatin1 = 0x0500
kCFStringEncodingISOLatin1 = 0x0201
kCFStringEncodingNextStepLatin = 0x0B01
kCFStringEncodingASCII = 0x0600
kCFStringEncodingUnicode = 0x0100
kCFStringEncodingUTF8 = 0x08000100
kCFStringEncodingNonLossyASCII = 0x0BFF

# MacTypes.h
noErr = 0

# CarbonEventsCore.h
eventLoopTimedOutErr = -9875
eventLoopQuitErr = -9876
kEventPriorityStandard = 1

# MacApplication.h
kUIModeNormal                 = 0
kUIModeContentSuppressed      = 1
kUIModeContentHidden          = 2
kUIModeAllSuppressed          = 4
kUIModeAllHidden              = 3
kUIOptionAutoShowMenuBar      = 1 << 0
kUIOptionDisableAppleMenu     = 1 << 2
kUIOptionDisableProcessSwitch = 1 << 3
kUIOptionDisableForceQuit     = 1 << 4
kUIOptionDisableSessionTerminate = 1 << 5
kUIOptionDisableHide          = 1 << 6

# MacWindows.h
kAlertWindowClass = 1
kMovableAlertWindowClass = 2
kModalWindowClass = 3
kMovableModalWindowClass = 4
kFloatingWindowClass = 5
kDocumentWindowClass = 6
kUtilityWindowClass = 8
kHelpWindowClass = 10
kSheetWindowClass = 11
kToolbarWindowClass = 12
kPlainWindowClass = 13
kOverlayWindowClass = 14
kSheetAlertWindowClass = 15
kAltPlainWindowClass = 16
kSimpleWindowClass = 18  # no window frame
kDrawerWindowClass = 20

kWindowNoAttributes = 0x0
kWindowCloseBoxAttribute = 0x1
kWindowHorizontalZoomAttribute = 0x2
kWindowVerticalZoomAttribute = 0x4
kWindowFullZoomAttribute = kWindowHorizontalZoomAttribute | \
    kWindowVerticalZoomAttribute
kWindowCollapseBoxAttribute = 0x8
kWindowResizableAttribute = 0x10 
kWindowSideTitlebarAttribute = 0x20
kWindowToolbarAttribute = 0x40
kWindowMetalAttribute = 1 << 8
kWindowDoesNotCycleAttribute = 1 << 15
kWindowNoupdatesAttribute = 1 << 16
kWindowNoActivatesAttribute = 1 << 17
kWindowOpaqueForEventsAttribute = 1 << 18
kWindowCompositingAttribute = 1 << 19
kWindowNoShadowAttribute = 1 << 21
kWindowHideOnSuspendAttribute = 1 << 24
kWindowAsyncDragAttribute = 1 << 23
kWindowStandardHandlerAttribute = 1 << 25
kWindowHideOnFullScreenAttribute = 1 << 26
kWindowInWindowMenuAttribute = 1 << 27
kWindowLiveResizeAttribute = 1 << 28
kWindowIgnoreClicksAttribute = 1 << 29
kWindowNoConstrainAttribute = 1 << 31
kWindowStandardDocumentAttributes = kWindowCloseBoxAttribute | \
                                    kWindowFullZoomAttribute | \
                                    kWindowCollapseBoxAttribute | \
                                    kWindowResizableAttribute
kWindowStandardFloatingAttributes = kWindowCloseBoxAttribute | \
                                    kWindowCollapseBoxAttribute

kWindowCenterOnMainScreen = 1
kWindowCenterOnParentWindow = 2
kWindowCenterOnParentWindowScreen = 3
kWindowCascadeOnMainScreen = 4
kWindowCascadeOnParentWindow = 5
kWindowCascadeonParentWindowScreen = 6
kWindowCascadeStartAtParentWindowScreen = 10
kWindowAlertPositionOnMainScreen = 7
kWindowAlertPositionOnParentWindow = 8
kWindowAlertPositionOnParentWindowScreen = 9


kWindowTitleBarRgn            = 0
kWindowTitleTextRgn           = 1
kWindowCloseBoxRgn            = 2
kWindowZoomBoxRgn             = 3
kWindowDragRgn                = 5
kWindowGrowRgn                = 6
kWindowCollapseBoxRgn         = 7
kWindowTitleProxyIconRgn      = 8
kWindowStructureRgn           = 32
kWindowContentRgn             = 33
kWindowUpdateRgn              = 34
kWindowOpaqueRgn              = 35
kWindowGlobalPortRgn          = 40
kWindowToolbarButtonRgn       = 41

inDesk                        = 0
inNoWindow                    = 0
inMenuBar                     = 1
inSysWindow                   = 2
inContent                     = 3
inDrag                        = 4
inGrow                        = 5
inGoAway                      = 6
inZoomIn                      = 7
inZoomOut                     = 8
inCollapseBox                 = 11
inProxyIcon                   = 12
inToolbarButton               = 13
inStructure                   = 15 

def _name(name):
    return ord(name[0]) << 24 | \
           ord(name[1]) << 16 | \
           ord(name[2]) << 8 | \
           ord(name[3])

# AEDataModel.h

typeBoolean = _name('bool')
typeChar = _name('TEXT')
typeSInt16 = _name('shor')
typeSInt32 = _name('long')
typeUInt32 = _name('magn')
typeSInt64 = _name('comp')
typeIEEE32BitFloatingPoint = _name('sing')
typeIEEE64BitFloatingPoint = _name('doub')
type128BitFloatingPoint = _name('ldbl')
typeDecimalStruct = _name('decm')

# AERegistry.h
typeUnicodeText = _name('utxt')
typeStyledUnicodeText = _name('sutx')
typeUTF8Text = _name('utf8')
typeEncodedString = _name('encs')
typeCString = _name('cstr')
typePString = _name('pstr')
typeEventRef = _name('evrf')

# CarbonEvents.h

kEventParamWindowRef          = _name('wind')
kEventParamWindowPartCode     = _name('wpar')
kEventParamGrafPort           = _name('graf')
kEventParamMenuRef            = _name('menu')
kEventParamEventRef           = _name('evnt')
kEventParamControlRef         = _name('ctrl')
kEventParamRgnHandle          = _name('rgnh')
kEventParamEnabled            = _name('enab')
kEventParamDimensions         = _name('dims')
kEventParamBounds             = _name('boun')
kEventParamAvailableBounds    = _name('avlb')
#kEventParamAEEventID          = keyAEEventID
#kEventParamAEEventClass       = keyAEEventClass
kEventParamCGContextRef       = _name('cntx')
kEventParamDeviceDepth        = _name('devd')
kEventParamDeviceColor        = _name('devc')
kEventParamMutableArray       = _name('marr')
kEventParamResult             = _name('ansr')
kEventParamMinimumSize        = _name('mnsz')
kEventParamMaximumSize        = _name('mxsz')
kEventParamAttributes         = _name('attr')
kEventParamReason             = _name('why?')
kEventParamTransactionID      = _name('trns')
kEventParamGDevice            = _name('gdev')
kEventParamIndex              = _name('indx')
kEventParamUserData           = _name('usrd')
kEventParamShape              = _name('shap')
typeWindowRef                 = _name('wind')
typeWindowPartCode            = _name('wpar')
typeGrafPtr                   = _name('graf')
typeGWorldPtr                 = _name('gwld')
typeMenuRef                   = _name('menu')
typeControlRef                = _name('ctrl')
typeCollection                = _name('cltn')
typeQDRgnHandle               = _name('rgnh')
typeOSStatus                  = _name('osst')
typeCFIndex                   = _name('cfix')
typeCGContextRef              = _name('cntx')
typeQDPoint                   = _name('QDpt')
typeHICommand                 = _name('hcmd')
typeHIPoint                   = _name('hipt')
typeHISize                    = _name('hisz')
typeHIRect                    = _name('hirc')
typeHIShapeRef                = _name('shap')
typeVoidPtr                   = _name('void')
typeGDHandle                  = _name('gdev') 

kCoreEventClass = _name('aevt')
kEventClassMouse = _name('mous')
kEventClassKeyboard = _name('keyb')
kEventClassTextInput = _name('text')
kEventClassApplication = _name('appl')
kEventClassAppleEvent = _name('eppc')
kEventClassMenu = _name('menu')
kEventClassWindow = _name('wind')
kEventClassControl = _name('cntl')
kEventClassCommand = _name('cmds')
kEventClassTablet = _name('tblt')
kEventClassVolume = _name('vol ')
kEventClassAppearance = _name('appm')
kEventClassService = _name('serv')
kEventClassToolbar = _name('tbar')
kEventClassToolbarItem = _name('tbit')
kEventClassToolbarItemView = _name('tbiv')
kEventClassAccessibility = _name('acce')
kEventClassSystem = _name('macs')
kEventClassInk = _name('ink ')
kEventClassTSMDocumentAccess = _name('tdac')

kEventDurationForever = -1.0

# Appearance.h
kThemeArrowCursor             = 0
kThemeCopyArrowCursor         = 1
kThemeAliasArrowCursor        = 2
kThemeContextualMenuArrowCursor = 3
kThemeIBeamCursor             = 4
kThemeCrossCursor             = 5
kThemePlusCursor              = 6
kThemeWatchCursor             = 7
kThemeClosedHandCursor        = 8
kThemeOpenHandCursor          = 9
kThemePointingHandCursor      = 10
kThemeCountingUpHandCursor    = 11
kThemeCountingDownHandCursor  = 12
kThemeCountingUpAndDownHandCursor = 13
kThemeSpinningCursor          = 14
kThemeResizeLeftCursor        = 15
kThemeResizeRightCursor       = 16
kThemeResizeLeftRightCursor   = 17
kThemeNotAllowedCursor        = 18
kThemeResizeUpCursor          = 19
kThemeResizeDownCursor        = 20
kThemeResizeUpDownCursor      = 21
kThemePoofCursor              = 22

# AE
kEventAppleEvent                = 1
kEventAppQuit                   = 3
kAEQuitApplication              = _name('quit')

# Commands
kEventProcessCommand            = 1
kEventParamHICommand            = _name('hcmd')
kEventParamDirectObject         = _name('----')
kHICommandQuit                  = _name('quit')

# Keyboard
kEventRawKeyDown                = 1
kEventRawKeyRepeat              = 2
kEventRawKeyUp                  = 3
kEventRawKeyModifiersChanged    = 4
kEventHotKeyPressed             = 5
kEventHotKeyReleased            = 6

kEventParamKeyCode = _name('kcod')
kEventParamKeyMacCharCodes = _name('kchr')
kEventParamKeyModifiers = _name('kmod')
kEventParamKeyUnicodes = _name('kuni')
kEventParamKeyboardType = _name('kbdt')
typeEventHotKeyID = _name('hkid')

activeFlagBit                 = 0
btnStateBit                   = 7
cmdKeyBit                     = 8
shiftKeyBit                   = 9
alphaLockBit                  = 10
optionKeyBit                  = 11
controlKeyBit                 = 12
rightShiftKeyBit              = 13
rightOptionKeyBit             = 14
rightControlKeyBit            = 15
numLockBit                    = 16

activeFlag                    = 1 << activeFlagBit
btnState                      = 1 << btnStateBit
cmdKey                        = 1 << cmdKeyBit
shiftKey                      = 1 << shiftKeyBit
alphaLock                     = 1 << alphaLockBit
optionKey                     = 1 << optionKeyBit
controlKey                    = 1 << controlKeyBit
rightShiftKey                 = 1 << rightShiftKeyBit
rightOptionKey                = 1 << rightOptionKeyBit
rightControlKey               = 1 << rightControlKeyBit
numLock                       = 1 << numLockBit

# TextInput
kEventTextInputUpdateActiveInputArea    = 1
kEventTextInputUnicodeForKeyEvent       = 2
kEventTextInputOffsetToPos              = 3
kEventTextInputPosToOffset              = 4
kEventTextInputShowHideBottomWindow     = 5
kEventTextInputGetSelectedText          = 6
kEventTextInputUnicodeText              = 7

kEventParamTextInputSendText = _name('tstx')
kEventParamTextInputSendKeyboardEvent = _name('tske')

# Mouse
kEventMouseDown                 = 1
kEventMouseUp                   = 2
kEventMouseMoved                = 5
kEventMouseDragged              = 6
kEventMouseEntered              = 8
kEventMouseExited               = 9
kEventMouseWheelMoved           = 10
kEventParamMouseLocation = _name('mloc')
kEventParamWindowMouseLocation = _name('wmou')
kEventParamMouseButton = _name('mbtn')
kEventParamClickCount = _name('ccnt')
kEventParamMouseWheelAxis = _name('mwax')
kEventParamMouseWheelDelta = _name('mwdl')
kEventParamMouseDelta = _name('mdta')
kEventParamMouseChord = _name('chor')
kEventParamTabletEventType = _name('tblt')
kEventParamMouseTrackingRef = _name('mtrf')
typeMouseButton         = _name('mbtn')
typeMouseWheelAxis      = _name('mwax')
typeMouseTrackingRef    = _name('mtrf')

kMouseTrackingOptionsLocalClip = 0
kMouseTrackingOptionsGlobalClip = 1

kEventMouseButtonPrimary = 1
kEventMouseButtonSecondary = 2
kEventMouseButtonTertiary = 3

kEventMouseWheelAxisX = 0
kEventMouseWheelAxisY = 1

DEFAULT_CREATOR_CODE = _name('PYGL')    # <ah> this is registered for Pyglet
                                        # apps.  register your own at:
                                        # http://developer.apple.com/datatype

# Window
kEventWindowUpdate                  = 1
kEventWindowDrawContent             = 2

# -- window activation events --

kEventWindowActivated               = 5
kEventWindowDeactivated             = 6
kEventWindowHandleActivate          = 91
kEventWindowHandleDeactivate        = 92
kEventWindowGetClickActivation      = 7
kEventWindowGetClickModality        = 8

# -- window state change events --

kEventWindowShowing                 = 22
kEventWindowHiding                  = 23
kEventWindowShown                   = 24
kEventWindowHidden                  = 25
kEventWindowCollapsing              = 86
kEventWindowCollapsed               = 67
kEventWindowExpanding               = 87
kEventWindowExpanded                = 70
kEventWindowZoomed                  = 76
kEventWindowBoundsChanging          = 26
kEventWindowBoundsChanged           = 27
kEventWindowResizeStarted           = 28
kEventWindowResizeCompleted         = 29
kEventWindowDragStarted             = 30
kEventWindowDragCompleted           = 31
kEventWindowClosed                  = 73
kEventWindowTransitionStarted       = 88
kEventWindowTransitionCompleted     = 89

# -- window click events --

kEventWindowClickDragRgn            = 32
kEventWindowClickResizeRgn          = 33
kEventWindowClickCollapseRgn        = 34
kEventWindowClickCloseRgn           = 35
kEventWindowClickZoomRgn            = 36
kEventWindowClickContentRgn         = 37
kEventWindowClickProxyIconRgn       = 38
kEventWindowClickToolbarButtonRgn   = 41
kEventWindowClickStructureRgn       = 42

# -- window cursor change events --

kEventWindowCursorChange            = 40

# -- window action events --

kEventWindowCollapse                = 66
kEventWindowCollapsed               = 67
kEventWindowCollapseAll             = 68
kEventWindowExpand                  = 69
kEventWindowExpanded                = 70
kEventWindowExpandAll               = 71
kEventWindowClose                   = 72
kEventWindowClosed                  = 73
kEventWindowCloseAll                = 74
kEventWindowZoom                    = 75
kEventWindowZoomed                  = 76
kEventWindowZoomAll                 = 77
kEventWindowContextualMenuSelect    = 78
kEventWindowPathSelect              = 79
kEventWindowGetIdealSize            = 80
kEventWindowGetMinimumSize          = 81
kEventWindowGetMaximumSize          = 82
kEventWindowConstrain               = 83
kEventWindowHandleContentClick      = 85
kEventWindowCollapsing              = 86
kEventWindowExpanding               = 87
kEventWindowTransitionStarted       = 88
kEventWindowTransitionCompleted     = 89
kEventWindowGetDockTileMenu         = 90
kEventWindowHandleActivate          = 91
kEventWindowHandleDeactivate        = 92
kEventWindowProxyBeginDrag          = 128
kEventWindowProxyEndDrag            = 129
kEventWindowToolbarSwitchMode       = 150

# -- window focus events --

kEventWindowFocusAcquired           = 200
kEventWindowFocusRelinquish         = 201
kEventWindowFocusContent            = 202
kEventWindowFocusToolbar            = 203
kEventWindowFocusDrawer             = 204

# -- sheet events --

kEventWindowSheetOpening            = 210
kEventWindowSheetOpened             = 211
kEventWindowSheetClosing            = 212
kEventWindowSheetClosed             = 213

# -- drawer events --

kEventWindowDrawerOpening           = 220
kEventWindowDrawerOpened            = 221
kEventWindowDrawerClosing           = 222
kEventWindowDrawerClosed            = 223

# -- window definition events --

kEventWindowDrawFrame               = 1000
kEventWindowDrawPart                = 1001
kEventWindowGetRegion               = 1002
kEventWindowHitTest                 = 1003
kEventWindowInit                    = 1004
kEventWindowDispose                 = 1005
kEventWindowDragHilite              = 1006
kEventWindowModified                = 1007
kEventWindowSetupProxyDragImage     = 1008
kEventWindowStateChanged            = 1009
kEventWindowMeasureTitle            = 1010
kEventWindowDrawGrowBox             = 1011
kEventWindowGetGrowImageRegion      = 1012
kEventWindowPaint                   = 1013

# Process.h

kNoProcess                    = 0
kSystemProcess                = 1
kCurrentProcess               = 2

# CGColorSpace.h
kCGRenderingIntentDefault = 0

# CGImage.h
kCGImageAlphaNone                   = 0
kCGImageAlphaPremultipliedLast      = 1
kCGImageAlphaPremultipliedFirst     = 2
kCGImageAlphaLast                   = 3
kCGImageAlphaFirst                  = 4
kCGImageAlphaNoneSkipLast           = 5
kCGImageAlphaNoneSkipFirst          = 6
kCGImageAlphaOnly                   = 7

# Tablet
kEventTabletPoint = 1
kEventTabletProximity = 2
kEventParamTabletPointRec = _name('tbrc')
kEventParamTabletProximityRec = _name('tbpx')
typeTabletPointRec = _name('tbrc')
typeTabletProximityRec = _name('tbpx')
