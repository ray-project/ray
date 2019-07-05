from builtins import str

from ctypes import *
from ctypes import util

from .runtime import send_message, ObjCInstance
from .cocoatypes import *

######################################################################

# CORE FOUNDATION

cf = cdll.LoadLibrary(util.find_library('CoreFoundation'))

kCFStringEncodingUTF8 = 0x08000100

CFAllocatorRef = c_void_p
CFStringEncoding = c_uint32

cf.CFStringCreateWithCString.restype = c_void_p
cf.CFStringCreateWithCString.argtypes = [CFAllocatorRef, c_char_p, CFStringEncoding]

cf.CFRelease.restype = c_void_p
cf.CFRelease.argtypes = [c_void_p]

cf.CFStringGetLength.restype = CFIndex
cf.CFStringGetLength.argtypes = [c_void_p]

cf.CFStringGetMaximumSizeForEncoding.restype = CFIndex
cf.CFStringGetMaximumSizeForEncoding.argtypes = [CFIndex, CFStringEncoding]

cf.CFStringGetCString.restype = c_bool
cf.CFStringGetCString.argtypes = [c_void_p, c_char_p, CFIndex, CFStringEncoding]

cf.CFStringGetTypeID.restype = CFTypeID
cf.CFStringGetTypeID.argtypes = []

cf.CFAttributedStringCreate.restype = c_void_p
cf.CFAttributedStringCreate.argtypes = [CFAllocatorRef, c_void_p, c_void_p]

# Core Foundation type to Python type conversion functions

def CFSTR(string):
    return ObjCInstance(c_void_p(cf.CFStringCreateWithCString(
            None, string.encode('utf8'), kCFStringEncodingUTF8)))

# Other possible names for this method:
# at, ampersat, arobe, apenstaartje (little monkey tail), strudel,
# klammeraffe (spider monkey), little_mouse, arroba, sobachka (doggie)
# malpa (monkey), snabel (trunk), papaki (small duck), afna (monkey),
# kukac (caterpillar).
def get_NSString(string):
    """Autoreleased version of CFSTR"""
    return CFSTR(string).autorelease()

def cfstring_to_string(cfstring):
    length = cf.CFStringGetLength(cfstring)
    size = cf.CFStringGetMaximumSizeForEncoding(length, kCFStringEncodingUTF8)
    buffer = c_buffer(size + 1)
    result = cf.CFStringGetCString(cfstring, buffer, len(buffer), kCFStringEncodingUTF8)
    if result:
        return str(buffer.value, 'utf-8')

cf.CFDataCreate.restype = c_void_p
cf.CFDataCreate.argtypes = [c_void_p, c_void_p, CFIndex]

cf.CFDataGetBytes.restype = None
cf.CFDataGetBytes.argtypes = [c_void_p, CFRange, c_void_p]

cf.CFDataGetLength.restype = CFIndex
cf.CFDataGetLength.argtypes = [c_void_p]

cf.CFDictionaryGetValue.restype = c_void_p
cf.CFDictionaryGetValue.argtypes = [c_void_p, c_void_p]

cf.CFDictionaryAddValue.restype = None
cf.CFDictionaryAddValue.argtypes = [c_void_p, c_void_p, c_void_p]

cf.CFDictionaryCreateMutable.restype = c_void_p
cf.CFDictionaryCreateMutable.argtypes = [CFAllocatorRef, CFIndex, c_void_p, c_void_p]

cf.CFNumberCreate.restype = c_void_p
cf.CFNumberCreate.argtypes = [CFAllocatorRef, CFNumberType, c_void_p]

cf.CFNumberGetType.restype = CFNumberType
cf.CFNumberGetType.argtypes = [c_void_p]

cf.CFNumberGetValue.restype = c_ubyte
cf.CFNumberGetValue.argtypes = [c_void_p, CFNumberType, c_void_p]

cf.CFNumberGetTypeID.restype = CFTypeID
cf.CFNumberGetTypeID.argtypes = []

cf.CFGetTypeID.restype = CFTypeID
cf.CFGetTypeID.argtypes = [c_void_p]

# CFNumber.h
kCFNumberSInt8Type     = 1
kCFNumberSInt16Type    = 2
kCFNumberSInt32Type    = 3
kCFNumberSInt64Type    = 4
kCFNumberFloat32Type   = 5
kCFNumberFloat64Type   = 6
kCFNumberCharType      = 7
kCFNumberShortType     = 8
kCFNumberIntType       = 9
kCFNumberLongType      = 10
kCFNumberLongLongType  = 11
kCFNumberFloatType     = 12
kCFNumberDoubleType    = 13
kCFNumberCFIndexType   = 14
kCFNumberNSIntegerType = 15
kCFNumberCGFloatType   = 16
kCFNumberMaxType       = 16

def cfnumber_to_number(cfnumber):
    """Convert CFNumber to python int or float."""
    numeric_type = cf.CFNumberGetType(cfnumber)
    cfnum_to_ctype = {kCFNumberSInt8Type:c_int8, kCFNumberSInt16Type:c_int16,
                      kCFNumberSInt32Type:c_int32, kCFNumberSInt64Type:c_int64,
                      kCFNumberFloat32Type:c_float, kCFNumberFloat64Type:c_double,
                      kCFNumberCharType:c_byte, kCFNumberShortType:c_short,
                      kCFNumberIntType:c_int, kCFNumberLongType:c_long,
                      kCFNumberLongLongType:c_longlong, kCFNumberFloatType:c_float,
                      kCFNumberDoubleType:c_double, kCFNumberCFIndexType:CFIndex,
                      kCFNumberCGFloatType:CGFloat}

    if numeric_type in cfnum_to_ctype:
        t = cfnum_to_ctype[numeric_type]
        result = t()
        if cf.CFNumberGetValue(cfnumber, numeric_type, byref(result)):
            return result.value
    else:
        raise Exception('cfnumber_to_number: unhandled CFNumber type %d' % numeric_type)

# Dictionary of cftypes matched to the method converting them to python values.
known_cftypes = { cf.CFStringGetTypeID() : cfstring_to_string,
                  cf.CFNumberGetTypeID() : cfnumber_to_number
                  }

def cftype_to_value(cftype):
    """Convert a CFType into an equivalent python type.
    The convertible CFTypes are taken from the known_cftypes
    dictionary, which may be added to if another library implements
    its own conversion methods."""
    if not cftype:
        return None
    typeID = cf.CFGetTypeID(cftype)
    if typeID in known_cftypes:
        convert_function = known_cftypes[typeID]
        return convert_function(cftype)
    else:
        return cftype

cf.CFSetGetCount.restype = CFIndex
cf.CFSetGetCount.argtypes = [c_void_p]

cf.CFSetGetValues.restype = None
# PyPy 1.7 is fine with 2nd arg as POINTER(c_void_p),
# but CPython ctypes 1.1.0 complains, so just use c_void_p.
cf.CFSetGetValues.argtypes = [c_void_p, c_void_p]

def cfset_to_set(cfset):
    """Convert CFSet to python set."""
    count = cf.CFSetGetCount(cfset)
    buffer = (c_void_p * count)()
    cf.CFSetGetValues(cfset, byref(buffer))
    return set([ cftype_to_value(c_void_p(buffer[i])) for i in range(count) ])
    
cf.CFArrayGetCount.restype = CFIndex
cf.CFArrayGetCount.argtypes = [c_void_p]

cf.CFArrayGetValueAtIndex.restype = c_void_p
cf.CFArrayGetValueAtIndex.argtypes = [c_void_p, CFIndex]

def cfarray_to_list(cfarray):
    """Convert CFArray to python list."""
    count = cf.CFArrayGetCount(cfarray)
    return [ cftype_to_value(c_void_p(cf.CFArrayGetValueAtIndex(cfarray, i))) 
             for i in range(count) ]


kCFRunLoopDefaultMode = c_void_p.in_dll(cf, 'kCFRunLoopDefaultMode')

cf.CFRunLoopGetCurrent.restype = c_void_p
cf.CFRunLoopGetCurrent.argtypes = []

cf.CFRunLoopGetMain.restype = c_void_p
cf.CFRunLoopGetMain.argtypes = []

######################################################################

# APPLICATION KIT

# Even though we don't use this directly, it must be loaded so that
# we can find the NSApplication, NSWindow, and NSView classes.
appkit = cdll.LoadLibrary(util.find_library('AppKit'))

NSDefaultRunLoopMode = c_void_p.in_dll(appkit, 'NSDefaultRunLoopMode')
NSEventTrackingRunLoopMode = c_void_p.in_dll(appkit, 'NSEventTrackingRunLoopMode')
NSApplicationDidHideNotification = c_void_p.in_dll(appkit, 'NSApplicationDidHideNotification')
NSApplicationDidUnhideNotification = c_void_p.in_dll(appkit, 'NSApplicationDidUnhideNotification')

# /System/Library/Frameworks/AppKit.framework/Headers/NSEvent.h
NSAnyEventMask = 0xFFFFFFFF     # NSUIntegerMax

NSKeyDown            = 10
NSKeyUp              = 11
NSFlagsChanged       = 12
NSApplicationDefined = 15

NSAlphaShiftKeyMask         = 1 << 16
NSShiftKeyMask              = 1 << 17
NSControlKeyMask            = 1 << 18
NSAlternateKeyMask          = 1 << 19
NSCommandKeyMask            = 1 << 20
NSNumericPadKeyMask         = 1 << 21
NSHelpKeyMask               = 1 << 22
NSFunctionKeyMask           = 1 << 23

NSInsertFunctionKey   = 0xF727
NSDeleteFunctionKey   = 0xF728
NSHomeFunctionKey     = 0xF729
NSBeginFunctionKey    = 0xF72A
NSEndFunctionKey      = 0xF72B
NSPageUpFunctionKey   = 0xF72C
NSPageDownFunctionKey = 0xF72D

# /System/Library/Frameworks/AppKit.framework/Headers/NSWindow.h
NSBorderlessWindowMask		= 0
NSTitledWindowMask		= 1 << 0
NSClosableWindowMask		= 1 << 1
NSMiniaturizableWindowMask	= 1 << 2
NSResizableWindowMask		= 1 << 3

# /System/Library/Frameworks/AppKit.framework/Headers/NSPanel.h
NSUtilityWindowMask		= 1 << 4

# /System/Library/Frameworks/AppKit.framework/Headers/NSGraphics.h
NSBackingStoreRetained	        = 0
NSBackingStoreNonretained	= 1
NSBackingStoreBuffered	        = 2

# /System/Library/Frameworks/AppKit.framework/Headers/NSTrackingArea.h
NSTrackingMouseEnteredAndExited  = 0x01
NSTrackingMouseMoved             = 0x02
NSTrackingCursorUpdate 		 = 0x04
NSTrackingActiveInActiveApp 	 = 0x40

# /System/Library/Frameworks/AppKit.framework/Headers/NSOpenGL.h
NSOpenGLPFAAllRenderers       =   1   # choose from all available renderers          
NSOpenGLPFADoubleBuffer       =   5   # choose a double buffered pixel format        
NSOpenGLPFAStereo             =   6   # stereo buffering supported                   
NSOpenGLPFAAuxBuffers         =   7   # number of aux buffers                        
NSOpenGLPFAColorSize          =   8   # number of color buffer bits                  
NSOpenGLPFAAlphaSize          =  11   # number of alpha component bits               
NSOpenGLPFADepthSize          =  12   # number of depth buffer bits                  
NSOpenGLPFAStencilSize        =  13   # number of stencil buffer bits                
NSOpenGLPFAAccumSize          =  14   # number of accum buffer bits                  
NSOpenGLPFAMinimumPolicy      =  51   # never choose smaller buffers than requested  
NSOpenGLPFAMaximumPolicy      =  52   # choose largest buffers of type requested     
NSOpenGLPFAOffScreen          =  53   # choose an off-screen capable renderer        
NSOpenGLPFAFullScreen         =  54   # choose a full-screen capable renderer        
NSOpenGLPFASampleBuffers      =  55   # number of multi sample buffers               
NSOpenGLPFASamples            =  56   # number of samples per multi sample buffer    
NSOpenGLPFAAuxDepthStencil    =  57   # each aux buffer has its own depth stencil    
NSOpenGLPFAColorFloat         =  58   # color buffers store floating point pixels    
NSOpenGLPFAMultisample        =  59   # choose multisampling                         
NSOpenGLPFASupersample        =  60   # choose supersampling                         
NSOpenGLPFASampleAlpha        =  61   # request alpha filtering                      
NSOpenGLPFARendererID         =  70   # request renderer by ID                       
NSOpenGLPFASingleRenderer     =  71   # choose a single renderer for all screens     
NSOpenGLPFANoRecovery         =  72   # disable all failure recovery systems         
NSOpenGLPFAAccelerated        =  73   # choose a hardware accelerated renderer       
NSOpenGLPFAClosestPolicy      =  74   # choose the closest color buffer to request   
NSOpenGLPFARobust             =  75   # renderer does not need failure recovery      
NSOpenGLPFABackingStore       =  76   # back buffer contents are valid after swap    
NSOpenGLPFAMPSafe             =  78   # renderer is multi-processor safe             
NSOpenGLPFAWindow             =  80   # can be used to render to an onscreen window  
NSOpenGLPFAMultiScreen        =  81   # single window can span multiple screens      
NSOpenGLPFACompliant          =  83   # renderer is opengl compliant                 
NSOpenGLPFAScreenMask         =  84   # bit mask of supported physical screens       
NSOpenGLPFAPixelBuffer        =  90   # can be used to render to a pbuffer           
NSOpenGLPFARemotePixelBuffer  =  91   # can be used to render offline to a pbuffer   
NSOpenGLPFAAllowOfflineRenderers = 96 # allow use of offline renderers               
NSOpenGLPFAAcceleratedCompute =  97   # choose a hardware accelerated compute device 
NSOpenGLPFAOpenGLProfile      =  99   # specify an OpenGL Profile to use             
NSOpenGLPFAVirtualScreenCount = 128   # number of virtual screens in this format     

NSOpenGLProfileVersionLegacy  = 0x1000    # choose a Legacy/Pre-OpenGL 3.0 Implementation
NSOpenGLProfileVersion3_2Core = 0x3200    # choose an OpenGL 3.2 Core Implementation
NSOpenGLProfileVersion4_1Core = 0x4100    # choose an OpenGL 4.1 Core Implementation

NSOpenGLCPSwapInterval        = 222


# /System/Library/Frameworks/ApplicationServices.framework/Frameworks/...
#     CoreGraphics.framework/Headers/CGImage.h
kCGImageAlphaNone                   = 0
kCGImageAlphaPremultipliedLast      = 1
kCGImageAlphaPremultipliedFirst     = 2
kCGImageAlphaLast                   = 3
kCGImageAlphaFirst                  = 4
kCGImageAlphaNoneSkipLast           = 5
kCGImageAlphaNoneSkipFirst          = 6
kCGImageAlphaOnly                   = 7

kCGImageAlphaPremultipliedLast = 1

kCGBitmapAlphaInfoMask              = 0x1F
kCGBitmapFloatComponents            = 1 << 8

kCGBitmapByteOrderMask              = 0x7000
kCGBitmapByteOrderDefault           = 0 << 12
kCGBitmapByteOrder16Little          = 1 << 12
kCGBitmapByteOrder32Little          = 2 << 12
kCGBitmapByteOrder16Big             = 3 << 12
kCGBitmapByteOrder32Big             = 4 << 12

# NSApplication.h
NSApplicationPresentationDefault = 0
NSApplicationPresentationHideDock = 1 << 1
NSApplicationPresentationHideMenuBar = 1 << 3
NSApplicationPresentationDisableProcessSwitching = 1 << 5
NSApplicationPresentationDisableHideApplication = 1 << 8

# NSRunningApplication.h
NSApplicationActivationPolicyRegular = 0
NSApplicationActivationPolicyAccessory = 1
NSApplicationActivationPolicyProhibited = 2

######################################################################

# QUARTZ / COREGRAPHICS

quartz = cdll.LoadLibrary(util.find_library('quartz'))

CGDirectDisplayID = c_uint32     # CGDirectDisplay.h
CGError = c_int32                # CGError.h
CGBitmapInfo = c_uint32          # CGImage.h

# /System/Library/Frameworks/ApplicationServices.framework/Frameworks/...
#     ImageIO.framework/Headers/CGImageProperties.h
kCGImagePropertyGIFDictionary = c_void_p.in_dll(quartz, 'kCGImagePropertyGIFDictionary')
kCGImagePropertyGIFDelayTime = c_void_p.in_dll(quartz, 'kCGImagePropertyGIFDelayTime')

# /System/Library/Frameworks/ApplicationServices.framework/Frameworks/...
#     CoreGraphics.framework/Headers/CGColorSpace.h
kCGRenderingIntentDefault = 0

quartz.CGDisplayIDToOpenGLDisplayMask.restype = c_uint32
quartz.CGDisplayIDToOpenGLDisplayMask.argtypes = [c_uint32]

quartz.CGMainDisplayID.restype = CGDirectDisplayID
quartz.CGMainDisplayID.argtypes = []

quartz.CGShieldingWindowLevel.restype = c_int32
quartz.CGShieldingWindowLevel.argtypes = []

quartz.CGCursorIsVisible.restype = c_bool

quartz.CGDisplayCopyAllDisplayModes.restype = c_void_p
quartz.CGDisplayCopyAllDisplayModes.argtypes = [CGDirectDisplayID, c_void_p]

quartz.CGDisplaySetDisplayMode.restype = CGError
quartz.CGDisplaySetDisplayMode.argtypes = [CGDirectDisplayID, c_void_p, c_void_p]

quartz.CGDisplayCapture.restype = CGError
quartz.CGDisplayCapture.argtypes = [CGDirectDisplayID]

quartz.CGDisplayRelease.restype = CGError
quartz.CGDisplayRelease.argtypes = [CGDirectDisplayID]

quartz.CGDisplayCopyDisplayMode.restype = c_void_p
quartz.CGDisplayCopyDisplayMode.argtypes = [CGDirectDisplayID]

quartz.CGDisplayModeGetRefreshRate.restype = c_double
quartz.CGDisplayModeGetRefreshRate.argtypes = [c_void_p]

quartz.CGDisplayModeRetain.restype = c_void_p
quartz.CGDisplayModeRetain.argtypes = [c_void_p] 

quartz.CGDisplayModeRelease.restype = None
quartz.CGDisplayModeRelease.argtypes = [c_void_p]

quartz.CGDisplayModeGetWidth.restype = c_size_t
quartz.CGDisplayModeGetWidth.argtypes = [c_void_p]

quartz.CGDisplayModeGetHeight.restype = c_size_t
quartz.CGDisplayModeGetHeight.argtypes = [c_void_p]

quartz.CGDisplayModeCopyPixelEncoding.restype = c_void_p
quartz.CGDisplayModeCopyPixelEncoding.argtypes = [c_void_p]

quartz.CGGetActiveDisplayList.restype = CGError
quartz.CGGetActiveDisplayList.argtypes = [c_uint32, POINTER(CGDirectDisplayID), POINTER(c_uint32)]

quartz.CGDisplayBounds.restype = CGRect
quartz.CGDisplayBounds.argtypes = [CGDirectDisplayID]

quartz.CGImageSourceCreateWithData.restype = c_void_p
quartz.CGImageSourceCreateWithData.argtypes = [c_void_p, c_void_p]

quartz.CGImageSourceCreateImageAtIndex.restype = c_void_p
quartz.CGImageSourceCreateImageAtIndex.argtypes = [c_void_p, c_size_t, c_void_p]

quartz.CGImageSourceCopyPropertiesAtIndex.restype = c_void_p
quartz.CGImageSourceCopyPropertiesAtIndex.argtypes = [c_void_p, c_size_t, c_void_p]

quartz.CGImageGetDataProvider.restype = c_void_p
quartz.CGImageGetDataProvider.argtypes = [c_void_p]

quartz.CGDataProviderCopyData.restype = c_void_p
quartz.CGDataProviderCopyData.argtypes = [c_void_p]

quartz.CGDataProviderCreateWithCFData.restype = c_void_p
quartz.CGDataProviderCreateWithCFData.argtypes = [c_void_p]

quartz.CGImageCreate.restype = c_void_p
quartz.CGImageCreate.argtypes = [c_size_t, c_size_t, c_size_t, c_size_t, c_size_t, c_void_p, c_uint32, c_void_p, c_void_p, c_bool, c_int]

quartz.CGImageRelease.restype = None
quartz.CGImageRelease.argtypes = [c_void_p]

quartz.CGImageGetBytesPerRow.restype = c_size_t
quartz.CGImageGetBytesPerRow.argtypes = [c_void_p]

quartz.CGImageGetWidth.restype = c_size_t
quartz.CGImageGetWidth.argtypes = [c_void_p]

quartz.CGImageGetHeight.restype = c_size_t
quartz.CGImageGetHeight.argtypes = [c_void_p]

quartz.CGImageGetBitsPerPixel.restype = c_size_t
quartz.CGImageGetBitsPerPixel.argtypes = [c_void_p]

quartz.CGImageGetBitmapInfo.restype = CGBitmapInfo
quartz.CGImageGetBitmapInfo.argtypes = [c_void_p]

quartz.CGColorSpaceCreateDeviceRGB.restype = c_void_p
quartz.CGColorSpaceCreateDeviceRGB.argtypes = []

quartz.CGDataProviderRelease.restype = None
quartz.CGDataProviderRelease.argtypes = [c_void_p]

quartz.CGColorSpaceRelease.restype = None
quartz.CGColorSpaceRelease.argtypes = [c_void_p]

quartz.CGWarpMouseCursorPosition.restype = CGError
quartz.CGWarpMouseCursorPosition.argtypes = [CGPoint]

quartz.CGDisplayMoveCursorToPoint.restype = CGError
quartz.CGDisplayMoveCursorToPoint.argtypes = [CGDirectDisplayID, CGPoint]

quartz.CGAssociateMouseAndMouseCursorPosition.restype = CGError
quartz.CGAssociateMouseAndMouseCursorPosition.argtypes = [c_bool]

quartz.CGBitmapContextCreate.restype = c_void_p
quartz.CGBitmapContextCreate.argtypes = [c_void_p, c_size_t, c_size_t, c_size_t, c_size_t, c_void_p, CGBitmapInfo]

quartz.CGBitmapContextCreateImage.restype = c_void_p
quartz.CGBitmapContextCreateImage.argtypes = [c_void_p]

quartz.CGFontCreateWithDataProvider.restype = c_void_p
quartz.CGFontCreateWithDataProvider.argtypes = [c_void_p]

quartz.CGFontCreateWithFontName.restype = c_void_p
quartz.CGFontCreateWithFontName.argtypes = [c_void_p]

quartz.CGContextDrawImage.restype = None
quartz.CGContextDrawImage.argtypes = [c_void_p, CGRect, c_void_p]

quartz.CGContextRelease.restype = None
quartz.CGContextRelease.argtypes = [c_void_p]

quartz.CGContextSetTextPosition.restype = None
quartz.CGContextSetTextPosition.argtypes = [c_void_p, CGFloat, CGFloat]

quartz.CGContextSetShouldAntialias.restype = None
quartz.CGContextSetShouldAntialias.argtypes = [c_void_p, c_bool]

######################################################################

# CORETEXT
ct = cdll.LoadLibrary(util.find_library('CoreText'))

# Types
CTFontOrientation = c_uint32      # CTFontDescriptor.h
CTFontSymbolicTraits = c_uint32   # CTFontTraits.h

# CoreText constants
kCTFontAttributeName = c_void_p.in_dll(ct, 'kCTFontAttributeName')
kCTFontFamilyNameAttribute = c_void_p.in_dll(ct, 'kCTFontFamilyNameAttribute')
kCTFontSymbolicTrait = c_void_p.in_dll(ct, 'kCTFontSymbolicTrait')
kCTFontWeightTrait = c_void_p.in_dll(ct, 'kCTFontWeightTrait')
kCTFontTraitsAttribute = c_void_p.in_dll(ct, 'kCTFontTraitsAttribute')

# constants from CTFontTraits.h
kCTFontItalicTrait = (1 << 0)
kCTFontBoldTrait   = (1 << 1)

ct.CTLineCreateWithAttributedString.restype = c_void_p
ct.CTLineCreateWithAttributedString.argtypes = [c_void_p]

ct.CTLineDraw.restype = None
ct.CTLineDraw.argtypes = [c_void_p, c_void_p]

ct.CTFontGetBoundingRectsForGlyphs.restype = CGRect
ct.CTFontGetBoundingRectsForGlyphs.argtypes = [c_void_p, CTFontOrientation, POINTER(CGGlyph), POINTER(CGRect), CFIndex]

ct.CTFontGetAdvancesForGlyphs.restype = c_double
ct.CTFontGetAdvancesForGlyphs.argtypes = [c_void_p, CTFontOrientation, POINTER(CGGlyph), POINTER(CGSize), CFIndex]

ct.CTFontGetAscent.restype = CGFloat
ct.CTFontGetAscent.argtypes = [c_void_p]

ct.CTFontGetDescent.restype = CGFloat
ct.CTFontGetDescent.argtypes = [c_void_p]

ct.CTFontGetSymbolicTraits.restype = CTFontSymbolicTraits
ct.CTFontGetSymbolicTraits.argtypes = [c_void_p]

ct.CTFontGetGlyphsForCharacters.restype = c_bool
ct.CTFontGetGlyphsForCharacters.argtypes = [c_void_p, POINTER(UniChar), POINTER(CGGlyph), CFIndex]

ct.CTFontCreateWithGraphicsFont.restype = c_void_p
ct.CTFontCreateWithGraphicsFont.argtypes = [c_void_p, CGFloat, c_void_p, c_void_p]

ct.CTFontCopyFamilyName.restype = c_void_p
ct.CTFontCopyFamilyName.argtypes = [c_void_p]

ct.CTFontCopyFullName.restype = c_void_p
ct.CTFontCopyFullName.argtypes = [c_void_p]

ct.CTFontCreateWithFontDescriptor.restype = c_void_p
ct.CTFontCreateWithFontDescriptor.argtypes = [c_void_p, CGFloat, c_void_p]

ct.CTFontDescriptorCreateWithAttributes.restype = c_void_p
ct.CTFontDescriptorCreateWithAttributes.argtypes = [c_void_p]

######################################################################

# FOUNDATION

foundation = cdll.LoadLibrary(util.find_library('Foundation'))

foundation.NSMouseInRect.restype = c_bool
foundation.NSMouseInRect.argtypes = [NSPoint, NSRect, c_bool]
