#pragma once

#include <Win32_Interop/win32_types.h>

#ifndef _INO_T_DEFINED
#define _INO_T_DEFINED

typedef unsigned short _ino_t;  // inode number (unused on Windows)

#if !__STDC__
typedef _ino_t ino_t;
#endif
#endif

#ifndef _DEV_T_DEFINED
#define _DEV_T_DEFINED

typedef unsigned int _dev_t;  // device code

#if !__STDC__
typedef _dev_t dev_t;
#endif
#endif

#ifndef _OFF_T_DEFINED
#define _OFF_T_DEFINED

typedef long _off_t;  // file offset value

#if !__STDC__
typedef _off_t off_t;
#endif
#endif
