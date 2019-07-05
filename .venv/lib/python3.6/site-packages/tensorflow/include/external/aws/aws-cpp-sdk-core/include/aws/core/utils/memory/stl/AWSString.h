/*
 * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#pragma once

#include <aws/core/Core_EXPORTS.h>

#include <aws/core/utils/memory/stl/AWSAllocator.h>

#include <functional>
#include <string>

namespace Aws
{

#if defined(_GLIBCXX_FULLY_DYNAMIC_STRING) && _GLIBCXX_FULLY_DYNAMIC_STRING == 0 && defined(__ANDROID__)

/*
using std::string with shared libraries is broken on android when using gnustl
due to the platform-level decision to set _GLIBCXX_FULLY_DYNAMIC_STRING to 0

The problem:

(1) _GLIBCXX_FULLY_DYNAMIC_STRING is set to 0 in Android's  c++config.h for gnustl
(2) The optimization that this enables is completely broken if using shared libraries and there is no way to opt out of using it.
    An optimization that uses a comparison to a static memory address is death for shared libraries.

Supposing you have a shared library B that depends on another shared library A.  There are a variety of inocuous scenarios where you end up crashing
in the std::basic_string destructor because it's attempting to free a static memory address (&std::string::_Rep_Base::_S_empty_rep_storage -- you'll need to temporarily 
flip the struct to "public:" in order to take this address from your code).  
If you take the address of this location, you will get two
different answers depending on whether you query it in library A or library B (oddly enough, if you look the symbol itself up, it only shows up as public weak in
libgnustl_shared.so).  When the string destructor is run from the context of library B, the _Rep::_M_dispose function will end up attempting to free
an address that is static memory (from library A).


Lessons (with the empty string optimization forced on you):
  (1) You can't move std::strings across shared libraries (as a part of another class, Outcome in our case)
  (2) If you default initialize a std::string member variable, you can't have a mismatched constructor/destructor pair such that one is in a cpp file and the other 
      is missing/implicit or defined in the header file

Solutions:

Use libc++ rather than gnustl

For those who must use gnustl, we have provided a working solution by cobbling together a set of hacks:

We prevent the empty string optimization from ever being run on our strings by:
  (1) Make Aws::Allocator always fail equality checks with itself; this check is part of the empty string optimization in several std::basic_string constructors 
  (2) All other cases are prevented by turning Aws::String into a subclass whose default constructor and move operations go to baseclass versions which will not
      perform the empty string optimization

Those changes prevent crashes, but lead to very poor performance when using a string stream; every character added will result in multiple copies of the entire
string (ie, quadratic).

To fix the performance problems, we have put together a set of replacement classes, SimpleStreamBuf and SimpleStringStream, that
replace std::stringstream and std::stringbuf in SDK code.  These replacements use raw buffers rather than strings in order to
avoid the performance issues.

This solution is only enabled if using gnustl on Android.  In all other situations, normal STL types are used.
*/

using AndroidBasicString = std::basic_string< char, std::char_traits< char >, Aws::Allocator< char > >;

class String : public AndroidBasicString
{
    public:
        using Base = AndroidBasicString;

        String() : Base("") {} // allocator comparison failure will cause empty string optimisation to be skipped
        String(const String& rhs ) : Base(rhs) {}
        String(String&& rhs) : Base(rhs) {} // DO NOT CALL std::move, let this go to the const ref constructor
        String(const AndroidBasicString& rhs) : Base(rhs) {}
        String(AndroidBasicString&& rhs) : Base(rhs) {} // DO NOT CALL std::move, let this go to the const ref constructor
        String(const char* str) : Base(str) {}
        String(const char* str_begin, const char* str_end) : Base(str_begin, str_end) {}
        String(const AndroidBasicString& str, size_type pos, size_type count) : Base(str, pos, count) {} 
        String(const String& str, size_type pos, size_type count) : Base(str, pos, count) {}
        String(const char* str, size_type count) : Base(str, count) {}
        String(size_type count, char c) : Base(count, c) {}
        String(std::initializer_list<char> __l) : Base(__l) {}

        template<class _InputIterator>
	String(_InputIterator __beg, _InputIterator __end) : Base(__beg, __end) {}

	String& operator=(const String& rhs) { Base::operator=(rhs); return *this; }
	String& operator=(String&& rhs) { Base::operator=(rhs); return *this; } // might be ok to use std::move (base class uses swap) but let's be safe
	String& operator=(const AndroidBasicString& rhs) { Base::operator=(rhs); return *this; }
	String& operator=(AndroidBasicString&& rhs) { Base::operator=(rhs); return *this; } // might be ok to use std::move (base class uses swap) but let's be safe
	String& operator=(const char* str) { Base::operator=(str); return *this; }
};

#else

using String = std::basic_string< char, std::char_traits< char >, Aws::Allocator< char > >;

#ifdef _WIN32
using WString = std::basic_string< wchar_t, std::char_traits< wchar_t >, Aws::Allocator< wchar_t > >;
#endif

#endif // __ANDROID

} // namespace Aws



