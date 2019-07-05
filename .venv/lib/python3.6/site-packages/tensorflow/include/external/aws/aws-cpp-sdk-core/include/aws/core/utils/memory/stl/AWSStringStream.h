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

#if defined(_GLIBCXX_FULLY_DYNAMIC_STRING) && _GLIBCXX_FULLY_DYNAMIC_STRING == 0 && defined(__ANDROID__)

#include <aws/core/utils/memory/stl/SimpleStringStream.h>

#else

#include <aws/core/utils/memory/stl/AWSAllocator.h>

#include <sstream>

#endif

namespace Aws
{

#if defined(_GLIBCXX_FULLY_DYNAMIC_STRING) && _GLIBCXX_FULLY_DYNAMIC_STRING == 0 && defined(__ANDROID__)

// see the large comment block in AWSString.h  for an explanation
typedef Aws::SimpleStringStream StringStream;
typedef Aws::SimpleIStringStream IStringStream;
typedef Aws::SimpleOStringStream OStringStream;
typedef Aws::Utils::Stream::SimpleStreamBuf StringBuf;

#else

typedef std::basic_stringstream< char, std::char_traits< char >, Aws::Allocator< char > > StringStream;
typedef std::basic_istringstream< char, std::char_traits< char >, Aws::Allocator< char > > IStringStream;
typedef std::basic_ostringstream< char, std::char_traits< char >, Aws::Allocator< char > > OStringStream;
typedef std::basic_stringbuf< char, std::char_traits< char >, Aws::Allocator< char > > StringBuf;

#endif

} // namespace Aws
