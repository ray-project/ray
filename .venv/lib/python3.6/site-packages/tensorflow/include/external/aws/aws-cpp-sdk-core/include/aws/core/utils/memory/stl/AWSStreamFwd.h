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

#include <iostream>
#include <functional>

namespace Aws
{

// Serves no purpose other than to help my conversion process
typedef std::basic_ifstream< char, std::char_traits< char > > IFStream;
typedef std::basic_ofstream< char, std::char_traits< char > > OFStream;
typedef std::basic_fstream< char, std::char_traits< char > > FStream;
typedef std::basic_istream< char, std::char_traits< char > > IStream;
typedef std::basic_ostream< char, std::char_traits< char > > OStream;
typedef std::basic_iostream< char, std::char_traits< char > > IOStream;
typedef std::istreambuf_iterator< char, std::char_traits< char > > IStreamBufIterator;

using IOStreamFactory = std::function< Aws::IOStream*(void) >;


} // namespace Aws
