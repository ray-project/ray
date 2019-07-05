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

#include <map>
#include <unordered_map>
#include <cstring>

namespace Aws
{

template< typename K, typename V > using Map = std::map< K, V, std::less< K >, Aws::Allocator< std::pair< const K, V > > >;
template< typename K, typename V > using UnorderedMap = std::unordered_map< K, V, std::hash< K >, std::equal_to< K >, Aws::Allocator< std::pair< const K, V > > >;
template< typename K, typename V > using MultiMap = std::multimap< K, V, std::less< K >, Aws::Allocator< std::pair< const K, V > > >;

struct CompareStrings
{
    bool operator()(const char* a, const char* b) const
    {
        return std::strcmp(a, b) < 0;
    }
};

template<typename V> using CStringMap = std::map<const char*, V, CompareStrings, Aws::Allocator<std::pair<const char*, V> > >;

} // namespace Aws
