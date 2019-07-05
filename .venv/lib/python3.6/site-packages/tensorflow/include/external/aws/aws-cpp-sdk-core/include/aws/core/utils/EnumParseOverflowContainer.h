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

#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/Core_EXPORTS.h>
#include <mutex>

namespace Aws
{
    namespace Utils
    {
        /**
         * This container is for storing unknown enum values that are encountered during parsing.
         * This is to work around the round-tripping enums problem. It's really just a simple thread-safe
         * hashmap.
         */
        class AWS_CORE_API EnumParseOverflowContainer
        {
        public:
            const Aws::String& RetrieveOverflow(int hashCode) const;
            void StoreOverflow(int hashCode, const Aws::String& value);

        private:
            mutable std::mutex m_overflowLock;
            Aws::Map<int, Aws::String> m_overflowMap;
            Aws::String m_emptyString;
        };
    }
}


