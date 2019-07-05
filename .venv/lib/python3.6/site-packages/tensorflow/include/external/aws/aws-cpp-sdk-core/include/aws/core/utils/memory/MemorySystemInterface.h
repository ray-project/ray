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

#include <cstdlib>

namespace Aws
{
    namespace Utils
    {
        namespace Memory
        {
            /**
             * Central interface for memory management customizations. To create a custom memory manager, implement this interface and then 
             * call InitializeAWSMemorySystem().
             */
            class AWS_CORE_API MemorySystemInterface
            {
            public:
                virtual ~MemorySystemInterface() = default;

                /**
                 * This is for initializing your memory manager in a static context. This can be empty if you don't need to do that.
                 */
                virtual void Begin() = 0;
                /**
                * This is for cleaning up your memory manager in a static context. This can be empty if you don't need to do that.
                */
                virtual void End() = 0;

                /**
                 * Allocate your memory inside this method. blocksize and alignment are exactly the same as the std::alocators interfaces.
                 * The allocationTag parameter is for memory tracking; you don't have to handle it.
                 */
                virtual void* AllocateMemory(std::size_t blockSize, std::size_t alignment, const char *allocationTag = nullptr) = 0;
                
                /**
                 * Free the memory pointed to by memoryPtr.
                 */
                virtual void FreeMemory(void* memoryPtr) = 0;
            };

        } // namespace Memory
    } // namespace Utils
} // namespace Aws
