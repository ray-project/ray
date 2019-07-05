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
#include <mutex>
#include <condition_variable>

namespace Aws
{
    namespace Utils
    {
        namespace Threading
        {
            class AWS_CORE_API Semaphore {
                public:
                    /**
                     * Initializes a new instance of Semaphore class specifying the initial number of entries and 
                     * the maximum number of concurrent entries.
                     */
                    Semaphore(size_t initialCount, size_t maxCount);
                    /**
                     * Blocks the current thread until it receives a signal.
                     */
                    void WaitOne();
                    /**
                     * Exits the semaphore once.
                     */
                    void Release();
                    /**
                     * Exit the semaphore up to the maximum number of entries available.
                     */
                    void ReleaseAll();
                private:
                    size_t m_count;
                    const size_t m_maxCount;
                    std::mutex m_mutex;
                    std::condition_variable m_syncPoint;
            };
        }
    }
}
