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
#include <functional>
#include <thread>
#include <atomic>

namespace Aws
{
    namespace Utils
    {
        namespace Threading
        {
            class PooledThreadExecutor;

            class AWS_CORE_API ThreadTask
            {
            public:
                ThreadTask(PooledThreadExecutor& executor);
                ~ThreadTask();

                /**
                * Rule of 5 stuff.
                * Don't copy or move
                */
                ThreadTask(const ThreadTask&) = delete;
                ThreadTask& operator =(const ThreadTask&) = delete;
                ThreadTask(ThreadTask&&) = delete;
                ThreadTask& operator =(ThreadTask&&) = delete;

                void StopProcessingWork();                

            protected:
                void MainTaskRunner();

            private:                
                std::atomic<bool> m_continue;
                PooledThreadExecutor& m_executor;
                std::thread m_thread;
            };
        }
    }
}
