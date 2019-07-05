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
#include <aws/core/utils/memory/stl/AWSStack.h>
#include <functional>
#include <atomic>

namespace Aws
{
    namespace Utils
    {
        /**
         * Make initialization and cleanup of shared resources less painful.
         * If you have this problem. Create a static instance of GetTheLights,
         * have each actor call Enter the room with your callable.
         *
         * When you are finished with the shared resources call LeaveRoom(). The last caller will
         * have its callable executed.
         */
        class AWS_CORE_API GetTheLights
        {
        public:
            GetTheLights();
            void EnterRoom(std::function<void()>&&);
            void LeaveRoom(std::function<void()>&&);
        private:
            std::atomic<int> m_value;
        };
    }
}