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

namespace Aws
{
    namespace Utils
    {
        class EnumParseOverflowContainer;
    }
    /**
     * This is used to handle the Enum round tripping problem
     * for when a service updates their enumerations, but the user does not
     * have an up to date client. This member will be initialized the first time a client
     * is created and will be cleaned up when the last client goes out of scope.
     */
    AWS_CORE_API Utils::EnumParseOverflowContainer* GetEnumOverflowContainer();

    /**
     * Atomically set the underlying container to newValue, if it's current value is expectedValue.
     */
    AWS_CORE_API bool CheckAndSwapEnumOverflowContainer(Utils::EnumParseOverflowContainer* expectedValue, Utils::EnumParseOverflowContainer* newValue);
}