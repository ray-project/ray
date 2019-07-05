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
#include <ctime>

namespace Aws
{
namespace Time
{

    /*
    * A platform-agnostic implementation of the timegm function from gnu extensions
    */
    AWS_CORE_API time_t TimeGM(tm* const t);

    /*
    * Converts from a time value of std::time_t type to a C tm structure for easier date analysis
    */
    AWS_CORE_API void LocalTime(tm* t, std::time_t time);

    /*
    * Converts from a time value of std::time_t type to a C tm structure for easier date analysis
    */
    AWS_CORE_API void GMTime(tm* t, std::time_t time);

} // namespace Time
} // namespace Aws
