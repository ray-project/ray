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

#include <aws/core/utils/memory/stl/AWSString.h>

namespace Aws
{
    /**
     * AWS Regions
     */
    namespace Region
    {
        static const char* const US_EAST_1 = "us-east-1";
        static const char* const US_WEST_1 = "us-west-1";
        static const char* const US_WEST_2 = "us-west-2";
        static const char* const EU_WEST_1 =  "eu-west-1";
        static const char* const EU_CENTRAL_1 = "eu-central-1";
        static const char* const AP_SOUTHEAST_1 = "ap-southeast-1";
        static const char* const AP_SOUTHEAST_2 = "ap-southeast-2";
        static const char* const AP_NORTHEAST_1 = "ap-northeast-1";
        static const char* const AP_NORTHEAST_2 = "ap-northeast-2";
        static const char* const SA_EAST_1 = "sa-east-1";
        static const char* const AP_SOUTH_1 = "ap-south-1";
        static const char* const CN_NORTH_1 = "cn-north-1";
    }

} // namespace Aws

