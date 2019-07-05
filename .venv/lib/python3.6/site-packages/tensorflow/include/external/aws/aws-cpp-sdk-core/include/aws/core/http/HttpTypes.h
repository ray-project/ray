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

#include <aws/core/utils/memory/stl/AWSList.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSMap.h>

#include <memory>

namespace Aws
{
    namespace Http
    {
        /**
         * Models Http methods.
         */
        enum class HttpMethod
        {
            HTTP_GET,
            HTTP_POST,
            HTTP_DELETE,
            HTTP_PUT,
            HTTP_HEAD,
            HTTP_PATCH
        };

        /**
         * Possible default http factory vended http client implementations.
         */
        enum class TransferLibType
        {
            DEFAULT_CLIENT,
            CURL_CLIENT,
            WIN_INET_CLIENT,
            WIN_HTTP_CLIENT
        };

        namespace HttpMethodMapper
        {
            /**
             * Gets the string value of an httpMethod.
             */
            AWS_CORE_API const char* GetNameForHttpMethod(HttpMethod httpMethod);
        } // namespace HttpMethodMapper

        typedef std::pair<Aws::String, Aws::String> HeaderValuePair;
        typedef Aws::Map<Aws::String, Aws::String> HeaderValueCollection;

    } // namespace Http
} // namespace Aws

