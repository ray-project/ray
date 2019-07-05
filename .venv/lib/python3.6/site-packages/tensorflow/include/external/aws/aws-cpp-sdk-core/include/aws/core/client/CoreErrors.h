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

#include "aws/core/Core_EXPORTS.h"

namespace Aws
{
    namespace Http 
    {
        enum class HttpResponseCode;
    }
    namespace Client
    {
        template<typename ERROR_TYPE>
        class AWSError;

        enum class CoreErrors
        {
            INCOMPLETE_SIGNATURE = 0,
            INTERNAL_FAILURE = 1,
            INVALID_ACTION = 2,
            INVALID_CLIENT_TOKEN_ID = 3,
            INVALID_PARAMETER_COMBINATION = 4,
            INVALID_QUERY_PARAMETER = 5,
            INVALID_PARAMETER_VALUE = 6,
            MISSING_ACTION = 7, // SDK should never allow
            MISSING_AUTHENTICATION_TOKEN = 8, // SDK should never allow
            MISSING_PARAMETER = 9, // SDK should never allow
            OPT_IN_REQUIRED = 10,
            REQUEST_EXPIRED = 11,
            SERVICE_UNAVAILABLE = 12,
            THROTTLING = 13,
            VALIDATION = 14,
            ACCESS_DENIED = 15,
            RESOURCE_NOT_FOUND = 16, // Shared with multiple services
            UNRECOGNIZED_CLIENT = 17, // Most likely caused by an invalid access key or secret key
            MALFORMED_QUERY_STRING = 18, // Where does this come from? (cognito identity uses it)
            SLOW_DOWN = 19,
            REQUEST_TIME_TOO_SKEWED = 20,
            INVALID_SIGNATURE = 21,
            SIGNATURE_DOES_NOT_MATCH = 22,
            INVALID_ACCESS_KEY_ID = 23,
            REQUEST_TIMEOUT = 24,

            NETWORK_CONNECTION = 99, // General failure to send message to service 

            // These are needed for logical reasons
            UNKNOWN = 100, // Unknown to the SDK
            SERVICE_EXTENSION_START_RANGE = 128
        };

        namespace CoreErrorsMapper
        {
            /**
             * Finds a CoreErrors member if possible. Otherwise, returns UNKNOWN
             */
            AWS_CORE_API AWSError<CoreErrors> GetErrorForName(const char* errorName);
            /**
             * Finds a CoreErrors member if possible by HTTP response code
             */
            AWS_CORE_API AWSError<CoreErrors> GetErrorForHttpResponseCode(Aws::Http::HttpResponseCode code);
        } // namespace CoreErrorsMapper
    } // namespace Client
} // namespace Aws


