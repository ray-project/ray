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
    namespace Client
    {

        enum class CoreErrors;
        template<typename ERROR_TYPE>
        class AWSError;

        /**
         * Interface for defining a Retry Strategy. Override this class to provide your own custom retry behavior.
         */
        class AWS_CORE_API RetryStrategy
        {
        public:
            virtual ~RetryStrategy() {}
            /**
             * Returns true if the error can be retried given the error and the number of times already tried.
             */
            virtual bool ShouldRetry(const AWSError<CoreErrors>& error, long attemptedRetries) const = 0;

            /**
             * Calculates the time in milliseconds the client should sleep before attemptinig another request based on the error and attemptedRetries count.
             */
            virtual long CalculateDelayBeforeNextRetry(const AWSError<CoreErrors>& error, long attemptedRetries) const = 0;

        };

    } // namespace Client
} // namespace Aws
