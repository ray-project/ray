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
#include <aws/core/utils/memory/stl/AWSStringStream.h>

namespace Aws
{
    namespace Utils
    {
        namespace Logging
        {
            enum class LogLevel : int;

            /**
             * Interface for logging implementations. If you want to write your own logger, you can start here, though you may have more
             * luck going down one more level to FormattedLogSystem. It does a bit more of the work for you and still gives you the ability
             * to override the IO portion.
             */
            class AWS_CORE_API LogSystemInterface
            {
            public:
                virtual ~LogSystemInterface() = default;

                /**
                 * Gets the currently configured log level for this logger.
                 */
                virtual LogLevel GetLogLevel(void) const = 0;
                /**
                 * Does a printf style output to the output stream. Don't use this, it's unsafe. See LogStream
                 */
                virtual void Log(LogLevel logLevel, const char* tag, const char* formatStr, ...) = 0;
                /**
                * Writes the stream to the output stream.
                */
                virtual void LogStream(LogLevel logLevel, const char* tag, const Aws::OStringStream &messageStream) = 0;

            };

        } // namespace Logging
    } // namespace Utils
} // namespace Aws
