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
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/core/utils/logging/LogLevel.h>

#include <atomic>

namespace Aws
{
    namespace Utils
    {
        namespace Logging
        {
            /**
             * Logger that formats log messages into [LEVEL] timestamp [threadid] message
             */
            class AWS_CORE_API FormattedLogSystem : public LogSystemInterface
            {
            public:
                using Base = LogSystemInterface;

                /**
                 * Initializes log system with logLevel
                 */
                FormattedLogSystem(LogLevel logLevel);
                virtual ~FormattedLogSystem() = default;

                /**
                 * Gets the currently configured log level.
                 */
                virtual LogLevel GetLogLevel(void) const override { return m_logLevel; }
                /**
                 * Set a new log level. This has the immediate effect of changing the log output to the new level.
                 */
                void SetLogLevel(LogLevel logLevel) { m_logLevel.store(logLevel); }

                /**
                 * Does a printf style output to ProcessFormattedStatement. Don't use this, it's unsafe. See LogStream
                 */
                virtual void Log(LogLevel logLevel, const char* tag, const char* formatStr, ...) override;

                /**
                 * Writes the stream to ProcessFormattedStatement.
                 */
                virtual void LogStream(LogLevel logLevel, const char* tag, const Aws::OStringStream &messageStream) override;

            protected:
                /**
                 * This is the method that most logger implementations will want to override.
                 * At this point the message is formatted and is ready to go to the output stream
                 */                
                virtual void ProcessFormattedStatement(Aws::String&& statement) = 0;

            private:
                std::atomic<LogLevel> m_logLevel;
            };

        } // namespace Logging
    } // namespace Utils
} // namespace Aws
