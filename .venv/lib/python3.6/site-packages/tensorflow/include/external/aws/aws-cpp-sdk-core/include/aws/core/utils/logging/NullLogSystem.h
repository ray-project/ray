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
#include <aws/core/utils/UnreferencedParam.h>

namespace Aws
{
    namespace Utils
    {
        namespace Logging
        {
            /**
             * Do nothing logger.
             */
            class AWS_CORE_API NullLogSystem : public LogSystemInterface
            {
            public:

                NullLogSystem() {}
                virtual ~NullLogSystem() {}

                virtual LogLevel GetLogLevel(void) const override { return LogLevel::Off; }

                virtual void Log(LogLevel logLevel, const char* tag, const char* formatStr, ...) override
                {
                    AWS_UNREFERENCED_PARAM(logLevel);
                    AWS_UNREFERENCED_PARAM(tag);
                    AWS_UNREFERENCED_PARAM(formatStr);
                }

                virtual void LogStream(LogLevel logLevel, const char* tag, const Aws::OStringStream &messageStream) override
                {
                    AWS_UNREFERENCED_PARAM(logLevel);
                    AWS_UNREFERENCED_PARAM(tag);
                    AWS_UNREFERENCED_PARAM(messageStream);
                }
            };

        } // namespace Logging
    } // namespace Utils
} // namespace Aws
