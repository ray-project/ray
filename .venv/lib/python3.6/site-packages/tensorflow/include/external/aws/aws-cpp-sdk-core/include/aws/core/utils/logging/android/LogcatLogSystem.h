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

#include <aws/core/utils/logging/FormattedLogSystem.h>

namespace Aws
{
namespace Utils
{
namespace Logging
{

class AWS_CORE_API LogcatLogSystem : public FormattedLogSystem
{
    public:

        using Base = FormattedLogSystem;

        LogcatLogSystem(LogLevel logLevel) :
	    Base(logLevel)
	{}

        virtual ~LogcatLogSystem() {}

    protected:

	virtual void ProcessFormattedStatement(Aws::String&& statement) override;
};

} // namespace Logging
} // namespace Utils
} // namespace Aws


