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

#include <aws/core/http/windows/WinConnectionPoolMgr.h>

namespace Aws
{
    namespace Http
    {

        /**
        * Connection pool manager for winhttp api. This is the default windows http implementation.
        * It is more efficient and performant than WinINet.
        */
        class AWS_CORE_API WinHttpConnectionPoolMgr : public WinConnectionPoolMgr
        {
        public:
            /**
             * Initializes the connection pool with the passedin iOpen handle from earlier winHttp init calls.
             * configures connect calls with the timeouts. MaxConnectionsPerHost sets the queue sizes for each endpoint.
             */
            WinHttpConnectionPoolMgr(void* iOpenHandle, unsigned maxConnectionsPerHost, long requestTimeout, long connectTimeout);

            virtual ~WinHttpConnectionPoolMgr();

            /**
             * Gets the log tag to use for logging in the base class.
             */
            const char* GetLogTag() const { return "WinHttpConnectionPoolMgr"; }

        private:
            virtual void DoCloseHandle(void* handle) const override;
            virtual void* CreateNewConnection(const Aws::String& host, HostConnectionContainer& connectionContainer) const override;

            WinHttpConnectionPoolMgr(const WinHttpConnectionPoolMgr&) = delete;
            const WinHttpConnectionPoolMgr& operator = (const WinHttpConnectionPoolMgr&) = delete;
            WinHttpConnectionPoolMgr(const WinHttpConnectionPoolMgr&&) = delete;
            const WinHttpConnectionPoolMgr& operator = (const WinHttpConnectionPoolMgr&&) = delete;

        };

    } // namespace Http
} // namespace Aws
