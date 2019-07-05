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

#include <aws/core/http/HttpClient.h>
#include <aws/core/http/windows/WinSyncHttpClient.h>

namespace Aws
{
    namespace Client
    {
        struct ClientConfiguration;
    } // namespace Client

    namespace Http
    {
        class WinINetConnectionPoolMgr;

        /**
         *WinInet implementation of an http client.
         */
        class AWS_CORE_API WinINetSyncHttpClient : public WinSyncHttpClient
        {
        public:
            using Base = WinSyncHttpClient;

            /**
            * Initializes the client with relevant parameters from clientConfig.
            */
            WinINetSyncHttpClient(const Aws::Client::ClientConfiguration& clientConfig);
            ~WinINetSyncHttpClient();

            /**
             * Gets log tag for use in logging in the base class.
             */
            const char* GetLogTag() const override { return "WinInetSyncHttpClient"; }
        private:

            // WinHttp specific implementations
            void* OpenRequest(const Aws::Http::HttpRequest& request, void* connection, const Aws::StringStream& ss) const override;
            void DoAddHeaders(void* hHttpRequest, Aws::String& headerStr) const override;
            uint64_t DoWriteData(void* hHttpRequest, char* streamBuffer, uint64_t bytesRead) const override;
            bool DoReceiveResponse(void* hHttpRequest) const override;
            bool DoQueryHeaders(void* hHttpRequest, std::shared_ptr<Aws::Http::Standard::StandardHttpResponse>& response, Aws::StringStream& ss, uint64_t& read) const override;
            bool DoSendRequest(void* hHttpRequest) const override;
            bool DoReadData(void* hHttpRequest, char* body, uint64_t size, uint64_t& read) const override;
            void* GetClientModule() const override;

            WinINetSyncHttpClient &operator =(const WinINetSyncHttpClient &rhs);

            bool m_usingProxy;
            Aws::String m_proxyUserName;
            Aws::String m_proxyPassword;
        };

    } // namespace Http
} // namespace Aws

