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

        class WinHttpConnectionPoolMgr;

        /**
         * WinHttp implementation of an http client
         */
        class AWS_CORE_API WinHttpSyncHttpClient : public WinSyncHttpClient
        {
        public:
            using Base = WinSyncHttpClient;

            /**
             * Initializes the client with relevant parameters from clientConfig.
             */
            WinHttpSyncHttpClient(const Aws::Client::ClientConfiguration& clientConfig);
            ~WinHttpSyncHttpClient();

            /**
             * Log tag for use in base class.
             */
            const char* GetLogTag() const override { return "WinHttpSyncHttpClient"; }

        private:
            // WinHttp specific implementations
            void* OpenRequest(const Aws::Http::HttpRequest& request, void* connection, const Aws::StringStream& ss) const override;
            void DoAddHeaders(void* httpRequest, Aws::String& headerStr) const override;
            uint64_t DoWriteData(void* httpRequest, char* streamBuffer, uint64_t bytesRead) const override;
            bool DoReceiveResponse(void* httpRequest) const override;
            bool DoQueryHeaders(void* httpRequest, std::shared_ptr<Aws::Http::Standard::StandardHttpResponse>& response, Aws::StringStream& ss, uint64_t& read) const override;
            bool DoSendRequest(void* httpRequest) const override;
            bool DoReadData(void* hHttpRequest, char* body, uint64_t size, uint64_t& read) const override;
            void* GetClientModule() const override;

            bool m_usingProxy;
            bool m_verifySSL;
            Aws::WString m_proxyUserName;
            Aws::WString m_proxyPassword;
        };

    } // namespace Http
} // namespace Aws

