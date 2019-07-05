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

#include <aws/core/utils/memory/stl/AwsStringStream.h>
#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/standard/StandardHttpResponse.h>

namespace Aws
{
    namespace Client
    {
        struct ClientConfiguration;
    } // namespace Client

    namespace Http
    {

        class WinConnectionPoolMgr;

        /**
         * Base implementation of a windows http client - used by WinInetSyncHttpClient and WinHttpSyncHttpClient
         */
        class AWS_CORE_API WinSyncHttpClient : public HttpClient
        {
        public:
            using Base = HttpClient;
            
            virtual ~WinSyncHttpClient();

            /**
             *Makes request and receives response synchronously
             */
            std::shared_ptr<HttpResponse> MakeRequest(HttpRequest& request,
                Aws::Utils::RateLimits::RateLimiterInterface* readLimiter = nullptr,
                Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter = nullptr) const;

            /**
             * Gets log tag for use in logging.
             */
            virtual const char* GetLogTag() const { return "WinSyncHttpClient"; }
        protected:
            /**
             * Sets the instance wide win handle for the module open calls.
             */
            void SetOpenHandle(void* handle);
            /**
             * Gets the instance wide win handle for the module open calls.
             */
            void* GetOpenHandle() const { return m_openHandle; }
            /**
             * Sets the connection pool manager for the client to use.
             */
            void SetConnectionPoolManager(WinConnectionPoolMgr* connectionMgr);
            /**
             * Gets the connection pool manager the client is ussing.
             */
            WinConnectionPoolMgr* GetConnectionPoolManager() const { return m_connectionPoolMgr; }
            /**
             * Return call from implementation specific openrequest call.
             */
            void* AllocateWindowsHttpRequest(const Aws::Http::HttpRequest& request, void* connection) const;
            /**
             * config flag for whether or not to tell apis to allow redirects.
             */
            bool m_allowRedirects;
        private:

            virtual void* OpenRequest(const Aws::Http::HttpRequest& request, void* connection, const Aws::StringStream& ss) const = 0;
            virtual void DoAddHeaders(void* hHttpRequest, Aws::String& headerStr) const = 0;
            virtual uint64_t DoWriteData(void* hHttpRequest, char* streamBuffer, uint64_t bytesRead) const = 0;
            virtual bool DoReceiveResponse(void* hHttpRequest) const = 0;
            virtual bool DoQueryHeaders(void* hHttpRequest, std::shared_ptr<Aws::Http::Standard::StandardHttpResponse>& response, Aws::StringStream& ss, uint64_t& read) const = 0;
            virtual bool DoSendRequest(void* hHttpRequest) const = 0;
            virtual bool DoReadData(void* hHttpRequest, char* body, uint64_t size, uint64_t& read) const = 0;
            virtual void* GetClientModule() const = 0;

            bool StreamPayloadToRequest(const HttpRequest& request, void* hHttpRequest, Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter) const;
            void LogRequestInternalFailure() const;
            std::shared_ptr<HttpResponse> BuildSuccessResponse(const Aws::Http::HttpRequest& request, void* hHttpRequest, Aws::Utils::RateLimits::RateLimiterInterface* readLimiter) const;
            void AddHeadersToRequest(const HttpRequest& request, void* hHttpRequest) const;

            void* m_openHandle;
            //we need control over the order in which this gets cleaned up
            //that's why I'm not using unique_ptr here.
            WinConnectionPoolMgr* m_connectionPoolMgr;
        };

    } // namespace Http
} // namespace Aws

