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

#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/ResourceManager.h>

#include <wrl.h> 

struct IXMLHTTPRequest2;

namespace Aws
{
    namespace Http
    {
        typedef Microsoft::WRL::ComPtr<IXMLHTTPRequest2> HttpRequestComHandle;

        /**
         * COM-based http client. To use this client see the CMake option USE_IXML_HTTP_REQUEST_2.
         * Note this client is written for compatibility with windows versions that do not ship with winhttp, and will only work
         * on windows versions > 8.1. WinHttp should still be the default client for windows when possible. This client will run the IXMLHttpRequest2
         * COM module in CLSCTX_SERVER mode.
         */
        class AWS_CORE_API IXmlHttpRequest2HttpClient : public HttpClient
        {
        public:
            /**
             * Initialize client based on clientConfiguration. This will create a connection pool and configure client
             * wide parameters.
             */
            IXmlHttpRequest2HttpClient(const Aws::Client::ClientConfiguration& clientConfiguration);
            virtual ~IXmlHttpRequest2HttpClient();

            /**
             * Makes http request, returns http response.
             */
            virtual std::shared_ptr<HttpResponse> MakeRequest(HttpRequest& request,
                Aws::Utils::RateLimits::RateLimiterInterface* readLimiter = nullptr,
                Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter = nullptr) const;

            /**
             * You must call this method before making any calls to the constructor, unless you have already
             * called CoInit elsewhere in your system.
             */
            static void InitCOM();

        private:
            void FillClientSettings(const HttpRequestComHandle&) const;

            mutable Aws::Utils::ExclusiveOwnershipResourceManager<HttpRequestComHandle> m_resourceManager;
            Aws::String m_proxyUserName;
            Aws::String m_proxyPassword;
            size_t m_poolSize;
            bool m_followRedirects;
            bool m_verifySSL;
            size_t m_totalTimeoutMs;
        };
    }
}

