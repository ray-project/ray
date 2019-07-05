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

#include <aws/core/http/HttpTypes.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>

namespace Aws
{
    namespace Client
    {
        struct ClientConfiguration;
    } // namespace Client
    namespace Http
    {
        class URI;
        class HttpClient;
        class HttpRequest;

        /**
         * Interface and default implementation of client for Http stack
         */
        class AWS_CORE_API HttpClientFactory
        {
        public:
            virtual ~HttpClientFactory() {}

            /**
             * Creates a shared_ptr of HttpClient with the relevant settings from clientConfiguration
             */
            virtual std::shared_ptr<HttpClient> CreateHttpClient(const Aws::Client::ClientConfiguration& clientConfiguration) const = 0;
            /**
             * Creates a shared_ptr of HttpRequest with uri, method, and closure for how to create a response stream.
             */
            virtual std::shared_ptr<HttpRequest> CreateHttpRequest(const Aws::String& uri, HttpMethod method, const Aws::IOStreamFactory& streamFactory) const = 0;
            /**
             * Creates a shared_ptr of HttpRequest with uri, method, and closure for how to create a response stream.
             */
            virtual std::shared_ptr<HttpRequest> CreateHttpRequest(const URI& uri, HttpMethod method, const Aws::IOStreamFactory& streamFactory) const = 0;

            virtual void InitStaticState() {}
            virtual void CleanupStaticState() {}
        };

        /**
         * libCurl infects everything with its global state. If it is being used then we automatically initialize and clean it up.
         * If this is a problem for you, set this to false. If you manually initialize libcurl please add the option CURL_GLOBAL_ALL to your init call.
         */
        AWS_CORE_API void SetInitCleanupCurlFlag(bool initCleanupFlag);
        AWS_CORE_API void SetInstallSigPipeHandlerFlag(bool installHandler);
        AWS_CORE_API void InitHttp();
        AWS_CORE_API void CleanupHttp();
        AWS_CORE_API void SetHttpClientFactory(const std::shared_ptr<HttpClientFactory>& factory);
        AWS_CORE_API std::shared_ptr<HttpClient> CreateHttpClient(const Aws::Client::ClientConfiguration& clientConfiguration);
        AWS_CORE_API std::shared_ptr<HttpRequest> CreateHttpRequest(const Aws::String& uri, HttpMethod method, const Aws::IOStreamFactory& streamFactory);
        AWS_CORE_API std::shared_ptr<HttpRequest> CreateHttpRequest(const URI& uri, HttpMethod method, const Aws::IOStreamFactory& streamFactory);

    } // namespace Http
} // namespace Aws

