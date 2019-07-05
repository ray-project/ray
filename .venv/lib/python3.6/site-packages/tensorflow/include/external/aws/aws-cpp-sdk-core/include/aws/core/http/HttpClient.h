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

#include <memory>
#include <atomic>
#include <mutex>
#include <condition_variable>

namespace Aws
{
    namespace Utils
    {
        namespace RateLimits
        {
            class RateLimiterInterface;
        } // namespace RateLimits
    } // namespace Utils

    namespace Http
    {
        class HttpRequest;
        class HttpResponse;

        /**
          * Abstract HttpClient. All it does is make HttpRequests and return their response.
          */
        class AWS_CORE_API HttpClient
        {
        public:
            HttpClient();
            virtual ~HttpClient() {}

            /*
            * Takes an http request, makes it, and returns the newly allocated HttpResponse
            */
            virtual std::shared_ptr<HttpResponse> MakeRequest(HttpRequest& request,
                Aws::Utils::RateLimits::RateLimiterInterface* readLimiter = nullptr,
                Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter = nullptr) const = 0;

            /**
             * Stops all requests in progress and prevents any others from initiating.
             */
            void DisableRequestProcessing();
            /**
             * Enables/ReEnables request processing.
             */
            void EnableRequestProcessing();
            /**
             * Returns true if request processing is enabled.
             */
            bool IsRequestProcessingEnabled() const;
            /**
             * Sleeps current thread for sleepTime.
             */
            void RetryRequestSleep(std::chrono::milliseconds sleepTime);

            bool ContinueRequest(const Aws::Http::HttpRequest&) const;

        private:

            std::atomic< bool > m_disableRequestProcessing;

            std::mutex m_requestProcessingSignalLock;
            std::condition_variable m_requestProcessingSignal;
        };

    } // namespace Http
} // namespace Aws


