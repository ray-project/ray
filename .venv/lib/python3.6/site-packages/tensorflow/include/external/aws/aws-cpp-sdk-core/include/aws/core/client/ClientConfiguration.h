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
#include <aws/core/http/Scheme.h>
#include <aws/core/Region.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/http/HttpTypes.h>
#include <memory>

namespace Aws
{
    namespace Utils
    {
        namespace Threading
        {
            class Executor;
        } // namespace Threading

        namespace RateLimits
        {
            class RateLimiterInterface;
        } // namespace RateLimits
    } // namespace Utils

    namespace Client
    {
        class RetryStrategy; // forward declare

        /**
          * This mutable structure is used to configure any of the AWS clients.
          * Default values can only be overwritten prior to passing to the client constructors.
          */
        struct AWS_CORE_API ClientConfiguration
        {
            ClientConfiguration();
            /**
             * User Agent string user for http calls. This is filled in for you in the constructor. Don't override this unless you have a really good reason.
             */
            Aws::String userAgent;
            /**
             * Http scheme to use. E.g. Http or Https. Default HTTPS
             */
            Aws::Http::Scheme scheme;
            /**
             * AWS Region to use in signing requests. Default US_EAST_1
             */
            Aws::String region;
            /**
             * Use dual stack endpoint in the endpoint calculation. It is your responsibility to verify that the service supports ipv6 in the region you select.
             */
            bool useDualStack;
            /**
             * Max concurrent tcp connections for a single http client to use. Default 25.
             */
            unsigned maxConnections;
            /**
             * Socket read timeouts. Default 3000 ms. This should be more than adequate for most services. However, if you are transfering large amounts of data
             * or are worried about higher latencies, you should set to something that makes more sense for your use case. 
             */
            long requestTimeoutMs;
            /**
             * Socket connect timeout. Default 1000 ms. Unless you are very far away from your the data center you are talking to. 1000ms is more than sufficient.
             */
            long connectTimeoutMs;
            /**
             * Strategy to use in case of failed requests. Default is DefaultRetryStrategy (e.g. exponential backoff)
             */
            std::shared_ptr<RetryStrategy> retryStrategy;
            /**
             * Override the http endpoint used to talk to a service.
             */
            Aws::String endpointOverride;
            /**
             * If you have users going through a proxy, set the proxy scheme here. Default HTTP
             */
            Aws::Http::Scheme proxyScheme;
            /**
             * If you have users going through a proxy, set the host here.
             */
            Aws::String proxyHost;
            /**
             * If you have users going through a proxy, set the port here.
             */
            unsigned proxyPort;
            /**
             * If you have users going through a proxy, set the username here.
             */
            Aws::String proxyUserName;
            /**
            * If you have users going through a proxy, set the password here.
            */
            Aws::String proxyPassword;
            /**
            * Threading Executor implementation. Default uses std::thread::detach()
            */
            std::shared_ptr<Aws::Utils::Threading::Executor> executor;
            /**
             * If you need to test and want to get around TLS validation errors, do that here.
             * you probably shouldn't use this flag in a production scenario.
             */
            bool verifySSL;
            /**
             * If your Certificate Authority path is different from the default, you can tell
             * clients that aren't using the default trust store where to find your CA trust store.
             * If you are on windows or apple, you likely don't want this.
             */
            Aws::String caPath;
            /**
             * If you certificate file is different from the default, you can tell clients that
             * aren't using the default trust store where to find your ca file.
             * If you are on windows or apple, you likely dont't want this.
             */
             Aws::String caFile;
            /**
             * Rate Limiter implementation for outgoing bandwidth. Default is wide-open.
             */
            std::shared_ptr<Aws::Utils::RateLimits::RateLimiterInterface> writeRateLimiter;
            /**
            * Rate Limiter implementation for incoming bandwidth. Default is wide-open.
            */
            std::shared_ptr<Aws::Utils::RateLimits::RateLimiterInterface> readRateLimiter;
            /**
             * Override the http implementation the default factory returns.
             */
            Aws::Http::TransferLibType httpLibOverride;
            /**
             * If set to true the http stack will follow 300 redirect codes.
             */
            bool followRedirects;
            /**
             * If set to true clock skew will be adjusted after each http attempt, default to true.
             */
            bool enableClockSkewAdjustment;
        };

    } // namespace Client
} // namespace Aws


