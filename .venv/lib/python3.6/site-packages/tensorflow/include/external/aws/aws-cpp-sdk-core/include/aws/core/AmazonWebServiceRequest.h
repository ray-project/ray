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

#include <aws/core/utils/memory/stl/AWSFunction.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/UnreferencedParam.h>
#include <aws/core/http/HttpTypes.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include <aws/core/auth/AWSAuthSigner.h>

namespace Aws
{
    namespace Http
    {
        class URI;
    } // namespace Http

    class AmazonWebServiceRequest;

    /**
     * Closure definition for handling a retry notification. This is only for if you want to be notified that a particular request is being retried.
     */
    typedef std::function<void(const AmazonWebServiceRequest&)> RequestRetryHandler;

    /**
     * Base level abstraction for all modeled AWS requests
     */
    class AWS_CORE_API AmazonWebServiceRequest
    {
    public:
        /**
         * Sets up default response stream factory. initializes other pointers to nullptr.
         */
        AmazonWebServiceRequest();
        virtual ~AmazonWebServiceRequest() = default;

        /**
         * Get the payload for the request
         */
        virtual std::shared_ptr<Aws::IOStream> GetBody() const = 0;
        /**
         * Get the headers for the request
         */
        virtual Aws::Http::HeaderValueCollection GetHeaders() const = 0;
        /**
         * Do nothing virtual, override this to add query strings to the request
         */
        virtual void AddQueryStringParameters(Aws::Http::URI& uri) const { AWS_UNREFERENCED_PARAM(uri); }

        /**
         * Put the request to a url for later presigning. This will push the body to the url and 
         * then adds the existing query string parameters as normal.
         */
        virtual void PutToPresignedUrl(Aws::Http::URI& uri) const { DumpBodyToUrl(uri); AddQueryStringParameters(uri); }

        /**
         * Defaults to true, if this is set to false, then signers, if they support body signing, will not do so
         */
        virtual bool SignBody() const { return true; }

        /**
         * Retrieves the factory for creating response streams.
         */
        const Aws::IOStreamFactory& GetResponseStreamFactory() const { return m_responseStreamFactory; }
        /**
         * Set the response stream factory.
         */
        void SetResponseStreamFactory(const Aws::IOStreamFactory& factory) { m_responseStreamFactory = AWS_BUILD_FUNCTION(factory); }
        /**
         * Register closure for data recieved event.
         */
        inline virtual void SetDataReceivedEventHandler(const Aws::Http::DataReceivedEventHandler& dataReceivedEventHandler) { m_onDataReceived = dataReceivedEventHandler; }
        /**
         * register closure for data sent event
         */
        inline virtual void SetDataSentEventHandler(const Aws::Http::DataSentEventHandler& dataSentEventHandler) { m_onDataSent = dataSentEventHandler; }
        /**
         * Register closure for  handling whether or not to continue a request.
         */
        inline virtual void SetContinueRequestHandler(const Aws::Http::ContinueRequestHandler& continueRequestHandler) { m_continueRequest = continueRequestHandler; }
        /**
        * Register closure for data recieved event.
        */
        inline virtual void SetDataReceivedEventHandler(Aws::Http::DataReceivedEventHandler&& dataReceivedEventHandler) { m_onDataReceived = std::move(dataReceivedEventHandler); }
        /**
        * register closure for data sent event
        */
        inline virtual void SetDataSentEventHandler(Aws::Http::DataSentEventHandler&& dataSentEventHandler) { m_onDataSent = std::move(dataSentEventHandler); }
        /**
         * Register closure for handling whether or not to cancel a request.
         */
        inline virtual void SetContinueRequestHandler(Aws::Http::ContinueRequestHandler&& continueRequestHandler) { m_continueRequest = std::move(continueRequestHandler); }
        /**
        * Register closure for notification that a request is being retried
        */
        inline virtual void SetRequestRetryHandler(const RequestRetryHandler& handler) { m_requestRetryHandler = handler; }
        /**
        * Register closure for notification that a request is being retried
        */
        inline virtual void SetRequestRetryHandler(RequestRetryHandler&& handler) { m_requestRetryHandler = std::move(handler); }
        /**
        * get closure for data recieved event.
        */
        inline virtual const Aws::Http::DataReceivedEventHandler& GetDataReceivedEventHandler() const { return m_onDataReceived; }
        /**
        * get closure for data sent event
        */
        inline virtual const Aws::Http::DataSentEventHandler& GetDataSentEventHandler() const { return m_onDataSent; }
        /**
         * get closure for handling whether or not to cancel a request.
         */
        inline virtual const Aws::Http::ContinueRequestHandler& GetContinueRequestHandler() const { return m_continueRequest; }
        /**
         * get closure for notification that a request is being retried
         */
        inline virtual const RequestRetryHandler& GetRequestRetryHandler() const { return m_requestRetryHandler; }
        /**
         * If this is set to true, content-md5 needs to be computed and set on the request
         */
        inline virtual bool ShouldComputeContentMd5() const { return false; }

        virtual const char* GetServiceRequestName() const = 0;

    protected:
        /**
         * Default does nothing. Override this to convert what would otherwise be the payload of the 
         *  request to a query string format.
         */
        virtual void DumpBodyToUrl(Aws::Http::URI& uri) const { AWS_UNREFERENCED_PARAM(uri); }

    private:
        Aws::IOStreamFactory m_responseStreamFactory;

        Aws::Http::DataReceivedEventHandler m_onDataReceived;
        Aws::Http::DataSentEventHandler m_onDataSent;
        Aws::Http::ContinueRequestHandler m_continueRequest;
        RequestRetryHandler m_requestRetryHandler;
    };

} // namespace Aws

