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
#include <aws/core/http/HttpRequest.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/stl/AWSString.h>

namespace Aws
{
    namespace Http
    {
        namespace Standard
        {
            /**
             * Simple STL container modeling of HttpRequest. Implementation of the HttpRequest interface.
             */
            class AWS_CORE_API StandardHttpRequest : public HttpRequest
            {
            public:
                /**
                 * Initializes an HttpRequest object with uri and http method.
                 */
                StandardHttpRequest(const URI& uri, HttpMethod method);

                /**
                 * Get All headers for this request.
                 */
                virtual HeaderValueCollection GetHeaders() const override;
                /**
                 * Get the value for a Header based on its name.
                 */                
                virtual const Aws::String& GetHeaderValue(const char* headerName) const override;
                /**
                 * Add a header pair
                 */
                virtual void SetHeaderValue(const char* headerName, const Aws::String& headerValue) override;
                /**
                 * Add a header pair
                 */
                virtual void SetHeaderValue(const Aws::String& headerName, const Aws::String& headerValue) override;
                /**
                 * delete pair by headerName
                 */
                virtual void DeleteHeader(const char* headerName) override;
                /**                 
                 * Adds a content body stream to the request. This stream will be used to send the body to the endpoint.
                 */               
                virtual inline void AddContentBody(const std::shared_ptr<Aws::IOStream>& strContent) override { bodyStream = strContent; }
                /**
                 * Gets the content body stream that will be used for this request.
                 */
                virtual inline const std::shared_ptr<Aws::IOStream>& GetContentBody() const override { return bodyStream; }
                /**
                 * Returns true if a header exists in the request with name
                 */
                virtual bool HasHeader(const char*) const override;
                /**
                 * Get size in bytes of the request when as it will be going accross the wire.
                 */
                virtual int64_t GetSize() const override;
                /**
                 * Gets the factory for creating the stream that will be used in the http response.
                 */
                virtual const Aws::IOStreamFactory& GetResponseStreamFactory() const override;
                /**
                 * Sets the factory for creating the stream that will be used in the http response.
                 */
                virtual void SetResponseStreamFactory(const Aws::IOStreamFactory& factory) override;

            private:
                HeaderValueCollection headerMap;
                std::shared_ptr<Aws::IOStream> bodyStream;
                Aws::IOStreamFactory m_responseStreamFactory;
            };

        } // namespace Standard
    } // namespace Http
} // namespace Aws


