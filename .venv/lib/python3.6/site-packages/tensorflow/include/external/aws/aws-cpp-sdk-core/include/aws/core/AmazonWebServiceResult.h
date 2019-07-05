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

#include <aws/core/http/HttpTypes.h>
#include <utility>
#include <aws/core/http/HttpResponse.h>

namespace Aws
{
    /**
     * Container for web response to an AWS Request.
     */
    template <typename PAYLOAD_TYPE>
    class AmazonWebServiceResult
    {
    public:
        AmazonWebServiceResult() {}

        /**
         * Sets payload, header collection and a response code.
         */
        AmazonWebServiceResult(const PAYLOAD_TYPE& payload, const Http::HeaderValueCollection& headers, Http::HttpResponseCode responseCode = Http::HttpResponseCode::OK) :
            m_payload(payload),
            m_responseHeaders(headers),
            m_responseCode(responseCode)
        {}

        /**
        * Sets payload, header collection and a response code, but transfers ownership of payload and headers (for move only operations).
        */
        AmazonWebServiceResult(PAYLOAD_TYPE&& payload, Http::HeaderValueCollection&& headers, Http::HttpResponseCode responseCode = Http::HttpResponseCode::OK) :
            m_payload(std::forward<PAYLOAD_TYPE>(payload)),
            m_responseHeaders(std::forward<Http::HeaderValueCollection>(headers)),
            m_responseCode(responseCode)
        {}

        AmazonWebServiceResult(const AmazonWebServiceResult& result) :
            m_payload(result.m_payload),
            m_responseHeaders(result.m_responseHeaders),
            m_responseCode(result.m_responseCode)
        {}

        AmazonWebServiceResult(AmazonWebServiceResult&& result) :
            m_payload(std::move(result.m_payload)),
            m_responseHeaders(std::move(result.m_responseHeaders)),
            m_responseCode(result.m_responseCode)
        {}

        /**
         * Get the payload from the response
         */
        inline const PAYLOAD_TYPE& GetPayload() const { return m_payload; }
        /**
         * Get the payload from the response and take ownership of it.
         */
        inline PAYLOAD_TYPE TakeOwnershipOfPayload() { return std::move(m_payload); }
        /**
        * Get the headers from the response
        */
        inline const Http::HeaderValueCollection& GetHeaderValueCollection() const { return m_responseHeaders; }
        /**
        * Get the http response code from the response
        */
        inline Http::HttpResponseCode GetResponseCode() const { return m_responseCode; }

    private:
        PAYLOAD_TYPE m_payload;
        Http::HeaderValueCollection m_responseHeaders;
        Http::HttpResponseCode m_responseCode;
    };


}
