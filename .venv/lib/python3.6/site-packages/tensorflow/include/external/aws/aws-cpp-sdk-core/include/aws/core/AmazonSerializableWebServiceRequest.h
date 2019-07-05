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
#include <aws/core/AmazonWebServiceRequest.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/UnreferencedParam.h>
#include <aws/core/http/HttpTypes.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>

namespace Aws
{
    static const char* JSON_CONTENT_TYPE = "application/json";
    static const char* AMZN_JSON_CONTENT_TYPE_1_0 = "application/x-amz-json-1.0";
    static const char* AMZN_JSON_CONTENT_TYPE_1_1 = "application/x-amz-json-1.1";
    static const char* FORM_CONTENT_TYPE = "application/x-www-form-urlencoded";
    static const char* AMZN_XML_CONTENT_TYPE = "application/xml";

    /**
     * High-level abstraction over AWS requests. GetBody() calls SerializePayload() and uses a stringbuf under the hood.
     * This is for payloads such as query, xml, or json
     */
    class AWS_CORE_API AmazonSerializableWebServiceRequest : public AmazonWebServiceRequest
    {
    public:
        AmazonSerializableWebServiceRequest()
        {
            //prevent unreferenced var warnings
            //for these values.
            AWS_UNREFERENCED_PARAM(JSON_CONTENT_TYPE);
            AWS_UNREFERENCED_PARAM(AMZN_JSON_CONTENT_TYPE_1_0);
            AWS_UNREFERENCED_PARAM(AMZN_JSON_CONTENT_TYPE_1_1);
            AWS_UNREFERENCED_PARAM(FORM_CONTENT_TYPE);
            AWS_UNREFERENCED_PARAM(AMZN_XML_CONTENT_TYPE);
        }

        virtual ~AmazonSerializableWebServiceRequest() {}

        /**
         * Convert payload into String.
         */
        virtual Aws::String SerializePayload() const = 0;

        /**
         * Loads serialized payload into string buf and returns the stream
         */
        std::shared_ptr<Aws::IOStream> GetBody() const override;
    };

} // namespace Aws

