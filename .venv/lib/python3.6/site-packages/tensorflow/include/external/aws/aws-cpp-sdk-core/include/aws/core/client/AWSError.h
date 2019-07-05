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
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/StringUtils.h>

namespace Aws
{
    namespace Client
    {
        enum class CoreErrors;

        /**
         * Container for Error enumerations with additional exception information. Name, message, retryable etc....
         */
        template<typename ERROR_TYPE>
        class AWSError
        {
        public:
            /**
             * Initializes AWSError object as empty with the error not being retryable.
             */
            AWSError() : m_isRetryable(false) {}
            /**
             * Initializes AWSError object with errorType, exceptionName, message, and retryable flag.
             */
            AWSError(ERROR_TYPE errorType, Aws::String exceptionName, const Aws::String message, bool isRetryable) :
                m_errorType(errorType), m_exceptionName(exceptionName), m_message(message), m_isRetryable(isRetryable) {}
            /**
             * Initializes AWSError object with errorType and retryable flag. ExceptionName and message are empty.
             */
            AWSError(ERROR_TYPE errorType, bool isRetryable) :
                m_errorType(errorType), m_isRetryable(isRetryable) {}

            //by policy we enforce all clients to contain a CoreErrors alignment for their Errors.
            AWSError(const AWSError<CoreErrors>& rhs) :
                m_errorType(static_cast<ERROR_TYPE>(rhs.GetErrorType())), m_exceptionName(rhs.GetExceptionName()), 
                m_message(rhs.GetMessage()), m_responseHeaders(rhs.GetResponseHeaders()), 
                m_responseCode(rhs.GetResponseCode()), m_isRetryable(rhs.ShouldRetry())
            {}          

            /**
             * Gets underlying errorType.
             */
            inline const ERROR_TYPE GetErrorType() const { return m_errorType; }
            /**
             * Gets the underlying ExceptionName.
             */
            inline const Aws::String& GetExceptionName() const { return m_exceptionName; }
            /**
             *Sets the underlying ExceptionName.
             */
            inline void SetExceptionName(const Aws::String& exceptionName) { m_exceptionName = exceptionName; }
            /**
             * Gets the error message.
             */
            inline const Aws::String& GetMessage() const { return m_message; }
            /**
             * Sets the error message
             */
            inline void SetMessage(const Aws::String& message) { m_message = message; }
            /**
             * Returns whether or not this error is eligible for retry.
             */
            inline bool ShouldRetry() const { return m_isRetryable; }
            /**
             * Gets the response headers from the http response.
             */
            inline const Aws::Http::HeaderValueCollection& GetResponseHeaders() const { return m_responseHeaders; }
            /**
             * Sets the response headers from the http response.
             */
            inline void SetResponseHeaders(const Aws::Http::HeaderValueCollection& headers) { m_responseHeaders = headers; }
            /**
             * Tests whether or not a header exists.
             */
            inline bool ResponseHeaderExists(const Aws::String& headerName) const { return m_responseHeaders.find(Aws::Utils::StringUtils::ToLower(headerName.c_str())) != m_responseHeaders.end(); }
            /**
             * Gets the response code from the http response
             */
            inline Aws::Http::HttpResponseCode GetResponseCode() const { return m_responseCode; }
            /**
             * Sets the response code from the http response
             */
            inline void SetResponseCode(Aws::Http::HttpResponseCode responseCode) { m_responseCode = responseCode; }

        private:
            ERROR_TYPE m_errorType;
            Aws::String m_exceptionName;
            Aws::String m_message;
            Aws::Http::HeaderValueCollection m_responseHeaders;
            Aws::Http::HttpResponseCode m_responseCode;
            bool m_isRetryable;
        };

    } // namespace Client
} // namespace Aws
