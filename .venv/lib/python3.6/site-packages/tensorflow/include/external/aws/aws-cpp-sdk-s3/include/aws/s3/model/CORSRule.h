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
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <utility>

namespace Aws
{
namespace Utils
{
namespace Xml
{
  class XmlNode;
} // namespace Xml
} // namespace Utils
namespace S3
{
namespace Model
{

  class AWS_S3_API CORSRule
  {
  public:
    CORSRule();
    CORSRule(const Aws::Utils::Xml::XmlNode& xmlNode);
    CORSRule& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Specifies which headers are allowed in a pre-flight OPTIONS request.
     */
    inline const Aws::Vector<Aws::String>& GetAllowedHeaders() const{ return m_allowedHeaders; }

    /**
     * Specifies which headers are allowed in a pre-flight OPTIONS request.
     */
    inline void SetAllowedHeaders(const Aws::Vector<Aws::String>& value) { m_allowedHeadersHasBeenSet = true; m_allowedHeaders = value; }

    /**
     * Specifies which headers are allowed in a pre-flight OPTIONS request.
     */
    inline void SetAllowedHeaders(Aws::Vector<Aws::String>&& value) { m_allowedHeadersHasBeenSet = true; m_allowedHeaders = std::move(value); }

    /**
     * Specifies which headers are allowed in a pre-flight OPTIONS request.
     */
    inline CORSRule& WithAllowedHeaders(const Aws::Vector<Aws::String>& value) { SetAllowedHeaders(value); return *this;}

    /**
     * Specifies which headers are allowed in a pre-flight OPTIONS request.
     */
    inline CORSRule& WithAllowedHeaders(Aws::Vector<Aws::String>&& value) { SetAllowedHeaders(std::move(value)); return *this;}

    /**
     * Specifies which headers are allowed in a pre-flight OPTIONS request.
     */
    inline CORSRule& AddAllowedHeaders(const Aws::String& value) { m_allowedHeadersHasBeenSet = true; m_allowedHeaders.push_back(value); return *this; }

    /**
     * Specifies which headers are allowed in a pre-flight OPTIONS request.
     */
    inline CORSRule& AddAllowedHeaders(Aws::String&& value) { m_allowedHeadersHasBeenSet = true; m_allowedHeaders.push_back(std::move(value)); return *this; }

    /**
     * Specifies which headers are allowed in a pre-flight OPTIONS request.
     */
    inline CORSRule& AddAllowedHeaders(const char* value) { m_allowedHeadersHasBeenSet = true; m_allowedHeaders.push_back(value); return *this; }


    /**
     * Identifies HTTP methods that the domain/origin specified in the rule is allowed
     * to execute.
     */
    inline const Aws::Vector<Aws::String>& GetAllowedMethods() const{ return m_allowedMethods; }

    /**
     * Identifies HTTP methods that the domain/origin specified in the rule is allowed
     * to execute.
     */
    inline void SetAllowedMethods(const Aws::Vector<Aws::String>& value) { m_allowedMethodsHasBeenSet = true; m_allowedMethods = value; }

    /**
     * Identifies HTTP methods that the domain/origin specified in the rule is allowed
     * to execute.
     */
    inline void SetAllowedMethods(Aws::Vector<Aws::String>&& value) { m_allowedMethodsHasBeenSet = true; m_allowedMethods = std::move(value); }

    /**
     * Identifies HTTP methods that the domain/origin specified in the rule is allowed
     * to execute.
     */
    inline CORSRule& WithAllowedMethods(const Aws::Vector<Aws::String>& value) { SetAllowedMethods(value); return *this;}

    /**
     * Identifies HTTP methods that the domain/origin specified in the rule is allowed
     * to execute.
     */
    inline CORSRule& WithAllowedMethods(Aws::Vector<Aws::String>&& value) { SetAllowedMethods(std::move(value)); return *this;}

    /**
     * Identifies HTTP methods that the domain/origin specified in the rule is allowed
     * to execute.
     */
    inline CORSRule& AddAllowedMethods(const Aws::String& value) { m_allowedMethodsHasBeenSet = true; m_allowedMethods.push_back(value); return *this; }

    /**
     * Identifies HTTP methods that the domain/origin specified in the rule is allowed
     * to execute.
     */
    inline CORSRule& AddAllowedMethods(Aws::String&& value) { m_allowedMethodsHasBeenSet = true; m_allowedMethods.push_back(std::move(value)); return *this; }

    /**
     * Identifies HTTP methods that the domain/origin specified in the rule is allowed
     * to execute.
     */
    inline CORSRule& AddAllowedMethods(const char* value) { m_allowedMethodsHasBeenSet = true; m_allowedMethods.push_back(value); return *this; }


    /**
     * One or more origins you want customers to be able to access the bucket from.
     */
    inline const Aws::Vector<Aws::String>& GetAllowedOrigins() const{ return m_allowedOrigins; }

    /**
     * One or more origins you want customers to be able to access the bucket from.
     */
    inline void SetAllowedOrigins(const Aws::Vector<Aws::String>& value) { m_allowedOriginsHasBeenSet = true; m_allowedOrigins = value; }

    /**
     * One or more origins you want customers to be able to access the bucket from.
     */
    inline void SetAllowedOrigins(Aws::Vector<Aws::String>&& value) { m_allowedOriginsHasBeenSet = true; m_allowedOrigins = std::move(value); }

    /**
     * One or more origins you want customers to be able to access the bucket from.
     */
    inline CORSRule& WithAllowedOrigins(const Aws::Vector<Aws::String>& value) { SetAllowedOrigins(value); return *this;}

    /**
     * One or more origins you want customers to be able to access the bucket from.
     */
    inline CORSRule& WithAllowedOrigins(Aws::Vector<Aws::String>&& value) { SetAllowedOrigins(std::move(value)); return *this;}

    /**
     * One or more origins you want customers to be able to access the bucket from.
     */
    inline CORSRule& AddAllowedOrigins(const Aws::String& value) { m_allowedOriginsHasBeenSet = true; m_allowedOrigins.push_back(value); return *this; }

    /**
     * One or more origins you want customers to be able to access the bucket from.
     */
    inline CORSRule& AddAllowedOrigins(Aws::String&& value) { m_allowedOriginsHasBeenSet = true; m_allowedOrigins.push_back(std::move(value)); return *this; }

    /**
     * One or more origins you want customers to be able to access the bucket from.
     */
    inline CORSRule& AddAllowedOrigins(const char* value) { m_allowedOriginsHasBeenSet = true; m_allowedOrigins.push_back(value); return *this; }


    /**
     * One or more headers in the response that you want customers to be able to access
     * from their applications (for example, from a JavaScript XMLHttpRequest object).
     */
    inline const Aws::Vector<Aws::String>& GetExposeHeaders() const{ return m_exposeHeaders; }

    /**
     * One or more headers in the response that you want customers to be able to access
     * from their applications (for example, from a JavaScript XMLHttpRequest object).
     */
    inline void SetExposeHeaders(const Aws::Vector<Aws::String>& value) { m_exposeHeadersHasBeenSet = true; m_exposeHeaders = value; }

    /**
     * One or more headers in the response that you want customers to be able to access
     * from their applications (for example, from a JavaScript XMLHttpRequest object).
     */
    inline void SetExposeHeaders(Aws::Vector<Aws::String>&& value) { m_exposeHeadersHasBeenSet = true; m_exposeHeaders = std::move(value); }

    /**
     * One or more headers in the response that you want customers to be able to access
     * from their applications (for example, from a JavaScript XMLHttpRequest object).
     */
    inline CORSRule& WithExposeHeaders(const Aws::Vector<Aws::String>& value) { SetExposeHeaders(value); return *this;}

    /**
     * One or more headers in the response that you want customers to be able to access
     * from their applications (for example, from a JavaScript XMLHttpRequest object).
     */
    inline CORSRule& WithExposeHeaders(Aws::Vector<Aws::String>&& value) { SetExposeHeaders(std::move(value)); return *this;}

    /**
     * One or more headers in the response that you want customers to be able to access
     * from their applications (for example, from a JavaScript XMLHttpRequest object).
     */
    inline CORSRule& AddExposeHeaders(const Aws::String& value) { m_exposeHeadersHasBeenSet = true; m_exposeHeaders.push_back(value); return *this; }

    /**
     * One or more headers in the response that you want customers to be able to access
     * from their applications (for example, from a JavaScript XMLHttpRequest object).
     */
    inline CORSRule& AddExposeHeaders(Aws::String&& value) { m_exposeHeadersHasBeenSet = true; m_exposeHeaders.push_back(std::move(value)); return *this; }

    /**
     * One or more headers in the response that you want customers to be able to access
     * from their applications (for example, from a JavaScript XMLHttpRequest object).
     */
    inline CORSRule& AddExposeHeaders(const char* value) { m_exposeHeadersHasBeenSet = true; m_exposeHeaders.push_back(value); return *this; }


    /**
     * The time in seconds that your browser is to cache the preflight response for the
     * specified resource.
     */
    inline int GetMaxAgeSeconds() const{ return m_maxAgeSeconds; }

    /**
     * The time in seconds that your browser is to cache the preflight response for the
     * specified resource.
     */
    inline void SetMaxAgeSeconds(int value) { m_maxAgeSecondsHasBeenSet = true; m_maxAgeSeconds = value; }

    /**
     * The time in seconds that your browser is to cache the preflight response for the
     * specified resource.
     */
    inline CORSRule& WithMaxAgeSeconds(int value) { SetMaxAgeSeconds(value); return *this;}

  private:

    Aws::Vector<Aws::String> m_allowedHeaders;
    bool m_allowedHeadersHasBeenSet;

    Aws::Vector<Aws::String> m_allowedMethods;
    bool m_allowedMethodsHasBeenSet;

    Aws::Vector<Aws::String> m_allowedOrigins;
    bool m_allowedOriginsHasBeenSet;

    Aws::Vector<Aws::String> m_exposeHeaders;
    bool m_exposeHeadersHasBeenSet;

    int m_maxAgeSeconds;
    bool m_maxAgeSecondsHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
