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

  class AWS_S3_API Condition
  {
  public:
    Condition();
    Condition(const Aws::Utils::Xml::XmlNode& xmlNode);
    Condition& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * The HTTP error code when the redirect is applied. In the event of an error, if
     * the error code equals this value, then the specified redirect is applied.
     * Required when parent element Condition is specified and sibling KeyPrefixEquals
     * is not specified. If both are specified, then both must be true for the redirect
     * to be applied.
     */
    inline const Aws::String& GetHttpErrorCodeReturnedEquals() const{ return m_httpErrorCodeReturnedEquals; }

    /**
     * The HTTP error code when the redirect is applied. In the event of an error, if
     * the error code equals this value, then the specified redirect is applied.
     * Required when parent element Condition is specified and sibling KeyPrefixEquals
     * is not specified. If both are specified, then both must be true for the redirect
     * to be applied.
     */
    inline void SetHttpErrorCodeReturnedEquals(const Aws::String& value) { m_httpErrorCodeReturnedEqualsHasBeenSet = true; m_httpErrorCodeReturnedEquals = value; }

    /**
     * The HTTP error code when the redirect is applied. In the event of an error, if
     * the error code equals this value, then the specified redirect is applied.
     * Required when parent element Condition is specified and sibling KeyPrefixEquals
     * is not specified. If both are specified, then both must be true for the redirect
     * to be applied.
     */
    inline void SetHttpErrorCodeReturnedEquals(Aws::String&& value) { m_httpErrorCodeReturnedEqualsHasBeenSet = true; m_httpErrorCodeReturnedEquals = std::move(value); }

    /**
     * The HTTP error code when the redirect is applied. In the event of an error, if
     * the error code equals this value, then the specified redirect is applied.
     * Required when parent element Condition is specified and sibling KeyPrefixEquals
     * is not specified. If both are specified, then both must be true for the redirect
     * to be applied.
     */
    inline void SetHttpErrorCodeReturnedEquals(const char* value) { m_httpErrorCodeReturnedEqualsHasBeenSet = true; m_httpErrorCodeReturnedEquals.assign(value); }

    /**
     * The HTTP error code when the redirect is applied. In the event of an error, if
     * the error code equals this value, then the specified redirect is applied.
     * Required when parent element Condition is specified and sibling KeyPrefixEquals
     * is not specified. If both are specified, then both must be true for the redirect
     * to be applied.
     */
    inline Condition& WithHttpErrorCodeReturnedEquals(const Aws::String& value) { SetHttpErrorCodeReturnedEquals(value); return *this;}

    /**
     * The HTTP error code when the redirect is applied. In the event of an error, if
     * the error code equals this value, then the specified redirect is applied.
     * Required when parent element Condition is specified and sibling KeyPrefixEquals
     * is not specified. If both are specified, then both must be true for the redirect
     * to be applied.
     */
    inline Condition& WithHttpErrorCodeReturnedEquals(Aws::String&& value) { SetHttpErrorCodeReturnedEquals(std::move(value)); return *this;}

    /**
     * The HTTP error code when the redirect is applied. In the event of an error, if
     * the error code equals this value, then the specified redirect is applied.
     * Required when parent element Condition is specified and sibling KeyPrefixEquals
     * is not specified. If both are specified, then both must be true for the redirect
     * to be applied.
     */
    inline Condition& WithHttpErrorCodeReturnedEquals(const char* value) { SetHttpErrorCodeReturnedEquals(value); return *this;}


    /**
     * The object key name prefix when the redirect is applied. For example, to
     * redirect requests for ExamplePage.html, the key prefix will be ExamplePage.html.
     * To redirect request for all pages with the prefix docs/, the key prefix will be
     * /docs, which identifies all objects in the docs/ folder. Required when the
     * parent element Condition is specified and sibling HttpErrorCodeReturnedEquals is
     * not specified. If both conditions are specified, both must be true for the
     * redirect to be applied.
     */
    inline const Aws::String& GetKeyPrefixEquals() const{ return m_keyPrefixEquals; }

    /**
     * The object key name prefix when the redirect is applied. For example, to
     * redirect requests for ExamplePage.html, the key prefix will be ExamplePage.html.
     * To redirect request for all pages with the prefix docs/, the key prefix will be
     * /docs, which identifies all objects in the docs/ folder. Required when the
     * parent element Condition is specified and sibling HttpErrorCodeReturnedEquals is
     * not specified. If both conditions are specified, both must be true for the
     * redirect to be applied.
     */
    inline void SetKeyPrefixEquals(const Aws::String& value) { m_keyPrefixEqualsHasBeenSet = true; m_keyPrefixEquals = value; }

    /**
     * The object key name prefix when the redirect is applied. For example, to
     * redirect requests for ExamplePage.html, the key prefix will be ExamplePage.html.
     * To redirect request for all pages with the prefix docs/, the key prefix will be
     * /docs, which identifies all objects in the docs/ folder. Required when the
     * parent element Condition is specified and sibling HttpErrorCodeReturnedEquals is
     * not specified. If both conditions are specified, both must be true for the
     * redirect to be applied.
     */
    inline void SetKeyPrefixEquals(Aws::String&& value) { m_keyPrefixEqualsHasBeenSet = true; m_keyPrefixEquals = std::move(value); }

    /**
     * The object key name prefix when the redirect is applied. For example, to
     * redirect requests for ExamplePage.html, the key prefix will be ExamplePage.html.
     * To redirect request for all pages with the prefix docs/, the key prefix will be
     * /docs, which identifies all objects in the docs/ folder. Required when the
     * parent element Condition is specified and sibling HttpErrorCodeReturnedEquals is
     * not specified. If both conditions are specified, both must be true for the
     * redirect to be applied.
     */
    inline void SetKeyPrefixEquals(const char* value) { m_keyPrefixEqualsHasBeenSet = true; m_keyPrefixEquals.assign(value); }

    /**
     * The object key name prefix when the redirect is applied. For example, to
     * redirect requests for ExamplePage.html, the key prefix will be ExamplePage.html.
     * To redirect request for all pages with the prefix docs/, the key prefix will be
     * /docs, which identifies all objects in the docs/ folder. Required when the
     * parent element Condition is specified and sibling HttpErrorCodeReturnedEquals is
     * not specified. If both conditions are specified, both must be true for the
     * redirect to be applied.
     */
    inline Condition& WithKeyPrefixEquals(const Aws::String& value) { SetKeyPrefixEquals(value); return *this;}

    /**
     * The object key name prefix when the redirect is applied. For example, to
     * redirect requests for ExamplePage.html, the key prefix will be ExamplePage.html.
     * To redirect request for all pages with the prefix docs/, the key prefix will be
     * /docs, which identifies all objects in the docs/ folder. Required when the
     * parent element Condition is specified and sibling HttpErrorCodeReturnedEquals is
     * not specified. If both conditions are specified, both must be true for the
     * redirect to be applied.
     */
    inline Condition& WithKeyPrefixEquals(Aws::String&& value) { SetKeyPrefixEquals(std::move(value)); return *this;}

    /**
     * The object key name prefix when the redirect is applied. For example, to
     * redirect requests for ExamplePage.html, the key prefix will be ExamplePage.html.
     * To redirect request for all pages with the prefix docs/, the key prefix will be
     * /docs, which identifies all objects in the docs/ folder. Required when the
     * parent element Condition is specified and sibling HttpErrorCodeReturnedEquals is
     * not specified. If both conditions are specified, both must be true for the
     * redirect to be applied.
     */
    inline Condition& WithKeyPrefixEquals(const char* value) { SetKeyPrefixEquals(value); return *this;}

  private:

    Aws::String m_httpErrorCodeReturnedEquals;
    bool m_httpErrorCodeReturnedEqualsHasBeenSet;

    Aws::String m_keyPrefixEquals;
    bool m_keyPrefixEqualsHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
