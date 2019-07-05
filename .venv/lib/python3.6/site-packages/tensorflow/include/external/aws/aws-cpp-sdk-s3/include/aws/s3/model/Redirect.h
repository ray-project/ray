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
#include <aws/s3/model/Protocol.h>
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

  class AWS_S3_API Redirect
  {
  public:
    Redirect();
    Redirect(const Aws::Utils::Xml::XmlNode& xmlNode);
    Redirect& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * The host name to use in the redirect request.
     */
    inline const Aws::String& GetHostName() const{ return m_hostName; }

    /**
     * The host name to use in the redirect request.
     */
    inline void SetHostName(const Aws::String& value) { m_hostNameHasBeenSet = true; m_hostName = value; }

    /**
     * The host name to use in the redirect request.
     */
    inline void SetHostName(Aws::String&& value) { m_hostNameHasBeenSet = true; m_hostName = std::move(value); }

    /**
     * The host name to use in the redirect request.
     */
    inline void SetHostName(const char* value) { m_hostNameHasBeenSet = true; m_hostName.assign(value); }

    /**
     * The host name to use in the redirect request.
     */
    inline Redirect& WithHostName(const Aws::String& value) { SetHostName(value); return *this;}

    /**
     * The host name to use in the redirect request.
     */
    inline Redirect& WithHostName(Aws::String&& value) { SetHostName(std::move(value)); return *this;}

    /**
     * The host name to use in the redirect request.
     */
    inline Redirect& WithHostName(const char* value) { SetHostName(value); return *this;}


    /**
     * The HTTP redirect code to use on the response. Not required if one of the
     * siblings is present.
     */
    inline const Aws::String& GetHttpRedirectCode() const{ return m_httpRedirectCode; }

    /**
     * The HTTP redirect code to use on the response. Not required if one of the
     * siblings is present.
     */
    inline void SetHttpRedirectCode(const Aws::String& value) { m_httpRedirectCodeHasBeenSet = true; m_httpRedirectCode = value; }

    /**
     * The HTTP redirect code to use on the response. Not required if one of the
     * siblings is present.
     */
    inline void SetHttpRedirectCode(Aws::String&& value) { m_httpRedirectCodeHasBeenSet = true; m_httpRedirectCode = std::move(value); }

    /**
     * The HTTP redirect code to use on the response. Not required if one of the
     * siblings is present.
     */
    inline void SetHttpRedirectCode(const char* value) { m_httpRedirectCodeHasBeenSet = true; m_httpRedirectCode.assign(value); }

    /**
     * The HTTP redirect code to use on the response. Not required if one of the
     * siblings is present.
     */
    inline Redirect& WithHttpRedirectCode(const Aws::String& value) { SetHttpRedirectCode(value); return *this;}

    /**
     * The HTTP redirect code to use on the response. Not required if one of the
     * siblings is present.
     */
    inline Redirect& WithHttpRedirectCode(Aws::String&& value) { SetHttpRedirectCode(std::move(value)); return *this;}

    /**
     * The HTTP redirect code to use on the response. Not required if one of the
     * siblings is present.
     */
    inline Redirect& WithHttpRedirectCode(const char* value) { SetHttpRedirectCode(value); return *this;}


    /**
     * Protocol to use (http, https) when redirecting requests. The default is the
     * protocol that is used in the original request.
     */
    inline const Protocol& GetProtocol() const{ return m_protocol; }

    /**
     * Protocol to use (http, https) when redirecting requests. The default is the
     * protocol that is used in the original request.
     */
    inline void SetProtocol(const Protocol& value) { m_protocolHasBeenSet = true; m_protocol = value; }

    /**
     * Protocol to use (http, https) when redirecting requests. The default is the
     * protocol that is used in the original request.
     */
    inline void SetProtocol(Protocol&& value) { m_protocolHasBeenSet = true; m_protocol = std::move(value); }

    /**
     * Protocol to use (http, https) when redirecting requests. The default is the
     * protocol that is used in the original request.
     */
    inline Redirect& WithProtocol(const Protocol& value) { SetProtocol(value); return *this;}

    /**
     * Protocol to use (http, https) when redirecting requests. The default is the
     * protocol that is used in the original request.
     */
    inline Redirect& WithProtocol(Protocol&& value) { SetProtocol(std::move(value)); return *this;}


    /**
     * The object key prefix to use in the redirect request. For example, to redirect
     * requests for all pages with prefix docs/ (objects in the docs/ folder) to
     * documents/, you can set a condition block with KeyPrefixEquals set to docs/ and
     * in the Redirect set ReplaceKeyPrefixWith to /documents. Not required if one of
     * the siblings is present. Can be present only if ReplaceKeyWith is not provided.
     */
    inline const Aws::String& GetReplaceKeyPrefixWith() const{ return m_replaceKeyPrefixWith; }

    /**
     * The object key prefix to use in the redirect request. For example, to redirect
     * requests for all pages with prefix docs/ (objects in the docs/ folder) to
     * documents/, you can set a condition block with KeyPrefixEquals set to docs/ and
     * in the Redirect set ReplaceKeyPrefixWith to /documents. Not required if one of
     * the siblings is present. Can be present only if ReplaceKeyWith is not provided.
     */
    inline void SetReplaceKeyPrefixWith(const Aws::String& value) { m_replaceKeyPrefixWithHasBeenSet = true; m_replaceKeyPrefixWith = value; }

    /**
     * The object key prefix to use in the redirect request. For example, to redirect
     * requests for all pages with prefix docs/ (objects in the docs/ folder) to
     * documents/, you can set a condition block with KeyPrefixEquals set to docs/ and
     * in the Redirect set ReplaceKeyPrefixWith to /documents. Not required if one of
     * the siblings is present. Can be present only if ReplaceKeyWith is not provided.
     */
    inline void SetReplaceKeyPrefixWith(Aws::String&& value) { m_replaceKeyPrefixWithHasBeenSet = true; m_replaceKeyPrefixWith = std::move(value); }

    /**
     * The object key prefix to use in the redirect request. For example, to redirect
     * requests for all pages with prefix docs/ (objects in the docs/ folder) to
     * documents/, you can set a condition block with KeyPrefixEquals set to docs/ and
     * in the Redirect set ReplaceKeyPrefixWith to /documents. Not required if one of
     * the siblings is present. Can be present only if ReplaceKeyWith is not provided.
     */
    inline void SetReplaceKeyPrefixWith(const char* value) { m_replaceKeyPrefixWithHasBeenSet = true; m_replaceKeyPrefixWith.assign(value); }

    /**
     * The object key prefix to use in the redirect request. For example, to redirect
     * requests for all pages with prefix docs/ (objects in the docs/ folder) to
     * documents/, you can set a condition block with KeyPrefixEquals set to docs/ and
     * in the Redirect set ReplaceKeyPrefixWith to /documents. Not required if one of
     * the siblings is present. Can be present only if ReplaceKeyWith is not provided.
     */
    inline Redirect& WithReplaceKeyPrefixWith(const Aws::String& value) { SetReplaceKeyPrefixWith(value); return *this;}

    /**
     * The object key prefix to use in the redirect request. For example, to redirect
     * requests for all pages with prefix docs/ (objects in the docs/ folder) to
     * documents/, you can set a condition block with KeyPrefixEquals set to docs/ and
     * in the Redirect set ReplaceKeyPrefixWith to /documents. Not required if one of
     * the siblings is present. Can be present only if ReplaceKeyWith is not provided.
     */
    inline Redirect& WithReplaceKeyPrefixWith(Aws::String&& value) { SetReplaceKeyPrefixWith(std::move(value)); return *this;}

    /**
     * The object key prefix to use in the redirect request. For example, to redirect
     * requests for all pages with prefix docs/ (objects in the docs/ folder) to
     * documents/, you can set a condition block with KeyPrefixEquals set to docs/ and
     * in the Redirect set ReplaceKeyPrefixWith to /documents. Not required if one of
     * the siblings is present. Can be present only if ReplaceKeyWith is not provided.
     */
    inline Redirect& WithReplaceKeyPrefixWith(const char* value) { SetReplaceKeyPrefixWith(value); return *this;}


    /**
     * The specific object key to use in the redirect request. For example, redirect
     * request to error.html. Not required if one of the sibling is present. Can be
     * present only if ReplaceKeyPrefixWith is not provided.
     */
    inline const Aws::String& GetReplaceKeyWith() const{ return m_replaceKeyWith; }

    /**
     * The specific object key to use in the redirect request. For example, redirect
     * request to error.html. Not required if one of the sibling is present. Can be
     * present only if ReplaceKeyPrefixWith is not provided.
     */
    inline void SetReplaceKeyWith(const Aws::String& value) { m_replaceKeyWithHasBeenSet = true; m_replaceKeyWith = value; }

    /**
     * The specific object key to use in the redirect request. For example, redirect
     * request to error.html. Not required if one of the sibling is present. Can be
     * present only if ReplaceKeyPrefixWith is not provided.
     */
    inline void SetReplaceKeyWith(Aws::String&& value) { m_replaceKeyWithHasBeenSet = true; m_replaceKeyWith = std::move(value); }

    /**
     * The specific object key to use in the redirect request. For example, redirect
     * request to error.html. Not required if one of the sibling is present. Can be
     * present only if ReplaceKeyPrefixWith is not provided.
     */
    inline void SetReplaceKeyWith(const char* value) { m_replaceKeyWithHasBeenSet = true; m_replaceKeyWith.assign(value); }

    /**
     * The specific object key to use in the redirect request. For example, redirect
     * request to error.html. Not required if one of the sibling is present. Can be
     * present only if ReplaceKeyPrefixWith is not provided.
     */
    inline Redirect& WithReplaceKeyWith(const Aws::String& value) { SetReplaceKeyWith(value); return *this;}

    /**
     * The specific object key to use in the redirect request. For example, redirect
     * request to error.html. Not required if one of the sibling is present. Can be
     * present only if ReplaceKeyPrefixWith is not provided.
     */
    inline Redirect& WithReplaceKeyWith(Aws::String&& value) { SetReplaceKeyWith(std::move(value)); return *this;}

    /**
     * The specific object key to use in the redirect request. For example, redirect
     * request to error.html. Not required if one of the sibling is present. Can be
     * present only if ReplaceKeyPrefixWith is not provided.
     */
    inline Redirect& WithReplaceKeyWith(const char* value) { SetReplaceKeyWith(value); return *this;}

  private:

    Aws::String m_hostName;
    bool m_hostNameHasBeenSet;

    Aws::String m_httpRedirectCode;
    bool m_httpRedirectCodeHasBeenSet;

    Protocol m_protocol;
    bool m_protocolHasBeenSet;

    Aws::String m_replaceKeyPrefixWith;
    bool m_replaceKeyPrefixWithHasBeenSet;

    Aws::String m_replaceKeyWith;
    bool m_replaceKeyWithHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
