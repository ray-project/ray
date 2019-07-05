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

  class AWS_S3_API RedirectAllRequestsTo
  {
  public:
    RedirectAllRequestsTo();
    RedirectAllRequestsTo(const Aws::Utils::Xml::XmlNode& xmlNode);
    RedirectAllRequestsTo& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Name of the host where requests will be redirected.
     */
    inline const Aws::String& GetHostName() const{ return m_hostName; }

    /**
     * Name of the host where requests will be redirected.
     */
    inline void SetHostName(const Aws::String& value) { m_hostNameHasBeenSet = true; m_hostName = value; }

    /**
     * Name of the host where requests will be redirected.
     */
    inline void SetHostName(Aws::String&& value) { m_hostNameHasBeenSet = true; m_hostName = std::move(value); }

    /**
     * Name of the host where requests will be redirected.
     */
    inline void SetHostName(const char* value) { m_hostNameHasBeenSet = true; m_hostName.assign(value); }

    /**
     * Name of the host where requests will be redirected.
     */
    inline RedirectAllRequestsTo& WithHostName(const Aws::String& value) { SetHostName(value); return *this;}

    /**
     * Name of the host where requests will be redirected.
     */
    inline RedirectAllRequestsTo& WithHostName(Aws::String&& value) { SetHostName(std::move(value)); return *this;}

    /**
     * Name of the host where requests will be redirected.
     */
    inline RedirectAllRequestsTo& WithHostName(const char* value) { SetHostName(value); return *this;}


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
    inline RedirectAllRequestsTo& WithProtocol(const Protocol& value) { SetProtocol(value); return *this;}

    /**
     * Protocol to use (http, https) when redirecting requests. The default is the
     * protocol that is used in the original request.
     */
    inline RedirectAllRequestsTo& WithProtocol(Protocol&& value) { SetProtocol(std::move(value)); return *this;}

  private:

    Aws::String m_hostName;
    bool m_hostNameHasBeenSet;

    Protocol m_protocol;
    bool m_protocolHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
