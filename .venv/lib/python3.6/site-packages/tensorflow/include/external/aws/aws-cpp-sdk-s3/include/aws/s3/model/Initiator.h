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

  class AWS_S3_API Initiator
  {
  public:
    Initiator();
    Initiator(const Aws::Utils::Xml::XmlNode& xmlNode);
    Initiator& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * If the principal is an AWS account, it provides the Canonical User ID. If the
     * principal is an IAM User, it provides a user ARN value.
     */
    inline const Aws::String& GetID() const{ return m_iD; }

    /**
     * If the principal is an AWS account, it provides the Canonical User ID. If the
     * principal is an IAM User, it provides a user ARN value.
     */
    inline void SetID(const Aws::String& value) { m_iDHasBeenSet = true; m_iD = value; }

    /**
     * If the principal is an AWS account, it provides the Canonical User ID. If the
     * principal is an IAM User, it provides a user ARN value.
     */
    inline void SetID(Aws::String&& value) { m_iDHasBeenSet = true; m_iD = std::move(value); }

    /**
     * If the principal is an AWS account, it provides the Canonical User ID. If the
     * principal is an IAM User, it provides a user ARN value.
     */
    inline void SetID(const char* value) { m_iDHasBeenSet = true; m_iD.assign(value); }

    /**
     * If the principal is an AWS account, it provides the Canonical User ID. If the
     * principal is an IAM User, it provides a user ARN value.
     */
    inline Initiator& WithID(const Aws::String& value) { SetID(value); return *this;}

    /**
     * If the principal is an AWS account, it provides the Canonical User ID. If the
     * principal is an IAM User, it provides a user ARN value.
     */
    inline Initiator& WithID(Aws::String&& value) { SetID(std::move(value)); return *this;}

    /**
     * If the principal is an AWS account, it provides the Canonical User ID. If the
     * principal is an IAM User, it provides a user ARN value.
     */
    inline Initiator& WithID(const char* value) { SetID(value); return *this;}


    /**
     * Name of the Principal.
     */
    inline const Aws::String& GetDisplayName() const{ return m_displayName; }

    /**
     * Name of the Principal.
     */
    inline void SetDisplayName(const Aws::String& value) { m_displayNameHasBeenSet = true; m_displayName = value; }

    /**
     * Name of the Principal.
     */
    inline void SetDisplayName(Aws::String&& value) { m_displayNameHasBeenSet = true; m_displayName = std::move(value); }

    /**
     * Name of the Principal.
     */
    inline void SetDisplayName(const char* value) { m_displayNameHasBeenSet = true; m_displayName.assign(value); }

    /**
     * Name of the Principal.
     */
    inline Initiator& WithDisplayName(const Aws::String& value) { SetDisplayName(value); return *this;}

    /**
     * Name of the Principal.
     */
    inline Initiator& WithDisplayName(Aws::String&& value) { SetDisplayName(std::move(value)); return *this;}

    /**
     * Name of the Principal.
     */
    inline Initiator& WithDisplayName(const char* value) { SetDisplayName(value); return *this;}

  private:

    Aws::String m_iD;
    bool m_iDHasBeenSet;

    Aws::String m_displayName;
    bool m_displayNameHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
