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
#include <aws/s3/model/OwnerOverride.h>
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

  /**
   * Container for information regarding the access control for replicas.<p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/AccessControlTranslation">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API AccessControlTranslation
  {
  public:
    AccessControlTranslation();
    AccessControlTranslation(const Aws::Utils::Xml::XmlNode& xmlNode);
    AccessControlTranslation& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * The override value for the owner of the replica object.
     */
    inline const OwnerOverride& GetOwner() const{ return m_owner; }

    /**
     * The override value for the owner of the replica object.
     */
    inline void SetOwner(const OwnerOverride& value) { m_ownerHasBeenSet = true; m_owner = value; }

    /**
     * The override value for the owner of the replica object.
     */
    inline void SetOwner(OwnerOverride&& value) { m_ownerHasBeenSet = true; m_owner = std::move(value); }

    /**
     * The override value for the owner of the replica object.
     */
    inline AccessControlTranslation& WithOwner(const OwnerOverride& value) { SetOwner(value); return *this;}

    /**
     * The override value for the owner of the replica object.
     */
    inline AccessControlTranslation& WithOwner(OwnerOverride&& value) { SetOwner(std::move(value)); return *this;}

  private:

    OwnerOverride m_owner;
    bool m_ownerHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
