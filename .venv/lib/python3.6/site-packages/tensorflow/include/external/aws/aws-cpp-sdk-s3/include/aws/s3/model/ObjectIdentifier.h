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

  class AWS_S3_API ObjectIdentifier
  {
  public:
    ObjectIdentifier();
    ObjectIdentifier(const Aws::Utils::Xml::XmlNode& xmlNode);
    ObjectIdentifier& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Key name of the object to delete.
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * Key name of the object to delete.
     */
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    /**
     * Key name of the object to delete.
     */
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    /**
     * Key name of the object to delete.
     */
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    /**
     * Key name of the object to delete.
     */
    inline ObjectIdentifier& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * Key name of the object to delete.
     */
    inline ObjectIdentifier& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * Key name of the object to delete.
     */
    inline ObjectIdentifier& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * VersionId for the specific version of the object to delete.
     */
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    /**
     * VersionId for the specific version of the object to delete.
     */
    inline void SetVersionId(const Aws::String& value) { m_versionIdHasBeenSet = true; m_versionId = value; }

    /**
     * VersionId for the specific version of the object to delete.
     */
    inline void SetVersionId(Aws::String&& value) { m_versionIdHasBeenSet = true; m_versionId = std::move(value); }

    /**
     * VersionId for the specific version of the object to delete.
     */
    inline void SetVersionId(const char* value) { m_versionIdHasBeenSet = true; m_versionId.assign(value); }

    /**
     * VersionId for the specific version of the object to delete.
     */
    inline ObjectIdentifier& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    /**
     * VersionId for the specific version of the object to delete.
     */
    inline ObjectIdentifier& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    /**
     * VersionId for the specific version of the object to delete.
     */
    inline ObjectIdentifier& WithVersionId(const char* value) { SetVersionId(value); return *this;}

  private:

    Aws::String m_key;
    bool m_keyHasBeenSet;

    Aws::String m_versionId;
    bool m_versionIdHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
