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
#include <aws/core/utils/DateTime.h>
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

  class AWS_S3_API CopyPartResult
  {
  public:
    CopyPartResult();
    CopyPartResult(const Aws::Utils::Xml::XmlNode& xmlNode);
    CopyPartResult& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Entity tag of the object.
     */
    inline const Aws::String& GetETag() const{ return m_eTag; }

    /**
     * Entity tag of the object.
     */
    inline void SetETag(const Aws::String& value) { m_eTagHasBeenSet = true; m_eTag = value; }

    /**
     * Entity tag of the object.
     */
    inline void SetETag(Aws::String&& value) { m_eTagHasBeenSet = true; m_eTag = std::move(value); }

    /**
     * Entity tag of the object.
     */
    inline void SetETag(const char* value) { m_eTagHasBeenSet = true; m_eTag.assign(value); }

    /**
     * Entity tag of the object.
     */
    inline CopyPartResult& WithETag(const Aws::String& value) { SetETag(value); return *this;}

    /**
     * Entity tag of the object.
     */
    inline CopyPartResult& WithETag(Aws::String&& value) { SetETag(std::move(value)); return *this;}

    /**
     * Entity tag of the object.
     */
    inline CopyPartResult& WithETag(const char* value) { SetETag(value); return *this;}


    /**
     * Date and time at which the object was uploaded.
     */
    inline const Aws::Utils::DateTime& GetLastModified() const{ return m_lastModified; }

    /**
     * Date and time at which the object was uploaded.
     */
    inline void SetLastModified(const Aws::Utils::DateTime& value) { m_lastModifiedHasBeenSet = true; m_lastModified = value; }

    /**
     * Date and time at which the object was uploaded.
     */
    inline void SetLastModified(Aws::Utils::DateTime&& value) { m_lastModifiedHasBeenSet = true; m_lastModified = std::move(value); }

    /**
     * Date and time at which the object was uploaded.
     */
    inline CopyPartResult& WithLastModified(const Aws::Utils::DateTime& value) { SetLastModified(value); return *this;}

    /**
     * Date and time at which the object was uploaded.
     */
    inline CopyPartResult& WithLastModified(Aws::Utils::DateTime&& value) { SetLastModified(std::move(value)); return *this;}

  private:

    Aws::String m_eTag;
    bool m_eTagHasBeenSet;

    Aws::Utils::DateTime m_lastModified;
    bool m_lastModifiedHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
