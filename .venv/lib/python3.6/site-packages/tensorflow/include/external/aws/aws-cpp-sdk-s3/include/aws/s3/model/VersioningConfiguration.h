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
#include <aws/s3/model/MFADelete.h>
#include <aws/s3/model/BucketVersioningStatus.h>
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

  class AWS_S3_API VersioningConfiguration
  {
  public:
    VersioningConfiguration();
    VersioningConfiguration(const Aws::Utils::Xml::XmlNode& xmlNode);
    VersioningConfiguration& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Specifies whether MFA delete is enabled in the bucket versioning configuration.
     * This element is only returned if the bucket has been configured with MFA delete.
     * If the bucket has never been so configured, this element is not returned.
     */
    inline const MFADelete& GetMFADelete() const{ return m_mFADelete; }

    /**
     * Specifies whether MFA delete is enabled in the bucket versioning configuration.
     * This element is only returned if the bucket has been configured with MFA delete.
     * If the bucket has never been so configured, this element is not returned.
     */
    inline void SetMFADelete(const MFADelete& value) { m_mFADeleteHasBeenSet = true; m_mFADelete = value; }

    /**
     * Specifies whether MFA delete is enabled in the bucket versioning configuration.
     * This element is only returned if the bucket has been configured with MFA delete.
     * If the bucket has never been so configured, this element is not returned.
     */
    inline void SetMFADelete(MFADelete&& value) { m_mFADeleteHasBeenSet = true; m_mFADelete = std::move(value); }

    /**
     * Specifies whether MFA delete is enabled in the bucket versioning configuration.
     * This element is only returned if the bucket has been configured with MFA delete.
     * If the bucket has never been so configured, this element is not returned.
     */
    inline VersioningConfiguration& WithMFADelete(const MFADelete& value) { SetMFADelete(value); return *this;}

    /**
     * Specifies whether MFA delete is enabled in the bucket versioning configuration.
     * This element is only returned if the bucket has been configured with MFA delete.
     * If the bucket has never been so configured, this element is not returned.
     */
    inline VersioningConfiguration& WithMFADelete(MFADelete&& value) { SetMFADelete(std::move(value)); return *this;}


    /**
     * The versioning state of the bucket.
     */
    inline const BucketVersioningStatus& GetStatus() const{ return m_status; }

    /**
     * The versioning state of the bucket.
     */
    inline void SetStatus(const BucketVersioningStatus& value) { m_statusHasBeenSet = true; m_status = value; }

    /**
     * The versioning state of the bucket.
     */
    inline void SetStatus(BucketVersioningStatus&& value) { m_statusHasBeenSet = true; m_status = std::move(value); }

    /**
     * The versioning state of the bucket.
     */
    inline VersioningConfiguration& WithStatus(const BucketVersioningStatus& value) { SetStatus(value); return *this;}

    /**
     * The versioning state of the bucket.
     */
    inline VersioningConfiguration& WithStatus(BucketVersioningStatus&& value) { SetStatus(std::move(value)); return *this;}

  private:

    MFADelete m_mFADelete;
    bool m_mFADeleteHasBeenSet;

    BucketVersioningStatus m_status;
    bool m_statusHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
