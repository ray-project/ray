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
   * Specifies when noncurrent object versions expire. Upon expiration, Amazon S3
   * permanently deletes the noncurrent object versions. You set this lifecycle
   * configuration action on a bucket that has versioning enabled (or suspended) to
   * request that Amazon S3 delete noncurrent object versions at a specific period in
   * the object's lifetime.<p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/NoncurrentVersionExpiration">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API NoncurrentVersionExpiration
  {
  public:
    NoncurrentVersionExpiration();
    NoncurrentVersionExpiration(const Aws::Utils::Xml::XmlNode& xmlNode);
    NoncurrentVersionExpiration& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Specifies the number of days an object is noncurrent before Amazon S3 can
     * perform the associated action. For information about the noncurrent days
     * calculations, see <a
     * href="http://docs.aws.amazon.com/AmazonS3/latest/dev/s3-access-control.html">How
     * Amazon S3 Calculates When an Object Became Noncurrent</a> in the Amazon Simple
     * Storage Service Developer Guide.
     */
    inline int GetNoncurrentDays() const{ return m_noncurrentDays; }

    /**
     * Specifies the number of days an object is noncurrent before Amazon S3 can
     * perform the associated action. For information about the noncurrent days
     * calculations, see <a
     * href="http://docs.aws.amazon.com/AmazonS3/latest/dev/s3-access-control.html">How
     * Amazon S3 Calculates When an Object Became Noncurrent</a> in the Amazon Simple
     * Storage Service Developer Guide.
     */
    inline void SetNoncurrentDays(int value) { m_noncurrentDaysHasBeenSet = true; m_noncurrentDays = value; }

    /**
     * Specifies the number of days an object is noncurrent before Amazon S3 can
     * perform the associated action. For information about the noncurrent days
     * calculations, see <a
     * href="http://docs.aws.amazon.com/AmazonS3/latest/dev/s3-access-control.html">How
     * Amazon S3 Calculates When an Object Became Noncurrent</a> in the Amazon Simple
     * Storage Service Developer Guide.
     */
    inline NoncurrentVersionExpiration& WithNoncurrentDays(int value) { SetNoncurrentDays(value); return *this;}

  private:

    int m_noncurrentDays;
    bool m_noncurrentDaysHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
