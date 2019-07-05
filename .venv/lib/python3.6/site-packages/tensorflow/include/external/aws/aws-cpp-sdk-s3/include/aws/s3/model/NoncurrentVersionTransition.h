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
#include <aws/s3/model/TransitionStorageClass.h>
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
   * Container for the transition rule that describes when noncurrent objects
   * transition to the STANDARD_IA or GLACIER storage class. If your bucket is
   * versioning-enabled (or versioning is suspended), you can set this action to
   * request that Amazon S3 transition noncurrent object versions to the STANDARD_IA
   * or GLACIER storage class at a specific period in the object's
   * lifetime.<p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/NoncurrentVersionTransition">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API NoncurrentVersionTransition
  {
  public:
    NoncurrentVersionTransition();
    NoncurrentVersionTransition(const Aws::Utils::Xml::XmlNode& xmlNode);
    NoncurrentVersionTransition& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

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
    inline NoncurrentVersionTransition& WithNoncurrentDays(int value) { SetNoncurrentDays(value); return *this;}


    /**
     * The class of storage used to store the object.
     */
    inline const TransitionStorageClass& GetStorageClass() const{ return m_storageClass; }

    /**
     * The class of storage used to store the object.
     */
    inline void SetStorageClass(const TransitionStorageClass& value) { m_storageClassHasBeenSet = true; m_storageClass = value; }

    /**
     * The class of storage used to store the object.
     */
    inline void SetStorageClass(TransitionStorageClass&& value) { m_storageClassHasBeenSet = true; m_storageClass = std::move(value); }

    /**
     * The class of storage used to store the object.
     */
    inline NoncurrentVersionTransition& WithStorageClass(const TransitionStorageClass& value) { SetStorageClass(value); return *this;}

    /**
     * The class of storage used to store the object.
     */
    inline NoncurrentVersionTransition& WithStorageClass(TransitionStorageClass&& value) { SetStorageClass(std::move(value)); return *this;}

  private:

    int m_noncurrentDays;
    bool m_noncurrentDaysHasBeenSet;

    TransitionStorageClass m_storageClass;
    bool m_storageClassHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
