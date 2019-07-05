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
   * Specifies the days since the initiation of an Incomplete Multipart Upload that
   * Lifecycle will wait before permanently removing all parts of the
   * upload.<p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/AbortIncompleteMultipartUpload">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API AbortIncompleteMultipartUpload
  {
  public:
    AbortIncompleteMultipartUpload();
    AbortIncompleteMultipartUpload(const Aws::Utils::Xml::XmlNode& xmlNode);
    AbortIncompleteMultipartUpload& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Indicates the number of days that must pass since initiation for Lifecycle to
     * abort an Incomplete Multipart Upload.
     */
    inline int GetDaysAfterInitiation() const{ return m_daysAfterInitiation; }

    /**
     * Indicates the number of days that must pass since initiation for Lifecycle to
     * abort an Incomplete Multipart Upload.
     */
    inline void SetDaysAfterInitiation(int value) { m_daysAfterInitiationHasBeenSet = true; m_daysAfterInitiation = value; }

    /**
     * Indicates the number of days that must pass since initiation for Lifecycle to
     * abort an Incomplete Multipart Upload.
     */
    inline AbortIncompleteMultipartUpload& WithDaysAfterInitiation(int value) { SetDaysAfterInitiation(value); return *this;}

  private:

    int m_daysAfterInitiation;
    bool m_daysAfterInitiationHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
