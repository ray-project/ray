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

  /**
   * Specifies the use of SSE-KMS to encrypt delievered Inventory reports.<p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/SSEKMS">AWS API
   * Reference</a></p>
   */
  class AWS_S3_API SSEKMS
  {
  public:
    SSEKMS();
    SSEKMS(const Aws::Utils::Xml::XmlNode& xmlNode);
    SSEKMS& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Specifies the ID of the AWS Key Management Service (KMS) master encryption key
     * to use for encrypting Inventory reports.
     */
    inline const Aws::String& GetKeyId() const{ return m_keyId; }

    /**
     * Specifies the ID of the AWS Key Management Service (KMS) master encryption key
     * to use for encrypting Inventory reports.
     */
    inline void SetKeyId(const Aws::String& value) { m_keyIdHasBeenSet = true; m_keyId = value; }

    /**
     * Specifies the ID of the AWS Key Management Service (KMS) master encryption key
     * to use for encrypting Inventory reports.
     */
    inline void SetKeyId(Aws::String&& value) { m_keyIdHasBeenSet = true; m_keyId = std::move(value); }

    /**
     * Specifies the ID of the AWS Key Management Service (KMS) master encryption key
     * to use for encrypting Inventory reports.
     */
    inline void SetKeyId(const char* value) { m_keyIdHasBeenSet = true; m_keyId.assign(value); }

    /**
     * Specifies the ID of the AWS Key Management Service (KMS) master encryption key
     * to use for encrypting Inventory reports.
     */
    inline SSEKMS& WithKeyId(const Aws::String& value) { SetKeyId(value); return *this;}

    /**
     * Specifies the ID of the AWS Key Management Service (KMS) master encryption key
     * to use for encrypting Inventory reports.
     */
    inline SSEKMS& WithKeyId(Aws::String&& value) { SetKeyId(std::move(value)); return *this;}

    /**
     * Specifies the ID of the AWS Key Management Service (KMS) master encryption key
     * to use for encrypting Inventory reports.
     */
    inline SSEKMS& WithKeyId(const char* value) { SetKeyId(value); return *this;}

  private:

    Aws::String m_keyId;
    bool m_keyIdHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
