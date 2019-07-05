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
#include <aws/s3/model/SSES3.h>
#include <aws/s3/model/SSEKMS.h>
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
   * Contains the type of server-side encryption used to encrypt the inventory
   * results.<p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/InventoryEncryption">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API InventoryEncryption
  {
  public:
    InventoryEncryption();
    InventoryEncryption(const Aws::Utils::Xml::XmlNode& xmlNode);
    InventoryEncryption& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Specifies the use of SSE-S3 to encrypt delievered Inventory reports.
     */
    inline const SSES3& GetSSES3() const{ return m_sSES3; }

    /**
     * Specifies the use of SSE-S3 to encrypt delievered Inventory reports.
     */
    inline void SetSSES3(const SSES3& value) { m_sSES3HasBeenSet = true; m_sSES3 = value; }

    /**
     * Specifies the use of SSE-S3 to encrypt delievered Inventory reports.
     */
    inline void SetSSES3(SSES3&& value) { m_sSES3HasBeenSet = true; m_sSES3 = std::move(value); }

    /**
     * Specifies the use of SSE-S3 to encrypt delievered Inventory reports.
     */
    inline InventoryEncryption& WithSSES3(const SSES3& value) { SetSSES3(value); return *this;}

    /**
     * Specifies the use of SSE-S3 to encrypt delievered Inventory reports.
     */
    inline InventoryEncryption& WithSSES3(SSES3&& value) { SetSSES3(std::move(value)); return *this;}


    /**
     * Specifies the use of SSE-KMS to encrypt delievered Inventory reports.
     */
    inline const SSEKMS& GetSSEKMS() const{ return m_sSEKMS; }

    /**
     * Specifies the use of SSE-KMS to encrypt delievered Inventory reports.
     */
    inline void SetSSEKMS(const SSEKMS& value) { m_sSEKMSHasBeenSet = true; m_sSEKMS = value; }

    /**
     * Specifies the use of SSE-KMS to encrypt delievered Inventory reports.
     */
    inline void SetSSEKMS(SSEKMS&& value) { m_sSEKMSHasBeenSet = true; m_sSEKMS = std::move(value); }

    /**
     * Specifies the use of SSE-KMS to encrypt delievered Inventory reports.
     */
    inline InventoryEncryption& WithSSEKMS(const SSEKMS& value) { SetSSEKMS(value); return *this;}

    /**
     * Specifies the use of SSE-KMS to encrypt delievered Inventory reports.
     */
    inline InventoryEncryption& WithSSEKMS(SSEKMS&& value) { SetSSEKMS(std::move(value)); return *this;}

  private:

    SSES3 m_sSES3;
    bool m_sSES3HasBeenSet;

    SSEKMS m_sSEKMS;
    bool m_sSEKMSHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
