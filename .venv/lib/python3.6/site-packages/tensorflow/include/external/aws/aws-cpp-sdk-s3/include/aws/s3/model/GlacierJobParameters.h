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
#include <aws/s3/model/Tier.h>
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

  class AWS_S3_API GlacierJobParameters
  {
  public:
    GlacierJobParameters();
    GlacierJobParameters(const Aws::Utils::Xml::XmlNode& xmlNode);
    GlacierJobParameters& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Glacier retrieval tier at which the restore will be processed.
     */
    inline const Tier& GetTier() const{ return m_tier; }

    /**
     * Glacier retrieval tier at which the restore will be processed.
     */
    inline void SetTier(const Tier& value) { m_tierHasBeenSet = true; m_tier = value; }

    /**
     * Glacier retrieval tier at which the restore will be processed.
     */
    inline void SetTier(Tier&& value) { m_tierHasBeenSet = true; m_tier = std::move(value); }

    /**
     * Glacier retrieval tier at which the restore will be processed.
     */
    inline GlacierJobParameters& WithTier(const Tier& value) { SetTier(value); return *this;}

    /**
     * Glacier retrieval tier at which the restore will be processed.
     */
    inline GlacierJobParameters& WithTier(Tier&& value) { SetTier(std::move(value)); return *this;}

  private:

    Tier m_tier;
    bool m_tierHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
