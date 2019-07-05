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

  class AWS_S3_API InventoryFilter
  {
  public:
    InventoryFilter();
    InventoryFilter(const Aws::Utils::Xml::XmlNode& xmlNode);
    InventoryFilter& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * The prefix that an object must have to be included in the inventory results.
     */
    inline const Aws::String& GetPrefix() const{ return m_prefix; }

    /**
     * The prefix that an object must have to be included in the inventory results.
     */
    inline void SetPrefix(const Aws::String& value) { m_prefixHasBeenSet = true; m_prefix = value; }

    /**
     * The prefix that an object must have to be included in the inventory results.
     */
    inline void SetPrefix(Aws::String&& value) { m_prefixHasBeenSet = true; m_prefix = std::move(value); }

    /**
     * The prefix that an object must have to be included in the inventory results.
     */
    inline void SetPrefix(const char* value) { m_prefixHasBeenSet = true; m_prefix.assign(value); }

    /**
     * The prefix that an object must have to be included in the inventory results.
     */
    inline InventoryFilter& WithPrefix(const Aws::String& value) { SetPrefix(value); return *this;}

    /**
     * The prefix that an object must have to be included in the inventory results.
     */
    inline InventoryFilter& WithPrefix(Aws::String&& value) { SetPrefix(std::move(value)); return *this;}

    /**
     * The prefix that an object must have to be included in the inventory results.
     */
    inline InventoryFilter& WithPrefix(const char* value) { SetPrefix(value); return *this;}

  private:

    Aws::String m_prefix;
    bool m_prefixHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
