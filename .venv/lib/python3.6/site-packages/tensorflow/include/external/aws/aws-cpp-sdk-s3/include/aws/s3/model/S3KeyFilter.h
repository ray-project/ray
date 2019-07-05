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
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/FilterRule.h>
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
   * Container for object key name prefix and suffix filtering rules.<p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/S3KeyFilter">AWS API
   * Reference</a></p>
   */
  class AWS_S3_API S3KeyFilter
  {
  public:
    S3KeyFilter();
    S3KeyFilter(const Aws::Utils::Xml::XmlNode& xmlNode);
    S3KeyFilter& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    
    inline const Aws::Vector<FilterRule>& GetFilterRules() const{ return m_filterRules; }

    
    inline void SetFilterRules(const Aws::Vector<FilterRule>& value) { m_filterRulesHasBeenSet = true; m_filterRules = value; }

    
    inline void SetFilterRules(Aws::Vector<FilterRule>&& value) { m_filterRulesHasBeenSet = true; m_filterRules = std::move(value); }

    
    inline S3KeyFilter& WithFilterRules(const Aws::Vector<FilterRule>& value) { SetFilterRules(value); return *this;}

    
    inline S3KeyFilter& WithFilterRules(Aws::Vector<FilterRule>&& value) { SetFilterRules(std::move(value)); return *this;}

    
    inline S3KeyFilter& AddFilterRules(const FilterRule& value) { m_filterRulesHasBeenSet = true; m_filterRules.push_back(value); return *this; }

    
    inline S3KeyFilter& AddFilterRules(FilterRule&& value) { m_filterRulesHasBeenSet = true; m_filterRules.push_back(std::move(value)); return *this; }

  private:

    Aws::Vector<FilterRule> m_filterRules;
    bool m_filterRulesHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
