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
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/Tag.h>
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
   * This is used in a Lifecycle Rule Filter to apply a logical AND to two or more
   * predicates. The Lifecycle Rule will apply to any object matching all of the
   * predicates configured inside the And operator.<p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/LifecycleRuleAndOperator">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API LifecycleRuleAndOperator
  {
  public:
    LifecycleRuleAndOperator();
    LifecycleRuleAndOperator(const Aws::Utils::Xml::XmlNode& xmlNode);
    LifecycleRuleAndOperator& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    
    inline const Aws::String& GetPrefix() const{ return m_prefix; }

    
    inline void SetPrefix(const Aws::String& value) { m_prefixHasBeenSet = true; m_prefix = value; }

    
    inline void SetPrefix(Aws::String&& value) { m_prefixHasBeenSet = true; m_prefix = std::move(value); }

    
    inline void SetPrefix(const char* value) { m_prefixHasBeenSet = true; m_prefix.assign(value); }

    
    inline LifecycleRuleAndOperator& WithPrefix(const Aws::String& value) { SetPrefix(value); return *this;}

    
    inline LifecycleRuleAndOperator& WithPrefix(Aws::String&& value) { SetPrefix(std::move(value)); return *this;}

    
    inline LifecycleRuleAndOperator& WithPrefix(const char* value) { SetPrefix(value); return *this;}


    /**
     * All of these tags must exist in the object's tag set in order for the rule to
     * apply.
     */
    inline const Aws::Vector<Tag>& GetTags() const{ return m_tags; }

    /**
     * All of these tags must exist in the object's tag set in order for the rule to
     * apply.
     */
    inline void SetTags(const Aws::Vector<Tag>& value) { m_tagsHasBeenSet = true; m_tags = value; }

    /**
     * All of these tags must exist in the object's tag set in order for the rule to
     * apply.
     */
    inline void SetTags(Aws::Vector<Tag>&& value) { m_tagsHasBeenSet = true; m_tags = std::move(value); }

    /**
     * All of these tags must exist in the object's tag set in order for the rule to
     * apply.
     */
    inline LifecycleRuleAndOperator& WithTags(const Aws::Vector<Tag>& value) { SetTags(value); return *this;}

    /**
     * All of these tags must exist in the object's tag set in order for the rule to
     * apply.
     */
    inline LifecycleRuleAndOperator& WithTags(Aws::Vector<Tag>&& value) { SetTags(std::move(value)); return *this;}

    /**
     * All of these tags must exist in the object's tag set in order for the rule to
     * apply.
     */
    inline LifecycleRuleAndOperator& AddTags(const Tag& value) { m_tagsHasBeenSet = true; m_tags.push_back(value); return *this; }

    /**
     * All of these tags must exist in the object's tag set in order for the rule to
     * apply.
     */
    inline LifecycleRuleAndOperator& AddTags(Tag&& value) { m_tagsHasBeenSet = true; m_tags.push_back(std::move(value)); return *this; }

  private:

    Aws::String m_prefix;
    bool m_prefixHasBeenSet;

    Aws::Vector<Tag> m_tags;
    bool m_tagsHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
