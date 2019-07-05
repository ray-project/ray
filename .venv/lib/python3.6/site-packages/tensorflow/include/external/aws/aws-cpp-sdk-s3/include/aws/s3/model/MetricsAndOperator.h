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

  class AWS_S3_API MetricsAndOperator
  {
  public:
    MetricsAndOperator();
    MetricsAndOperator(const Aws::Utils::Xml::XmlNode& xmlNode);
    MetricsAndOperator& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * The prefix used when evaluating an AND predicate.
     */
    inline const Aws::String& GetPrefix() const{ return m_prefix; }

    /**
     * The prefix used when evaluating an AND predicate.
     */
    inline void SetPrefix(const Aws::String& value) { m_prefixHasBeenSet = true; m_prefix = value; }

    /**
     * The prefix used when evaluating an AND predicate.
     */
    inline void SetPrefix(Aws::String&& value) { m_prefixHasBeenSet = true; m_prefix = std::move(value); }

    /**
     * The prefix used when evaluating an AND predicate.
     */
    inline void SetPrefix(const char* value) { m_prefixHasBeenSet = true; m_prefix.assign(value); }

    /**
     * The prefix used when evaluating an AND predicate.
     */
    inline MetricsAndOperator& WithPrefix(const Aws::String& value) { SetPrefix(value); return *this;}

    /**
     * The prefix used when evaluating an AND predicate.
     */
    inline MetricsAndOperator& WithPrefix(Aws::String&& value) { SetPrefix(std::move(value)); return *this;}

    /**
     * The prefix used when evaluating an AND predicate.
     */
    inline MetricsAndOperator& WithPrefix(const char* value) { SetPrefix(value); return *this;}


    /**
     * The list of tags used when evaluating an AND predicate.
     */
    inline const Aws::Vector<Tag>& GetTags() const{ return m_tags; }

    /**
     * The list of tags used when evaluating an AND predicate.
     */
    inline void SetTags(const Aws::Vector<Tag>& value) { m_tagsHasBeenSet = true; m_tags = value; }

    /**
     * The list of tags used when evaluating an AND predicate.
     */
    inline void SetTags(Aws::Vector<Tag>&& value) { m_tagsHasBeenSet = true; m_tags = std::move(value); }

    /**
     * The list of tags used when evaluating an AND predicate.
     */
    inline MetricsAndOperator& WithTags(const Aws::Vector<Tag>& value) { SetTags(value); return *this;}

    /**
     * The list of tags used when evaluating an AND predicate.
     */
    inline MetricsAndOperator& WithTags(Aws::Vector<Tag>&& value) { SetTags(std::move(value)); return *this;}

    /**
     * The list of tags used when evaluating an AND predicate.
     */
    inline MetricsAndOperator& AddTags(const Tag& value) { m_tagsHasBeenSet = true; m_tags.push_back(value); return *this; }

    /**
     * The list of tags used when evaluating an AND predicate.
     */
    inline MetricsAndOperator& AddTags(Tag&& value) { m_tagsHasBeenSet = true; m_tags.push_back(std::move(value)); return *this; }

  private:

    Aws::String m_prefix;
    bool m_prefixHasBeenSet;

    Aws::Vector<Tag> m_tags;
    bool m_tagsHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
