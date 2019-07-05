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
#include <aws/s3/model/Tag.h>
#include <aws/s3/model/MetricsAndOperator.h>
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

  class AWS_S3_API MetricsFilter
  {
  public:
    MetricsFilter();
    MetricsFilter(const Aws::Utils::Xml::XmlNode& xmlNode);
    MetricsFilter& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * The prefix used when evaluating a metrics filter.
     */
    inline const Aws::String& GetPrefix() const{ return m_prefix; }

    /**
     * The prefix used when evaluating a metrics filter.
     */
    inline void SetPrefix(const Aws::String& value) { m_prefixHasBeenSet = true; m_prefix = value; }

    /**
     * The prefix used when evaluating a metrics filter.
     */
    inline void SetPrefix(Aws::String&& value) { m_prefixHasBeenSet = true; m_prefix = std::move(value); }

    /**
     * The prefix used when evaluating a metrics filter.
     */
    inline void SetPrefix(const char* value) { m_prefixHasBeenSet = true; m_prefix.assign(value); }

    /**
     * The prefix used when evaluating a metrics filter.
     */
    inline MetricsFilter& WithPrefix(const Aws::String& value) { SetPrefix(value); return *this;}

    /**
     * The prefix used when evaluating a metrics filter.
     */
    inline MetricsFilter& WithPrefix(Aws::String&& value) { SetPrefix(std::move(value)); return *this;}

    /**
     * The prefix used when evaluating a metrics filter.
     */
    inline MetricsFilter& WithPrefix(const char* value) { SetPrefix(value); return *this;}


    /**
     * The tag used when evaluating a metrics filter.
     */
    inline const Tag& GetTag() const{ return m_tag; }

    /**
     * The tag used when evaluating a metrics filter.
     */
    inline void SetTag(const Tag& value) { m_tagHasBeenSet = true; m_tag = value; }

    /**
     * The tag used when evaluating a metrics filter.
     */
    inline void SetTag(Tag&& value) { m_tagHasBeenSet = true; m_tag = std::move(value); }

    /**
     * The tag used when evaluating a metrics filter.
     */
    inline MetricsFilter& WithTag(const Tag& value) { SetTag(value); return *this;}

    /**
     * The tag used when evaluating a metrics filter.
     */
    inline MetricsFilter& WithTag(Tag&& value) { SetTag(std::move(value)); return *this;}


    /**
     * A conjunction (logical AND) of predicates, which is used in evaluating a metrics
     * filter. The operator must have at least two predicates, and an object must match
     * all of the predicates in order for the filter to apply.
     */
    inline const MetricsAndOperator& GetAnd() const{ return m_and; }

    /**
     * A conjunction (logical AND) of predicates, which is used in evaluating a metrics
     * filter. The operator must have at least two predicates, and an object must match
     * all of the predicates in order for the filter to apply.
     */
    inline void SetAnd(const MetricsAndOperator& value) { m_andHasBeenSet = true; m_and = value; }

    /**
     * A conjunction (logical AND) of predicates, which is used in evaluating a metrics
     * filter. The operator must have at least two predicates, and an object must match
     * all of the predicates in order for the filter to apply.
     */
    inline void SetAnd(MetricsAndOperator&& value) { m_andHasBeenSet = true; m_and = std::move(value); }

    /**
     * A conjunction (logical AND) of predicates, which is used in evaluating a metrics
     * filter. The operator must have at least two predicates, and an object must match
     * all of the predicates in order for the filter to apply.
     */
    inline MetricsFilter& WithAnd(const MetricsAndOperator& value) { SetAnd(value); return *this;}

    /**
     * A conjunction (logical AND) of predicates, which is used in evaluating a metrics
     * filter. The operator must have at least two predicates, and an object must match
     * all of the predicates in order for the filter to apply.
     */
    inline MetricsFilter& WithAnd(MetricsAndOperator&& value) { SetAnd(std::move(value)); return *this;}

  private:

    Aws::String m_prefix;
    bool m_prefixHasBeenSet;

    Tag m_tag;
    bool m_tagHasBeenSet;

    MetricsAndOperator m_and;
    bool m_andHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
