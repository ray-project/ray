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
#include <aws/kinesis/Kinesis_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/kinesis/model/Tag.h>
#include <utility>

namespace Aws
{
template<typename RESULT_TYPE>
class AmazonWebServiceResult;

namespace Utils
{
namespace Json
{
  class JsonValue;
} // namespace Json
} // namespace Utils
namespace Kinesis
{
namespace Model
{
  /**
   * <p>Represents the output for <code>ListTagsForStream</code>.</p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/ListTagsForStreamOutput">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API ListTagsForStreamResult
  {
  public:
    ListTagsForStreamResult();
    ListTagsForStreamResult(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);
    ListTagsForStreamResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);


    /**
     * <p>A list of tags associated with <code>StreamName</code>, starting with the
     * first tag after <code>ExclusiveStartTagKey</code> and up to the specified
     * <code>Limit</code>. </p>
     */
    inline const Aws::Vector<Tag>& GetTags() const{ return m_tags; }

    /**
     * <p>A list of tags associated with <code>StreamName</code>, starting with the
     * first tag after <code>ExclusiveStartTagKey</code> and up to the specified
     * <code>Limit</code>. </p>
     */
    inline void SetTags(const Aws::Vector<Tag>& value) { m_tags = value; }

    /**
     * <p>A list of tags associated with <code>StreamName</code>, starting with the
     * first tag after <code>ExclusiveStartTagKey</code> and up to the specified
     * <code>Limit</code>. </p>
     */
    inline void SetTags(Aws::Vector<Tag>&& value) { m_tags = std::move(value); }

    /**
     * <p>A list of tags associated with <code>StreamName</code>, starting with the
     * first tag after <code>ExclusiveStartTagKey</code> and up to the specified
     * <code>Limit</code>. </p>
     */
    inline ListTagsForStreamResult& WithTags(const Aws::Vector<Tag>& value) { SetTags(value); return *this;}

    /**
     * <p>A list of tags associated with <code>StreamName</code>, starting with the
     * first tag after <code>ExclusiveStartTagKey</code> and up to the specified
     * <code>Limit</code>. </p>
     */
    inline ListTagsForStreamResult& WithTags(Aws::Vector<Tag>&& value) { SetTags(std::move(value)); return *this;}

    /**
     * <p>A list of tags associated with <code>StreamName</code>, starting with the
     * first tag after <code>ExclusiveStartTagKey</code> and up to the specified
     * <code>Limit</code>. </p>
     */
    inline ListTagsForStreamResult& AddTags(const Tag& value) { m_tags.push_back(value); return *this; }

    /**
     * <p>A list of tags associated with <code>StreamName</code>, starting with the
     * first tag after <code>ExclusiveStartTagKey</code> and up to the specified
     * <code>Limit</code>. </p>
     */
    inline ListTagsForStreamResult& AddTags(Tag&& value) { m_tags.push_back(std::move(value)); return *this; }


    /**
     * <p>If set to <code>true</code>, more tags are available. To request additional
     * tags, set <code>ExclusiveStartTagKey</code> to the key of the last tag
     * returned.</p>
     */
    inline bool GetHasMoreTags() const{ return m_hasMoreTags; }

    /**
     * <p>If set to <code>true</code>, more tags are available. To request additional
     * tags, set <code>ExclusiveStartTagKey</code> to the key of the last tag
     * returned.</p>
     */
    inline void SetHasMoreTags(bool value) { m_hasMoreTags = value; }

    /**
     * <p>If set to <code>true</code>, more tags are available. To request additional
     * tags, set <code>ExclusiveStartTagKey</code> to the key of the last tag
     * returned.</p>
     */
    inline ListTagsForStreamResult& WithHasMoreTags(bool value) { SetHasMoreTags(value); return *this;}

  private:

    Aws::Vector<Tag> m_tags;

    bool m_hasMoreTags;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
