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
#include <aws/kinesis/KinesisRequest.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <utility>

namespace Aws
{
namespace Kinesis
{
namespace Model
{

  /**
   * <p>Represents the input for <code>ListTagsForStream</code>.</p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/ListTagsForStreamInput">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API ListTagsForStreamRequest : public KinesisRequest
  {
  public:
    ListTagsForStreamRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "ListTagsForStream"; }

    Aws::String SerializePayload() const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    /**
     * <p>The name of the stream.</p>
     */
    inline const Aws::String& GetStreamName() const{ return m_streamName; }

    /**
     * <p>The name of the stream.</p>
     */
    inline void SetStreamName(const Aws::String& value) { m_streamNameHasBeenSet = true; m_streamName = value; }

    /**
     * <p>The name of the stream.</p>
     */
    inline void SetStreamName(Aws::String&& value) { m_streamNameHasBeenSet = true; m_streamName = std::move(value); }

    /**
     * <p>The name of the stream.</p>
     */
    inline void SetStreamName(const char* value) { m_streamNameHasBeenSet = true; m_streamName.assign(value); }

    /**
     * <p>The name of the stream.</p>
     */
    inline ListTagsForStreamRequest& WithStreamName(const Aws::String& value) { SetStreamName(value); return *this;}

    /**
     * <p>The name of the stream.</p>
     */
    inline ListTagsForStreamRequest& WithStreamName(Aws::String&& value) { SetStreamName(std::move(value)); return *this;}

    /**
     * <p>The name of the stream.</p>
     */
    inline ListTagsForStreamRequest& WithStreamName(const char* value) { SetStreamName(value); return *this;}


    /**
     * <p>The key to use as the starting point for the list of tags. If this parameter
     * is set, <code>ListTagsForStream</code> gets all tags that occur after
     * <code>ExclusiveStartTagKey</code>. </p>
     */
    inline const Aws::String& GetExclusiveStartTagKey() const{ return m_exclusiveStartTagKey; }

    /**
     * <p>The key to use as the starting point for the list of tags. If this parameter
     * is set, <code>ListTagsForStream</code> gets all tags that occur after
     * <code>ExclusiveStartTagKey</code>. </p>
     */
    inline void SetExclusiveStartTagKey(const Aws::String& value) { m_exclusiveStartTagKeyHasBeenSet = true; m_exclusiveStartTagKey = value; }

    /**
     * <p>The key to use as the starting point for the list of tags. If this parameter
     * is set, <code>ListTagsForStream</code> gets all tags that occur after
     * <code>ExclusiveStartTagKey</code>. </p>
     */
    inline void SetExclusiveStartTagKey(Aws::String&& value) { m_exclusiveStartTagKeyHasBeenSet = true; m_exclusiveStartTagKey = std::move(value); }

    /**
     * <p>The key to use as the starting point for the list of tags. If this parameter
     * is set, <code>ListTagsForStream</code> gets all tags that occur after
     * <code>ExclusiveStartTagKey</code>. </p>
     */
    inline void SetExclusiveStartTagKey(const char* value) { m_exclusiveStartTagKeyHasBeenSet = true; m_exclusiveStartTagKey.assign(value); }

    /**
     * <p>The key to use as the starting point for the list of tags. If this parameter
     * is set, <code>ListTagsForStream</code> gets all tags that occur after
     * <code>ExclusiveStartTagKey</code>. </p>
     */
    inline ListTagsForStreamRequest& WithExclusiveStartTagKey(const Aws::String& value) { SetExclusiveStartTagKey(value); return *this;}

    /**
     * <p>The key to use as the starting point for the list of tags. If this parameter
     * is set, <code>ListTagsForStream</code> gets all tags that occur after
     * <code>ExclusiveStartTagKey</code>. </p>
     */
    inline ListTagsForStreamRequest& WithExclusiveStartTagKey(Aws::String&& value) { SetExclusiveStartTagKey(std::move(value)); return *this;}

    /**
     * <p>The key to use as the starting point for the list of tags. If this parameter
     * is set, <code>ListTagsForStream</code> gets all tags that occur after
     * <code>ExclusiveStartTagKey</code>. </p>
     */
    inline ListTagsForStreamRequest& WithExclusiveStartTagKey(const char* value) { SetExclusiveStartTagKey(value); return *this;}


    /**
     * <p>The number of tags to return. If this number is less than the total number of
     * tags associated with the stream, <code>HasMoreTags</code> is set to
     * <code>true</code>. To list additional tags, set
     * <code>ExclusiveStartTagKey</code> to the last key in the response.</p>
     */
    inline int GetLimit() const{ return m_limit; }

    /**
     * <p>The number of tags to return. If this number is less than the total number of
     * tags associated with the stream, <code>HasMoreTags</code> is set to
     * <code>true</code>. To list additional tags, set
     * <code>ExclusiveStartTagKey</code> to the last key in the response.</p>
     */
    inline void SetLimit(int value) { m_limitHasBeenSet = true; m_limit = value; }

    /**
     * <p>The number of tags to return. If this number is less than the total number of
     * tags associated with the stream, <code>HasMoreTags</code> is set to
     * <code>true</code>. To list additional tags, set
     * <code>ExclusiveStartTagKey</code> to the last key in the response.</p>
     */
    inline ListTagsForStreamRequest& WithLimit(int value) { SetLimit(value); return *this;}

  private:

    Aws::String m_streamName;
    bool m_streamNameHasBeenSet;

    Aws::String m_exclusiveStartTagKey;
    bool m_exclusiveStartTagKeyHasBeenSet;

    int m_limit;
    bool m_limitHasBeenSet;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
