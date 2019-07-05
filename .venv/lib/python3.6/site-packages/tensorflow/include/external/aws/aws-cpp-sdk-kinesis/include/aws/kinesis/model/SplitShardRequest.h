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
   * <p>Represents the input for <code>SplitShard</code>.</p><p><h3>See Also:</h3>  
   * <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/SplitShardInput">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API SplitShardRequest : public KinesisRequest
  {
  public:
    SplitShardRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "SplitShard"; }

    Aws::String SerializePayload() const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    /**
     * <p>The name of the stream for the shard split.</p>
     */
    inline const Aws::String& GetStreamName() const{ return m_streamName; }

    /**
     * <p>The name of the stream for the shard split.</p>
     */
    inline void SetStreamName(const Aws::String& value) { m_streamNameHasBeenSet = true; m_streamName = value; }

    /**
     * <p>The name of the stream for the shard split.</p>
     */
    inline void SetStreamName(Aws::String&& value) { m_streamNameHasBeenSet = true; m_streamName = std::move(value); }

    /**
     * <p>The name of the stream for the shard split.</p>
     */
    inline void SetStreamName(const char* value) { m_streamNameHasBeenSet = true; m_streamName.assign(value); }

    /**
     * <p>The name of the stream for the shard split.</p>
     */
    inline SplitShardRequest& WithStreamName(const Aws::String& value) { SetStreamName(value); return *this;}

    /**
     * <p>The name of the stream for the shard split.</p>
     */
    inline SplitShardRequest& WithStreamName(Aws::String&& value) { SetStreamName(std::move(value)); return *this;}

    /**
     * <p>The name of the stream for the shard split.</p>
     */
    inline SplitShardRequest& WithStreamName(const char* value) { SetStreamName(value); return *this;}


    /**
     * <p>The shard ID of the shard to split.</p>
     */
    inline const Aws::String& GetShardToSplit() const{ return m_shardToSplit; }

    /**
     * <p>The shard ID of the shard to split.</p>
     */
    inline void SetShardToSplit(const Aws::String& value) { m_shardToSplitHasBeenSet = true; m_shardToSplit = value; }

    /**
     * <p>The shard ID of the shard to split.</p>
     */
    inline void SetShardToSplit(Aws::String&& value) { m_shardToSplitHasBeenSet = true; m_shardToSplit = std::move(value); }

    /**
     * <p>The shard ID of the shard to split.</p>
     */
    inline void SetShardToSplit(const char* value) { m_shardToSplitHasBeenSet = true; m_shardToSplit.assign(value); }

    /**
     * <p>The shard ID of the shard to split.</p>
     */
    inline SplitShardRequest& WithShardToSplit(const Aws::String& value) { SetShardToSplit(value); return *this;}

    /**
     * <p>The shard ID of the shard to split.</p>
     */
    inline SplitShardRequest& WithShardToSplit(Aws::String&& value) { SetShardToSplit(std::move(value)); return *this;}

    /**
     * <p>The shard ID of the shard to split.</p>
     */
    inline SplitShardRequest& WithShardToSplit(const char* value) { SetShardToSplit(value); return *this;}


    /**
     * <p>A hash key value for the starting hash key of one of the child shards created
     * by the split. The hash key range for a given shard constitutes a set of ordered
     * contiguous positive integers. The value for <code>NewStartingHashKey</code> must
     * be in the range of hash keys being mapped into the shard. The
     * <code>NewStartingHashKey</code> hash key value and all higher hash key values in
     * hash key range are distributed to one of the child shards. All the lower hash
     * key values in the range are distributed to the other child shard.</p>
     */
    inline const Aws::String& GetNewStartingHashKey() const{ return m_newStartingHashKey; }

    /**
     * <p>A hash key value for the starting hash key of one of the child shards created
     * by the split. The hash key range for a given shard constitutes a set of ordered
     * contiguous positive integers. The value for <code>NewStartingHashKey</code> must
     * be in the range of hash keys being mapped into the shard. The
     * <code>NewStartingHashKey</code> hash key value and all higher hash key values in
     * hash key range are distributed to one of the child shards. All the lower hash
     * key values in the range are distributed to the other child shard.</p>
     */
    inline void SetNewStartingHashKey(const Aws::String& value) { m_newStartingHashKeyHasBeenSet = true; m_newStartingHashKey = value; }

    /**
     * <p>A hash key value for the starting hash key of one of the child shards created
     * by the split. The hash key range for a given shard constitutes a set of ordered
     * contiguous positive integers. The value for <code>NewStartingHashKey</code> must
     * be in the range of hash keys being mapped into the shard. The
     * <code>NewStartingHashKey</code> hash key value and all higher hash key values in
     * hash key range are distributed to one of the child shards. All the lower hash
     * key values in the range are distributed to the other child shard.</p>
     */
    inline void SetNewStartingHashKey(Aws::String&& value) { m_newStartingHashKeyHasBeenSet = true; m_newStartingHashKey = std::move(value); }

    /**
     * <p>A hash key value for the starting hash key of one of the child shards created
     * by the split. The hash key range for a given shard constitutes a set of ordered
     * contiguous positive integers. The value for <code>NewStartingHashKey</code> must
     * be in the range of hash keys being mapped into the shard. The
     * <code>NewStartingHashKey</code> hash key value and all higher hash key values in
     * hash key range are distributed to one of the child shards. All the lower hash
     * key values in the range are distributed to the other child shard.</p>
     */
    inline void SetNewStartingHashKey(const char* value) { m_newStartingHashKeyHasBeenSet = true; m_newStartingHashKey.assign(value); }

    /**
     * <p>A hash key value for the starting hash key of one of the child shards created
     * by the split. The hash key range for a given shard constitutes a set of ordered
     * contiguous positive integers. The value for <code>NewStartingHashKey</code> must
     * be in the range of hash keys being mapped into the shard. The
     * <code>NewStartingHashKey</code> hash key value and all higher hash key values in
     * hash key range are distributed to one of the child shards. All the lower hash
     * key values in the range are distributed to the other child shard.</p>
     */
    inline SplitShardRequest& WithNewStartingHashKey(const Aws::String& value) { SetNewStartingHashKey(value); return *this;}

    /**
     * <p>A hash key value for the starting hash key of one of the child shards created
     * by the split. The hash key range for a given shard constitutes a set of ordered
     * contiguous positive integers. The value for <code>NewStartingHashKey</code> must
     * be in the range of hash keys being mapped into the shard. The
     * <code>NewStartingHashKey</code> hash key value and all higher hash key values in
     * hash key range are distributed to one of the child shards. All the lower hash
     * key values in the range are distributed to the other child shard.</p>
     */
    inline SplitShardRequest& WithNewStartingHashKey(Aws::String&& value) { SetNewStartingHashKey(std::move(value)); return *this;}

    /**
     * <p>A hash key value for the starting hash key of one of the child shards created
     * by the split. The hash key range for a given shard constitutes a set of ordered
     * contiguous positive integers. The value for <code>NewStartingHashKey</code> must
     * be in the range of hash keys being mapped into the shard. The
     * <code>NewStartingHashKey</code> hash key value and all higher hash key values in
     * hash key range are distributed to one of the child shards. All the lower hash
     * key values in the range are distributed to the other child shard.</p>
     */
    inline SplitShardRequest& WithNewStartingHashKey(const char* value) { SetNewStartingHashKey(value); return *this;}

  private:

    Aws::String m_streamName;
    bool m_streamNameHasBeenSet;

    Aws::String m_shardToSplit;
    bool m_shardToSplitHasBeenSet;

    Aws::String m_newStartingHashKey;
    bool m_newStartingHashKeyHasBeenSet;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
