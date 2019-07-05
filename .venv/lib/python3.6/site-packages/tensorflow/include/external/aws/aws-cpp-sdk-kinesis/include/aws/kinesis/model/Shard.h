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
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/kinesis/model/HashKeyRange.h>
#include <aws/kinesis/model/SequenceNumberRange.h>
#include <utility>

namespace Aws
{
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
   * <p>A uniquely identified group of data records in a Kinesis
   * stream.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/Shard">AWS API
   * Reference</a></p>
   */
  class AWS_KINESIS_API Shard
  {
  public:
    Shard();
    Shard(const Aws::Utils::Json::JsonValue& jsonValue);
    Shard& operator=(const Aws::Utils::Json::JsonValue& jsonValue);
    Aws::Utils::Json::JsonValue Jsonize() const;


    /**
     * <p>The unique identifier of the shard within the stream.</p>
     */
    inline const Aws::String& GetShardId() const{ return m_shardId; }

    /**
     * <p>The unique identifier of the shard within the stream.</p>
     */
    inline void SetShardId(const Aws::String& value) { m_shardIdHasBeenSet = true; m_shardId = value; }

    /**
     * <p>The unique identifier of the shard within the stream.</p>
     */
    inline void SetShardId(Aws::String&& value) { m_shardIdHasBeenSet = true; m_shardId = std::move(value); }

    /**
     * <p>The unique identifier of the shard within the stream.</p>
     */
    inline void SetShardId(const char* value) { m_shardIdHasBeenSet = true; m_shardId.assign(value); }

    /**
     * <p>The unique identifier of the shard within the stream.</p>
     */
    inline Shard& WithShardId(const Aws::String& value) { SetShardId(value); return *this;}

    /**
     * <p>The unique identifier of the shard within the stream.</p>
     */
    inline Shard& WithShardId(Aws::String&& value) { SetShardId(std::move(value)); return *this;}

    /**
     * <p>The unique identifier of the shard within the stream.</p>
     */
    inline Shard& WithShardId(const char* value) { SetShardId(value); return *this;}


    /**
     * <p>The shard ID of the shard's parent.</p>
     */
    inline const Aws::String& GetParentShardId() const{ return m_parentShardId; }

    /**
     * <p>The shard ID of the shard's parent.</p>
     */
    inline void SetParentShardId(const Aws::String& value) { m_parentShardIdHasBeenSet = true; m_parentShardId = value; }

    /**
     * <p>The shard ID of the shard's parent.</p>
     */
    inline void SetParentShardId(Aws::String&& value) { m_parentShardIdHasBeenSet = true; m_parentShardId = std::move(value); }

    /**
     * <p>The shard ID of the shard's parent.</p>
     */
    inline void SetParentShardId(const char* value) { m_parentShardIdHasBeenSet = true; m_parentShardId.assign(value); }

    /**
     * <p>The shard ID of the shard's parent.</p>
     */
    inline Shard& WithParentShardId(const Aws::String& value) { SetParentShardId(value); return *this;}

    /**
     * <p>The shard ID of the shard's parent.</p>
     */
    inline Shard& WithParentShardId(Aws::String&& value) { SetParentShardId(std::move(value)); return *this;}

    /**
     * <p>The shard ID of the shard's parent.</p>
     */
    inline Shard& WithParentShardId(const char* value) { SetParentShardId(value); return *this;}


    /**
     * <p>The shard ID of the shard adjacent to the shard's parent.</p>
     */
    inline const Aws::String& GetAdjacentParentShardId() const{ return m_adjacentParentShardId; }

    /**
     * <p>The shard ID of the shard adjacent to the shard's parent.</p>
     */
    inline void SetAdjacentParentShardId(const Aws::String& value) { m_adjacentParentShardIdHasBeenSet = true; m_adjacentParentShardId = value; }

    /**
     * <p>The shard ID of the shard adjacent to the shard's parent.</p>
     */
    inline void SetAdjacentParentShardId(Aws::String&& value) { m_adjacentParentShardIdHasBeenSet = true; m_adjacentParentShardId = std::move(value); }

    /**
     * <p>The shard ID of the shard adjacent to the shard's parent.</p>
     */
    inline void SetAdjacentParentShardId(const char* value) { m_adjacentParentShardIdHasBeenSet = true; m_adjacentParentShardId.assign(value); }

    /**
     * <p>The shard ID of the shard adjacent to the shard's parent.</p>
     */
    inline Shard& WithAdjacentParentShardId(const Aws::String& value) { SetAdjacentParentShardId(value); return *this;}

    /**
     * <p>The shard ID of the shard adjacent to the shard's parent.</p>
     */
    inline Shard& WithAdjacentParentShardId(Aws::String&& value) { SetAdjacentParentShardId(std::move(value)); return *this;}

    /**
     * <p>The shard ID of the shard adjacent to the shard's parent.</p>
     */
    inline Shard& WithAdjacentParentShardId(const char* value) { SetAdjacentParentShardId(value); return *this;}


    /**
     * <p>The range of possible hash key values for the shard, which is a set of
     * ordered contiguous positive integers.</p>
     */
    inline const HashKeyRange& GetHashKeyRange() const{ return m_hashKeyRange; }

    /**
     * <p>The range of possible hash key values for the shard, which is a set of
     * ordered contiguous positive integers.</p>
     */
    inline void SetHashKeyRange(const HashKeyRange& value) { m_hashKeyRangeHasBeenSet = true; m_hashKeyRange = value; }

    /**
     * <p>The range of possible hash key values for the shard, which is a set of
     * ordered contiguous positive integers.</p>
     */
    inline void SetHashKeyRange(HashKeyRange&& value) { m_hashKeyRangeHasBeenSet = true; m_hashKeyRange = std::move(value); }

    /**
     * <p>The range of possible hash key values for the shard, which is a set of
     * ordered contiguous positive integers.</p>
     */
    inline Shard& WithHashKeyRange(const HashKeyRange& value) { SetHashKeyRange(value); return *this;}

    /**
     * <p>The range of possible hash key values for the shard, which is a set of
     * ordered contiguous positive integers.</p>
     */
    inline Shard& WithHashKeyRange(HashKeyRange&& value) { SetHashKeyRange(std::move(value)); return *this;}


    /**
     * <p>The range of possible sequence numbers for the shard.</p>
     */
    inline const SequenceNumberRange& GetSequenceNumberRange() const{ return m_sequenceNumberRange; }

    /**
     * <p>The range of possible sequence numbers for the shard.</p>
     */
    inline void SetSequenceNumberRange(const SequenceNumberRange& value) { m_sequenceNumberRangeHasBeenSet = true; m_sequenceNumberRange = value; }

    /**
     * <p>The range of possible sequence numbers for the shard.</p>
     */
    inline void SetSequenceNumberRange(SequenceNumberRange&& value) { m_sequenceNumberRangeHasBeenSet = true; m_sequenceNumberRange = std::move(value); }

    /**
     * <p>The range of possible sequence numbers for the shard.</p>
     */
    inline Shard& WithSequenceNumberRange(const SequenceNumberRange& value) { SetSequenceNumberRange(value); return *this;}

    /**
     * <p>The range of possible sequence numbers for the shard.</p>
     */
    inline Shard& WithSequenceNumberRange(SequenceNumberRange&& value) { SetSequenceNumberRange(std::move(value)); return *this;}

  private:

    Aws::String m_shardId;
    bool m_shardIdHasBeenSet;

    Aws::String m_parentShardId;
    bool m_parentShardIdHasBeenSet;

    Aws::String m_adjacentParentShardId;
    bool m_adjacentParentShardIdHasBeenSet;

    HashKeyRange m_hashKeyRange;
    bool m_hashKeyRangeHasBeenSet;

    SequenceNumberRange m_sequenceNumberRange;
    bool m_sequenceNumberRangeHasBeenSet;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
