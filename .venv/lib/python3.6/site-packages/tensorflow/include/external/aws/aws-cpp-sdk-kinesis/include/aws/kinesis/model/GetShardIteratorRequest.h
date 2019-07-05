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
#include <aws/kinesis/model/ShardIteratorType.h>
#include <aws/core/utils/DateTime.h>
#include <utility>

namespace Aws
{
namespace Kinesis
{
namespace Model
{

  /**
   * <p>Represents the input for <code>GetShardIterator</code>.</p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/GetShardIteratorInput">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API GetShardIteratorRequest : public KinesisRequest
  {
  public:
    GetShardIteratorRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "GetShardIterator"; }

    Aws::String SerializePayload() const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    /**
     * <p>The name of the Amazon Kinesis stream.</p>
     */
    inline const Aws::String& GetStreamName() const{ return m_streamName; }

    /**
     * <p>The name of the Amazon Kinesis stream.</p>
     */
    inline void SetStreamName(const Aws::String& value) { m_streamNameHasBeenSet = true; m_streamName = value; }

    /**
     * <p>The name of the Amazon Kinesis stream.</p>
     */
    inline void SetStreamName(Aws::String&& value) { m_streamNameHasBeenSet = true; m_streamName = std::move(value); }

    /**
     * <p>The name of the Amazon Kinesis stream.</p>
     */
    inline void SetStreamName(const char* value) { m_streamNameHasBeenSet = true; m_streamName.assign(value); }

    /**
     * <p>The name of the Amazon Kinesis stream.</p>
     */
    inline GetShardIteratorRequest& WithStreamName(const Aws::String& value) { SetStreamName(value); return *this;}

    /**
     * <p>The name of the Amazon Kinesis stream.</p>
     */
    inline GetShardIteratorRequest& WithStreamName(Aws::String&& value) { SetStreamName(std::move(value)); return *this;}

    /**
     * <p>The name of the Amazon Kinesis stream.</p>
     */
    inline GetShardIteratorRequest& WithStreamName(const char* value) { SetStreamName(value); return *this;}


    /**
     * <p>The shard ID of the Kinesis Streams shard to get the iterator for.</p>
     */
    inline const Aws::String& GetShardId() const{ return m_shardId; }

    /**
     * <p>The shard ID of the Kinesis Streams shard to get the iterator for.</p>
     */
    inline void SetShardId(const Aws::String& value) { m_shardIdHasBeenSet = true; m_shardId = value; }

    /**
     * <p>The shard ID of the Kinesis Streams shard to get the iterator for.</p>
     */
    inline void SetShardId(Aws::String&& value) { m_shardIdHasBeenSet = true; m_shardId = std::move(value); }

    /**
     * <p>The shard ID of the Kinesis Streams shard to get the iterator for.</p>
     */
    inline void SetShardId(const char* value) { m_shardIdHasBeenSet = true; m_shardId.assign(value); }

    /**
     * <p>The shard ID of the Kinesis Streams shard to get the iterator for.</p>
     */
    inline GetShardIteratorRequest& WithShardId(const Aws::String& value) { SetShardId(value); return *this;}

    /**
     * <p>The shard ID of the Kinesis Streams shard to get the iterator for.</p>
     */
    inline GetShardIteratorRequest& WithShardId(Aws::String&& value) { SetShardId(std::move(value)); return *this;}

    /**
     * <p>The shard ID of the Kinesis Streams shard to get the iterator for.</p>
     */
    inline GetShardIteratorRequest& WithShardId(const char* value) { SetShardId(value); return *this;}


    /**
     * <p>Determines how the shard iterator is used to start reading data records from
     * the shard.</p> <p>The following are the valid Amazon Kinesis shard iterator
     * types:</p> <ul> <li> <p>AT_SEQUENCE_NUMBER - Start reading from the position
     * denoted by a specific sequence number, provided in the value
     * <code>StartingSequenceNumber</code>.</p> </li> <li> <p>AFTER_SEQUENCE_NUMBER -
     * Start reading right after the position denoted by a specific sequence number,
     * provided in the value <code>StartingSequenceNumber</code>.</p> </li> <li>
     * <p>AT_TIMESTAMP - Start reading from the position denoted by a specific time
     * stamp, provided in the value <code>Timestamp</code>.</p> </li> <li>
     * <p>TRIM_HORIZON - Start reading at the last untrimmed record in the shard in the
     * system, which is the oldest data record in the shard.</p> </li> <li> <p>LATEST -
     * Start reading just after the most recent record in the shard, so that you always
     * read the most recent data in the shard.</p> </li> </ul>
     */
    inline const ShardIteratorType& GetShardIteratorType() const{ return m_shardIteratorType; }

    /**
     * <p>Determines how the shard iterator is used to start reading data records from
     * the shard.</p> <p>The following are the valid Amazon Kinesis shard iterator
     * types:</p> <ul> <li> <p>AT_SEQUENCE_NUMBER - Start reading from the position
     * denoted by a specific sequence number, provided in the value
     * <code>StartingSequenceNumber</code>.</p> </li> <li> <p>AFTER_SEQUENCE_NUMBER -
     * Start reading right after the position denoted by a specific sequence number,
     * provided in the value <code>StartingSequenceNumber</code>.</p> </li> <li>
     * <p>AT_TIMESTAMP - Start reading from the position denoted by a specific time
     * stamp, provided in the value <code>Timestamp</code>.</p> </li> <li>
     * <p>TRIM_HORIZON - Start reading at the last untrimmed record in the shard in the
     * system, which is the oldest data record in the shard.</p> </li> <li> <p>LATEST -
     * Start reading just after the most recent record in the shard, so that you always
     * read the most recent data in the shard.</p> </li> </ul>
     */
    inline void SetShardIteratorType(const ShardIteratorType& value) { m_shardIteratorTypeHasBeenSet = true; m_shardIteratorType = value; }

    /**
     * <p>Determines how the shard iterator is used to start reading data records from
     * the shard.</p> <p>The following are the valid Amazon Kinesis shard iterator
     * types:</p> <ul> <li> <p>AT_SEQUENCE_NUMBER - Start reading from the position
     * denoted by a specific sequence number, provided in the value
     * <code>StartingSequenceNumber</code>.</p> </li> <li> <p>AFTER_SEQUENCE_NUMBER -
     * Start reading right after the position denoted by a specific sequence number,
     * provided in the value <code>StartingSequenceNumber</code>.</p> </li> <li>
     * <p>AT_TIMESTAMP - Start reading from the position denoted by a specific time
     * stamp, provided in the value <code>Timestamp</code>.</p> </li> <li>
     * <p>TRIM_HORIZON - Start reading at the last untrimmed record in the shard in the
     * system, which is the oldest data record in the shard.</p> </li> <li> <p>LATEST -
     * Start reading just after the most recent record in the shard, so that you always
     * read the most recent data in the shard.</p> </li> </ul>
     */
    inline void SetShardIteratorType(ShardIteratorType&& value) { m_shardIteratorTypeHasBeenSet = true; m_shardIteratorType = std::move(value); }

    /**
     * <p>Determines how the shard iterator is used to start reading data records from
     * the shard.</p> <p>The following are the valid Amazon Kinesis shard iterator
     * types:</p> <ul> <li> <p>AT_SEQUENCE_NUMBER - Start reading from the position
     * denoted by a specific sequence number, provided in the value
     * <code>StartingSequenceNumber</code>.</p> </li> <li> <p>AFTER_SEQUENCE_NUMBER -
     * Start reading right after the position denoted by a specific sequence number,
     * provided in the value <code>StartingSequenceNumber</code>.</p> </li> <li>
     * <p>AT_TIMESTAMP - Start reading from the position denoted by a specific time
     * stamp, provided in the value <code>Timestamp</code>.</p> </li> <li>
     * <p>TRIM_HORIZON - Start reading at the last untrimmed record in the shard in the
     * system, which is the oldest data record in the shard.</p> </li> <li> <p>LATEST -
     * Start reading just after the most recent record in the shard, so that you always
     * read the most recent data in the shard.</p> </li> </ul>
     */
    inline GetShardIteratorRequest& WithShardIteratorType(const ShardIteratorType& value) { SetShardIteratorType(value); return *this;}

    /**
     * <p>Determines how the shard iterator is used to start reading data records from
     * the shard.</p> <p>The following are the valid Amazon Kinesis shard iterator
     * types:</p> <ul> <li> <p>AT_SEQUENCE_NUMBER - Start reading from the position
     * denoted by a specific sequence number, provided in the value
     * <code>StartingSequenceNumber</code>.</p> </li> <li> <p>AFTER_SEQUENCE_NUMBER -
     * Start reading right after the position denoted by a specific sequence number,
     * provided in the value <code>StartingSequenceNumber</code>.</p> </li> <li>
     * <p>AT_TIMESTAMP - Start reading from the position denoted by a specific time
     * stamp, provided in the value <code>Timestamp</code>.</p> </li> <li>
     * <p>TRIM_HORIZON - Start reading at the last untrimmed record in the shard in the
     * system, which is the oldest data record in the shard.</p> </li> <li> <p>LATEST -
     * Start reading just after the most recent record in the shard, so that you always
     * read the most recent data in the shard.</p> </li> </ul>
     */
    inline GetShardIteratorRequest& WithShardIteratorType(ShardIteratorType&& value) { SetShardIteratorType(std::move(value)); return *this;}


    /**
     * <p>The sequence number of the data record in the shard from which to start
     * reading. Used with shard iterator type AT_SEQUENCE_NUMBER and
     * AFTER_SEQUENCE_NUMBER.</p>
     */
    inline const Aws::String& GetStartingSequenceNumber() const{ return m_startingSequenceNumber; }

    /**
     * <p>The sequence number of the data record in the shard from which to start
     * reading. Used with shard iterator type AT_SEQUENCE_NUMBER and
     * AFTER_SEQUENCE_NUMBER.</p>
     */
    inline void SetStartingSequenceNumber(const Aws::String& value) { m_startingSequenceNumberHasBeenSet = true; m_startingSequenceNumber = value; }

    /**
     * <p>The sequence number of the data record in the shard from which to start
     * reading. Used with shard iterator type AT_SEQUENCE_NUMBER and
     * AFTER_SEQUENCE_NUMBER.</p>
     */
    inline void SetStartingSequenceNumber(Aws::String&& value) { m_startingSequenceNumberHasBeenSet = true; m_startingSequenceNumber = std::move(value); }

    /**
     * <p>The sequence number of the data record in the shard from which to start
     * reading. Used with shard iterator type AT_SEQUENCE_NUMBER and
     * AFTER_SEQUENCE_NUMBER.</p>
     */
    inline void SetStartingSequenceNumber(const char* value) { m_startingSequenceNumberHasBeenSet = true; m_startingSequenceNumber.assign(value); }

    /**
     * <p>The sequence number of the data record in the shard from which to start
     * reading. Used with shard iterator type AT_SEQUENCE_NUMBER and
     * AFTER_SEQUENCE_NUMBER.</p>
     */
    inline GetShardIteratorRequest& WithStartingSequenceNumber(const Aws::String& value) { SetStartingSequenceNumber(value); return *this;}

    /**
     * <p>The sequence number of the data record in the shard from which to start
     * reading. Used with shard iterator type AT_SEQUENCE_NUMBER and
     * AFTER_SEQUENCE_NUMBER.</p>
     */
    inline GetShardIteratorRequest& WithStartingSequenceNumber(Aws::String&& value) { SetStartingSequenceNumber(std::move(value)); return *this;}

    /**
     * <p>The sequence number of the data record in the shard from which to start
     * reading. Used with shard iterator type AT_SEQUENCE_NUMBER and
     * AFTER_SEQUENCE_NUMBER.</p>
     */
    inline GetShardIteratorRequest& WithStartingSequenceNumber(const char* value) { SetStartingSequenceNumber(value); return *this;}


    /**
     * <p>The time stamp of the data record from which to start reading. Used with
     * shard iterator type AT_TIMESTAMP. A time stamp is the Unix epoch date with
     * precision in milliseconds. For example,
     * <code>2016-04-04T19:58:46.480-00:00</code> or <code>1459799926.480</code>. If a
     * record with this exact time stamp does not exist, the iterator returned is for
     * the next (later) record. If the time stamp is older than the current trim
     * horizon, the iterator returned is for the oldest untrimmed data record
     * (TRIM_HORIZON).</p>
     */
    inline const Aws::Utils::DateTime& GetTimestamp() const{ return m_timestamp; }

    /**
     * <p>The time stamp of the data record from which to start reading. Used with
     * shard iterator type AT_TIMESTAMP. A time stamp is the Unix epoch date with
     * precision in milliseconds. For example,
     * <code>2016-04-04T19:58:46.480-00:00</code> or <code>1459799926.480</code>. If a
     * record with this exact time stamp does not exist, the iterator returned is for
     * the next (later) record. If the time stamp is older than the current trim
     * horizon, the iterator returned is for the oldest untrimmed data record
     * (TRIM_HORIZON).</p>
     */
    inline void SetTimestamp(const Aws::Utils::DateTime& value) { m_timestampHasBeenSet = true; m_timestamp = value; }

    /**
     * <p>The time stamp of the data record from which to start reading. Used with
     * shard iterator type AT_TIMESTAMP. A time stamp is the Unix epoch date with
     * precision in milliseconds. For example,
     * <code>2016-04-04T19:58:46.480-00:00</code> or <code>1459799926.480</code>. If a
     * record with this exact time stamp does not exist, the iterator returned is for
     * the next (later) record. If the time stamp is older than the current trim
     * horizon, the iterator returned is for the oldest untrimmed data record
     * (TRIM_HORIZON).</p>
     */
    inline void SetTimestamp(Aws::Utils::DateTime&& value) { m_timestampHasBeenSet = true; m_timestamp = std::move(value); }

    /**
     * <p>The time stamp of the data record from which to start reading. Used with
     * shard iterator type AT_TIMESTAMP. A time stamp is the Unix epoch date with
     * precision in milliseconds. For example,
     * <code>2016-04-04T19:58:46.480-00:00</code> or <code>1459799926.480</code>. If a
     * record with this exact time stamp does not exist, the iterator returned is for
     * the next (later) record. If the time stamp is older than the current trim
     * horizon, the iterator returned is for the oldest untrimmed data record
     * (TRIM_HORIZON).</p>
     */
    inline GetShardIteratorRequest& WithTimestamp(const Aws::Utils::DateTime& value) { SetTimestamp(value); return *this;}

    /**
     * <p>The time stamp of the data record from which to start reading. Used with
     * shard iterator type AT_TIMESTAMP. A time stamp is the Unix epoch date with
     * precision in milliseconds. For example,
     * <code>2016-04-04T19:58:46.480-00:00</code> or <code>1459799926.480</code>. If a
     * record with this exact time stamp does not exist, the iterator returned is for
     * the next (later) record. If the time stamp is older than the current trim
     * horizon, the iterator returned is for the oldest untrimmed data record
     * (TRIM_HORIZON).</p>
     */
    inline GetShardIteratorRequest& WithTimestamp(Aws::Utils::DateTime&& value) { SetTimestamp(std::move(value)); return *this;}

  private:

    Aws::String m_streamName;
    bool m_streamNameHasBeenSet;

    Aws::String m_shardId;
    bool m_shardIdHasBeenSet;

    ShardIteratorType m_shardIteratorType;
    bool m_shardIteratorTypeHasBeenSet;

    Aws::String m_startingSequenceNumber;
    bool m_startingSequenceNumberHasBeenSet;

    Aws::Utils::DateTime m_timestamp;
    bool m_timestampHasBeenSet;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
