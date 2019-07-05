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
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/kinesis/model/Record.h>
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
   * <p>Represents the output for <a>GetRecords</a>.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/GetRecordsOutput">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API GetRecordsResult
  {
  public:
    GetRecordsResult();
    GetRecordsResult(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);
    GetRecordsResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);


    /**
     * <p>The data records retrieved from the shard.</p>
     */
    inline const Aws::Vector<Record>& GetRecords() const{ return m_records; }

    /**
     * <p>The data records retrieved from the shard.</p>
     */
    inline void SetRecords(const Aws::Vector<Record>& value) { m_records = value; }

    /**
     * <p>The data records retrieved from the shard.</p>
     */
    inline void SetRecords(Aws::Vector<Record>&& value) { m_records = std::move(value); }

    /**
     * <p>The data records retrieved from the shard.</p>
     */
    inline GetRecordsResult& WithRecords(const Aws::Vector<Record>& value) { SetRecords(value); return *this;}

    /**
     * <p>The data records retrieved from the shard.</p>
     */
    inline GetRecordsResult& WithRecords(Aws::Vector<Record>&& value) { SetRecords(std::move(value)); return *this;}

    /**
     * <p>The data records retrieved from the shard.</p>
     */
    inline GetRecordsResult& AddRecords(const Record& value) { m_records.push_back(value); return *this; }

    /**
     * <p>The data records retrieved from the shard.</p>
     */
    inline GetRecordsResult& AddRecords(Record&& value) { m_records.push_back(std::move(value)); return *this; }


    /**
     * <p>The next position in the shard from which to start sequentially reading data
     * records. If set to <code>null</code>, the shard has been closed and the
     * requested iterator does not return any more data. </p>
     */
    inline const Aws::String& GetNextShardIterator() const{ return m_nextShardIterator; }

    /**
     * <p>The next position in the shard from which to start sequentially reading data
     * records. If set to <code>null</code>, the shard has been closed and the
     * requested iterator does not return any more data. </p>
     */
    inline void SetNextShardIterator(const Aws::String& value) { m_nextShardIterator = value; }

    /**
     * <p>The next position in the shard from which to start sequentially reading data
     * records. If set to <code>null</code>, the shard has been closed and the
     * requested iterator does not return any more data. </p>
     */
    inline void SetNextShardIterator(Aws::String&& value) { m_nextShardIterator = std::move(value); }

    /**
     * <p>The next position in the shard from which to start sequentially reading data
     * records. If set to <code>null</code>, the shard has been closed and the
     * requested iterator does not return any more data. </p>
     */
    inline void SetNextShardIterator(const char* value) { m_nextShardIterator.assign(value); }

    /**
     * <p>The next position in the shard from which to start sequentially reading data
     * records. If set to <code>null</code>, the shard has been closed and the
     * requested iterator does not return any more data. </p>
     */
    inline GetRecordsResult& WithNextShardIterator(const Aws::String& value) { SetNextShardIterator(value); return *this;}

    /**
     * <p>The next position in the shard from which to start sequentially reading data
     * records. If set to <code>null</code>, the shard has been closed and the
     * requested iterator does not return any more data. </p>
     */
    inline GetRecordsResult& WithNextShardIterator(Aws::String&& value) { SetNextShardIterator(std::move(value)); return *this;}

    /**
     * <p>The next position in the shard from which to start sequentially reading data
     * records. If set to <code>null</code>, the shard has been closed and the
     * requested iterator does not return any more data. </p>
     */
    inline GetRecordsResult& WithNextShardIterator(const char* value) { SetNextShardIterator(value); return *this;}


    /**
     * <p>The number of milliseconds the <a>GetRecords</a> response is from the tip of
     * the stream, indicating how far behind current time the consumer is. A value of
     * zero indicates that record processing is caught up, and there are no new records
     * to process at this moment.</p>
     */
    inline long long GetMillisBehindLatest() const{ return m_millisBehindLatest; }

    /**
     * <p>The number of milliseconds the <a>GetRecords</a> response is from the tip of
     * the stream, indicating how far behind current time the consumer is. A value of
     * zero indicates that record processing is caught up, and there are no new records
     * to process at this moment.</p>
     */
    inline void SetMillisBehindLatest(long long value) { m_millisBehindLatest = value; }

    /**
     * <p>The number of milliseconds the <a>GetRecords</a> response is from the tip of
     * the stream, indicating how far behind current time the consumer is. A value of
     * zero indicates that record processing is caught up, and there are no new records
     * to process at this moment.</p>
     */
    inline GetRecordsResult& WithMillisBehindLatest(long long value) { SetMillisBehindLatest(value); return *this;}

  private:

    Aws::Vector<Record> m_records;

    Aws::String m_nextShardIterator;

    long long m_millisBehindLatest;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
