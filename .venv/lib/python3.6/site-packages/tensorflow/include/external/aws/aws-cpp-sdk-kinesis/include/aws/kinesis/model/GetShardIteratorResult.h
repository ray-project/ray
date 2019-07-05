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
   * <p>Represents the output for <code>GetShardIterator</code>.</p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/GetShardIteratorOutput">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API GetShardIteratorResult
  {
  public:
    GetShardIteratorResult();
    GetShardIteratorResult(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);
    GetShardIteratorResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);


    /**
     * <p>The position in the shard from which to start reading data records
     * sequentially. A shard iterator specifies this position using the sequence number
     * of a data record in a shard.</p>
     */
    inline const Aws::String& GetShardIterator() const{ return m_shardIterator; }

    /**
     * <p>The position in the shard from which to start reading data records
     * sequentially. A shard iterator specifies this position using the sequence number
     * of a data record in a shard.</p>
     */
    inline void SetShardIterator(const Aws::String& value) { m_shardIterator = value; }

    /**
     * <p>The position in the shard from which to start reading data records
     * sequentially. A shard iterator specifies this position using the sequence number
     * of a data record in a shard.</p>
     */
    inline void SetShardIterator(Aws::String&& value) { m_shardIterator = std::move(value); }

    /**
     * <p>The position in the shard from which to start reading data records
     * sequentially. A shard iterator specifies this position using the sequence number
     * of a data record in a shard.</p>
     */
    inline void SetShardIterator(const char* value) { m_shardIterator.assign(value); }

    /**
     * <p>The position in the shard from which to start reading data records
     * sequentially. A shard iterator specifies this position using the sequence number
     * of a data record in a shard.</p>
     */
    inline GetShardIteratorResult& WithShardIterator(const Aws::String& value) { SetShardIterator(value); return *this;}

    /**
     * <p>The position in the shard from which to start reading data records
     * sequentially. A shard iterator specifies this position using the sequence number
     * of a data record in a shard.</p>
     */
    inline GetShardIteratorResult& WithShardIterator(Aws::String&& value) { SetShardIterator(std::move(value)); return *this;}

    /**
     * <p>The position in the shard from which to start reading data records
     * sequentially. A shard iterator specifies this position using the sequence number
     * of a data record in a shard.</p>
     */
    inline GetShardIteratorResult& WithShardIterator(const char* value) { SetShardIterator(value); return *this;}

  private:

    Aws::String m_shardIterator;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
