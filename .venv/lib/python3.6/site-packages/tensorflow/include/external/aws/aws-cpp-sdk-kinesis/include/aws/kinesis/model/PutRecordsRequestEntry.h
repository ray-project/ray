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
#include <aws/core/utils/Array.h>
#include <aws/core/utils/memory/stl/AWSString.h>
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
   * <p>Represents the output for <code>PutRecords</code>.</p><p><h3>See Also:</h3>  
   * <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/PutRecordsRequestEntry">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API PutRecordsRequestEntry
  {
  public:
    PutRecordsRequestEntry();
    PutRecordsRequestEntry(const Aws::Utils::Json::JsonValue& jsonValue);
    PutRecordsRequestEntry& operator=(const Aws::Utils::Json::JsonValue& jsonValue);
    Aws::Utils::Json::JsonValue Jsonize() const;


    /**
     * <p>The data blob to put into the record, which is base64-encoded when the blob
     * is serialized. When the data blob (the payload before base64-encoding) is added
     * to the partition key size, the total size must not exceed the maximum record
     * size (1 MB).</p>
     */
    inline const Aws::Utils::ByteBuffer& GetData() const{ return m_data; }

    /**
     * <p>The data blob to put into the record, which is base64-encoded when the blob
     * is serialized. When the data blob (the payload before base64-encoding) is added
     * to the partition key size, the total size must not exceed the maximum record
     * size (1 MB).</p>
     */
    inline void SetData(const Aws::Utils::ByteBuffer& value) { m_dataHasBeenSet = true; m_data = value; }

    /**
     * <p>The data blob to put into the record, which is base64-encoded when the blob
     * is serialized. When the data blob (the payload before base64-encoding) is added
     * to the partition key size, the total size must not exceed the maximum record
     * size (1 MB).</p>
     */
    inline void SetData(Aws::Utils::ByteBuffer&& value) { m_dataHasBeenSet = true; m_data = std::move(value); }

    /**
     * <p>The data blob to put into the record, which is base64-encoded when the blob
     * is serialized. When the data blob (the payload before base64-encoding) is added
     * to the partition key size, the total size must not exceed the maximum record
     * size (1 MB).</p>
     */
    inline PutRecordsRequestEntry& WithData(const Aws::Utils::ByteBuffer& value) { SetData(value); return *this;}

    /**
     * <p>The data blob to put into the record, which is base64-encoded when the blob
     * is serialized. When the data blob (the payload before base64-encoding) is added
     * to the partition key size, the total size must not exceed the maximum record
     * size (1 MB).</p>
     */
    inline PutRecordsRequestEntry& WithData(Aws::Utils::ByteBuffer&& value) { SetData(std::move(value)); return *this;}


    /**
     * <p>The hash value used to determine explicitly the shard that the data record is
     * assigned to by overriding the partition key hash.</p>
     */
    inline const Aws::String& GetExplicitHashKey() const{ return m_explicitHashKey; }

    /**
     * <p>The hash value used to determine explicitly the shard that the data record is
     * assigned to by overriding the partition key hash.</p>
     */
    inline void SetExplicitHashKey(const Aws::String& value) { m_explicitHashKeyHasBeenSet = true; m_explicitHashKey = value; }

    /**
     * <p>The hash value used to determine explicitly the shard that the data record is
     * assigned to by overriding the partition key hash.</p>
     */
    inline void SetExplicitHashKey(Aws::String&& value) { m_explicitHashKeyHasBeenSet = true; m_explicitHashKey = std::move(value); }

    /**
     * <p>The hash value used to determine explicitly the shard that the data record is
     * assigned to by overriding the partition key hash.</p>
     */
    inline void SetExplicitHashKey(const char* value) { m_explicitHashKeyHasBeenSet = true; m_explicitHashKey.assign(value); }

    /**
     * <p>The hash value used to determine explicitly the shard that the data record is
     * assigned to by overriding the partition key hash.</p>
     */
    inline PutRecordsRequestEntry& WithExplicitHashKey(const Aws::String& value) { SetExplicitHashKey(value); return *this;}

    /**
     * <p>The hash value used to determine explicitly the shard that the data record is
     * assigned to by overriding the partition key hash.</p>
     */
    inline PutRecordsRequestEntry& WithExplicitHashKey(Aws::String&& value) { SetExplicitHashKey(std::move(value)); return *this;}

    /**
     * <p>The hash value used to determine explicitly the shard that the data record is
     * assigned to by overriding the partition key hash.</p>
     */
    inline PutRecordsRequestEntry& WithExplicitHashKey(const char* value) { SetExplicitHashKey(value); return *this;}


    /**
     * <p>Determines which shard in the stream the data record is assigned to.
     * Partition keys are Unicode strings with a maximum length limit of 256 characters
     * for each key. Amazon Kinesis uses the partition key as input to a hash function
     * that maps the partition key and associated data to a specific shard.
     * Specifically, an MD5 hash function is used to map partition keys to 128-bit
     * integer values and to map associated data records to shards. As a result of this
     * hashing mechanism, all data records with the same partition key map to the same
     * shard within the stream.</p>
     */
    inline const Aws::String& GetPartitionKey() const{ return m_partitionKey; }

    /**
     * <p>Determines which shard in the stream the data record is assigned to.
     * Partition keys are Unicode strings with a maximum length limit of 256 characters
     * for each key. Amazon Kinesis uses the partition key as input to a hash function
     * that maps the partition key and associated data to a specific shard.
     * Specifically, an MD5 hash function is used to map partition keys to 128-bit
     * integer values and to map associated data records to shards. As a result of this
     * hashing mechanism, all data records with the same partition key map to the same
     * shard within the stream.</p>
     */
    inline void SetPartitionKey(const Aws::String& value) { m_partitionKeyHasBeenSet = true; m_partitionKey = value; }

    /**
     * <p>Determines which shard in the stream the data record is assigned to.
     * Partition keys are Unicode strings with a maximum length limit of 256 characters
     * for each key. Amazon Kinesis uses the partition key as input to a hash function
     * that maps the partition key and associated data to a specific shard.
     * Specifically, an MD5 hash function is used to map partition keys to 128-bit
     * integer values and to map associated data records to shards. As a result of this
     * hashing mechanism, all data records with the same partition key map to the same
     * shard within the stream.</p>
     */
    inline void SetPartitionKey(Aws::String&& value) { m_partitionKeyHasBeenSet = true; m_partitionKey = std::move(value); }

    /**
     * <p>Determines which shard in the stream the data record is assigned to.
     * Partition keys are Unicode strings with a maximum length limit of 256 characters
     * for each key. Amazon Kinesis uses the partition key as input to a hash function
     * that maps the partition key and associated data to a specific shard.
     * Specifically, an MD5 hash function is used to map partition keys to 128-bit
     * integer values and to map associated data records to shards. As a result of this
     * hashing mechanism, all data records with the same partition key map to the same
     * shard within the stream.</p>
     */
    inline void SetPartitionKey(const char* value) { m_partitionKeyHasBeenSet = true; m_partitionKey.assign(value); }

    /**
     * <p>Determines which shard in the stream the data record is assigned to.
     * Partition keys are Unicode strings with a maximum length limit of 256 characters
     * for each key. Amazon Kinesis uses the partition key as input to a hash function
     * that maps the partition key and associated data to a specific shard.
     * Specifically, an MD5 hash function is used to map partition keys to 128-bit
     * integer values and to map associated data records to shards. As a result of this
     * hashing mechanism, all data records with the same partition key map to the same
     * shard within the stream.</p>
     */
    inline PutRecordsRequestEntry& WithPartitionKey(const Aws::String& value) { SetPartitionKey(value); return *this;}

    /**
     * <p>Determines which shard in the stream the data record is assigned to.
     * Partition keys are Unicode strings with a maximum length limit of 256 characters
     * for each key. Amazon Kinesis uses the partition key as input to a hash function
     * that maps the partition key and associated data to a specific shard.
     * Specifically, an MD5 hash function is used to map partition keys to 128-bit
     * integer values and to map associated data records to shards. As a result of this
     * hashing mechanism, all data records with the same partition key map to the same
     * shard within the stream.</p>
     */
    inline PutRecordsRequestEntry& WithPartitionKey(Aws::String&& value) { SetPartitionKey(std::move(value)); return *this;}

    /**
     * <p>Determines which shard in the stream the data record is assigned to.
     * Partition keys are Unicode strings with a maximum length limit of 256 characters
     * for each key. Amazon Kinesis uses the partition key as input to a hash function
     * that maps the partition key and associated data to a specific shard.
     * Specifically, an MD5 hash function is used to map partition keys to 128-bit
     * integer values and to map associated data records to shards. As a result of this
     * hashing mechanism, all data records with the same partition key map to the same
     * shard within the stream.</p>
     */
    inline PutRecordsRequestEntry& WithPartitionKey(const char* value) { SetPartitionKey(value); return *this;}

  private:

    Aws::Utils::ByteBuffer m_data;
    bool m_dataHasBeenSet;

    Aws::String m_explicitHashKey;
    bool m_explicitHashKeyHasBeenSet;

    Aws::String m_partitionKey;
    bool m_partitionKeyHasBeenSet;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
