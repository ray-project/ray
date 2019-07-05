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
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/Array.h>
#include <aws/kinesis/model/EncryptionType.h>
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
   * <p>The unit of data of the Kinesis stream, which is composed of a sequence
   * number, a partition key, and a data blob.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/Record">AWS API
   * Reference</a></p>
   */
  class AWS_KINESIS_API Record
  {
  public:
    Record();
    Record(const Aws::Utils::Json::JsonValue& jsonValue);
    Record& operator=(const Aws::Utils::Json::JsonValue& jsonValue);
    Aws::Utils::Json::JsonValue Jsonize() const;


    /**
     * <p>The unique identifier of the record within its shard.</p>
     */
    inline const Aws::String& GetSequenceNumber() const{ return m_sequenceNumber; }

    /**
     * <p>The unique identifier of the record within its shard.</p>
     */
    inline void SetSequenceNumber(const Aws::String& value) { m_sequenceNumberHasBeenSet = true; m_sequenceNumber = value; }

    /**
     * <p>The unique identifier of the record within its shard.</p>
     */
    inline void SetSequenceNumber(Aws::String&& value) { m_sequenceNumberHasBeenSet = true; m_sequenceNumber = std::move(value); }

    /**
     * <p>The unique identifier of the record within its shard.</p>
     */
    inline void SetSequenceNumber(const char* value) { m_sequenceNumberHasBeenSet = true; m_sequenceNumber.assign(value); }

    /**
     * <p>The unique identifier of the record within its shard.</p>
     */
    inline Record& WithSequenceNumber(const Aws::String& value) { SetSequenceNumber(value); return *this;}

    /**
     * <p>The unique identifier of the record within its shard.</p>
     */
    inline Record& WithSequenceNumber(Aws::String&& value) { SetSequenceNumber(std::move(value)); return *this;}

    /**
     * <p>The unique identifier of the record within its shard.</p>
     */
    inline Record& WithSequenceNumber(const char* value) { SetSequenceNumber(value); return *this;}


    /**
     * <p>The approximate time that the record was inserted into the stream.</p>
     */
    inline const Aws::Utils::DateTime& GetApproximateArrivalTimestamp() const{ return m_approximateArrivalTimestamp; }

    /**
     * <p>The approximate time that the record was inserted into the stream.</p>
     */
    inline void SetApproximateArrivalTimestamp(const Aws::Utils::DateTime& value) { m_approximateArrivalTimestampHasBeenSet = true; m_approximateArrivalTimestamp = value; }

    /**
     * <p>The approximate time that the record was inserted into the stream.</p>
     */
    inline void SetApproximateArrivalTimestamp(Aws::Utils::DateTime&& value) { m_approximateArrivalTimestampHasBeenSet = true; m_approximateArrivalTimestamp = std::move(value); }

    /**
     * <p>The approximate time that the record was inserted into the stream.</p>
     */
    inline Record& WithApproximateArrivalTimestamp(const Aws::Utils::DateTime& value) { SetApproximateArrivalTimestamp(value); return *this;}

    /**
     * <p>The approximate time that the record was inserted into the stream.</p>
     */
    inline Record& WithApproximateArrivalTimestamp(Aws::Utils::DateTime&& value) { SetApproximateArrivalTimestamp(std::move(value)); return *this;}


    /**
     * <p>The data blob. The data in the blob is both opaque and immutable to Kinesis
     * Streams, which does not inspect, interpret, or change the data in the blob in
     * any way. When the data blob (the payload before base64-encoding) is added to the
     * partition key size, the total size must not exceed the maximum record size (1
     * MB).</p>
     */
    inline const Aws::Utils::ByteBuffer& GetData() const{ return m_data; }

    /**
     * <p>The data blob. The data in the blob is both opaque and immutable to Kinesis
     * Streams, which does not inspect, interpret, or change the data in the blob in
     * any way. When the data blob (the payload before base64-encoding) is added to the
     * partition key size, the total size must not exceed the maximum record size (1
     * MB).</p>
     */
    inline void SetData(const Aws::Utils::ByteBuffer& value) { m_dataHasBeenSet = true; m_data = value; }

    /**
     * <p>The data blob. The data in the blob is both opaque and immutable to Kinesis
     * Streams, which does not inspect, interpret, or change the data in the blob in
     * any way. When the data blob (the payload before base64-encoding) is added to the
     * partition key size, the total size must not exceed the maximum record size (1
     * MB).</p>
     */
    inline void SetData(Aws::Utils::ByteBuffer&& value) { m_dataHasBeenSet = true; m_data = std::move(value); }

    /**
     * <p>The data blob. The data in the blob is both opaque and immutable to Kinesis
     * Streams, which does not inspect, interpret, or change the data in the blob in
     * any way. When the data blob (the payload before base64-encoding) is added to the
     * partition key size, the total size must not exceed the maximum record size (1
     * MB).</p>
     */
    inline Record& WithData(const Aws::Utils::ByteBuffer& value) { SetData(value); return *this;}

    /**
     * <p>The data blob. The data in the blob is both opaque and immutable to Kinesis
     * Streams, which does not inspect, interpret, or change the data in the blob in
     * any way. When the data blob (the payload before base64-encoding) is added to the
     * partition key size, the total size must not exceed the maximum record size (1
     * MB).</p>
     */
    inline Record& WithData(Aws::Utils::ByteBuffer&& value) { SetData(std::move(value)); return *this;}


    /**
     * <p>Identifies which shard in the stream the data record is assigned to.</p>
     */
    inline const Aws::String& GetPartitionKey() const{ return m_partitionKey; }

    /**
     * <p>Identifies which shard in the stream the data record is assigned to.</p>
     */
    inline void SetPartitionKey(const Aws::String& value) { m_partitionKeyHasBeenSet = true; m_partitionKey = value; }

    /**
     * <p>Identifies which shard in the stream the data record is assigned to.</p>
     */
    inline void SetPartitionKey(Aws::String&& value) { m_partitionKeyHasBeenSet = true; m_partitionKey = std::move(value); }

    /**
     * <p>Identifies which shard in the stream the data record is assigned to.</p>
     */
    inline void SetPartitionKey(const char* value) { m_partitionKeyHasBeenSet = true; m_partitionKey.assign(value); }

    /**
     * <p>Identifies which shard in the stream the data record is assigned to.</p>
     */
    inline Record& WithPartitionKey(const Aws::String& value) { SetPartitionKey(value); return *this;}

    /**
     * <p>Identifies which shard in the stream the data record is assigned to.</p>
     */
    inline Record& WithPartitionKey(Aws::String&& value) { SetPartitionKey(std::move(value)); return *this;}

    /**
     * <p>Identifies which shard in the stream the data record is assigned to.</p>
     */
    inline Record& WithPartitionKey(const char* value) { SetPartitionKey(value); return *this;}


    /**
     * <p>The encryption type used on the record. This parameter can be one of the
     * following values:</p> <ul> <li> <p> <code>NONE</code>: Do not encrypt the
     * records in the stream.</p> </li> <li> <p> <code>KMS</code>: Use server-side
     * encryption on the records in the stream using a customer-managed KMS key.</p>
     * </li> </ul>
     */
    inline const EncryptionType& GetEncryptionType() const{ return m_encryptionType; }

    /**
     * <p>The encryption type used on the record. This parameter can be one of the
     * following values:</p> <ul> <li> <p> <code>NONE</code>: Do not encrypt the
     * records in the stream.</p> </li> <li> <p> <code>KMS</code>: Use server-side
     * encryption on the records in the stream using a customer-managed KMS key.</p>
     * </li> </ul>
     */
    inline void SetEncryptionType(const EncryptionType& value) { m_encryptionTypeHasBeenSet = true; m_encryptionType = value; }

    /**
     * <p>The encryption type used on the record. This parameter can be one of the
     * following values:</p> <ul> <li> <p> <code>NONE</code>: Do not encrypt the
     * records in the stream.</p> </li> <li> <p> <code>KMS</code>: Use server-side
     * encryption on the records in the stream using a customer-managed KMS key.</p>
     * </li> </ul>
     */
    inline void SetEncryptionType(EncryptionType&& value) { m_encryptionTypeHasBeenSet = true; m_encryptionType = std::move(value); }

    /**
     * <p>The encryption type used on the record. This parameter can be one of the
     * following values:</p> <ul> <li> <p> <code>NONE</code>: Do not encrypt the
     * records in the stream.</p> </li> <li> <p> <code>KMS</code>: Use server-side
     * encryption on the records in the stream using a customer-managed KMS key.</p>
     * </li> </ul>
     */
    inline Record& WithEncryptionType(const EncryptionType& value) { SetEncryptionType(value); return *this;}

    /**
     * <p>The encryption type used on the record. This parameter can be one of the
     * following values:</p> <ul> <li> <p> <code>NONE</code>: Do not encrypt the
     * records in the stream.</p> </li> <li> <p> <code>KMS</code>: Use server-side
     * encryption on the records in the stream using a customer-managed KMS key.</p>
     * </li> </ul>
     */
    inline Record& WithEncryptionType(EncryptionType&& value) { SetEncryptionType(std::move(value)); return *this;}

  private:

    Aws::String m_sequenceNumber;
    bool m_sequenceNumberHasBeenSet;

    Aws::Utils::DateTime m_approximateArrivalTimestamp;
    bool m_approximateArrivalTimestampHasBeenSet;

    Aws::Utils::ByteBuffer m_data;
    bool m_dataHasBeenSet;

    Aws::String m_partitionKey;
    bool m_partitionKeyHasBeenSet;

    EncryptionType m_encryptionType;
    bool m_encryptionTypeHasBeenSet;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
