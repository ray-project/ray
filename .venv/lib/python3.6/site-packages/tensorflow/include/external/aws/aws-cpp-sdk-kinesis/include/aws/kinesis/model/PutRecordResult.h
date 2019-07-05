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
#include <aws/kinesis/model/EncryptionType.h>
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
   * <p>Represents the output for <code>PutRecord</code>.</p><p><h3>See Also:</h3>  
   * <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/PutRecordOutput">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API PutRecordResult
  {
  public:
    PutRecordResult();
    PutRecordResult(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);
    PutRecordResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);


    /**
     * <p>The shard ID of the shard where the data record was placed.</p>
     */
    inline const Aws::String& GetShardId() const{ return m_shardId; }

    /**
     * <p>The shard ID of the shard where the data record was placed.</p>
     */
    inline void SetShardId(const Aws::String& value) { m_shardId = value; }

    /**
     * <p>The shard ID of the shard where the data record was placed.</p>
     */
    inline void SetShardId(Aws::String&& value) { m_shardId = std::move(value); }

    /**
     * <p>The shard ID of the shard where the data record was placed.</p>
     */
    inline void SetShardId(const char* value) { m_shardId.assign(value); }

    /**
     * <p>The shard ID of the shard where the data record was placed.</p>
     */
    inline PutRecordResult& WithShardId(const Aws::String& value) { SetShardId(value); return *this;}

    /**
     * <p>The shard ID of the shard where the data record was placed.</p>
     */
    inline PutRecordResult& WithShardId(Aws::String&& value) { SetShardId(std::move(value)); return *this;}

    /**
     * <p>The shard ID of the shard where the data record was placed.</p>
     */
    inline PutRecordResult& WithShardId(const char* value) { SetShardId(value); return *this;}


    /**
     * <p>The sequence number identifier that was assigned to the put data record. The
     * sequence number for the record is unique across all records in the stream. A
     * sequence number is the identifier associated with every record put into the
     * stream.</p>
     */
    inline const Aws::String& GetSequenceNumber() const{ return m_sequenceNumber; }

    /**
     * <p>The sequence number identifier that was assigned to the put data record. The
     * sequence number for the record is unique across all records in the stream. A
     * sequence number is the identifier associated with every record put into the
     * stream.</p>
     */
    inline void SetSequenceNumber(const Aws::String& value) { m_sequenceNumber = value; }

    /**
     * <p>The sequence number identifier that was assigned to the put data record. The
     * sequence number for the record is unique across all records in the stream. A
     * sequence number is the identifier associated with every record put into the
     * stream.</p>
     */
    inline void SetSequenceNumber(Aws::String&& value) { m_sequenceNumber = std::move(value); }

    /**
     * <p>The sequence number identifier that was assigned to the put data record. The
     * sequence number for the record is unique across all records in the stream. A
     * sequence number is the identifier associated with every record put into the
     * stream.</p>
     */
    inline void SetSequenceNumber(const char* value) { m_sequenceNumber.assign(value); }

    /**
     * <p>The sequence number identifier that was assigned to the put data record. The
     * sequence number for the record is unique across all records in the stream. A
     * sequence number is the identifier associated with every record put into the
     * stream.</p>
     */
    inline PutRecordResult& WithSequenceNumber(const Aws::String& value) { SetSequenceNumber(value); return *this;}

    /**
     * <p>The sequence number identifier that was assigned to the put data record. The
     * sequence number for the record is unique across all records in the stream. A
     * sequence number is the identifier associated with every record put into the
     * stream.</p>
     */
    inline PutRecordResult& WithSequenceNumber(Aws::String&& value) { SetSequenceNumber(std::move(value)); return *this;}

    /**
     * <p>The sequence number identifier that was assigned to the put data record. The
     * sequence number for the record is unique across all records in the stream. A
     * sequence number is the identifier associated with every record put into the
     * stream.</p>
     */
    inline PutRecordResult& WithSequenceNumber(const char* value) { SetSequenceNumber(value); return *this;}


    /**
     * <p>The encryption type to use on the record. This parameter can be one of the
     * following values:</p> <ul> <li> <p> <code>NONE</code>: Do not encrypt the
     * records in the stream.</p> </li> <li> <p> <code>KMS</code>: Use server-side
     * encryption on the records in the stream using a customer-managed KMS key.</p>
     * </li> </ul>
     */
    inline const EncryptionType& GetEncryptionType() const{ return m_encryptionType; }

    /**
     * <p>The encryption type to use on the record. This parameter can be one of the
     * following values:</p> <ul> <li> <p> <code>NONE</code>: Do not encrypt the
     * records in the stream.</p> </li> <li> <p> <code>KMS</code>: Use server-side
     * encryption on the records in the stream using a customer-managed KMS key.</p>
     * </li> </ul>
     */
    inline void SetEncryptionType(const EncryptionType& value) { m_encryptionType = value; }

    /**
     * <p>The encryption type to use on the record. This parameter can be one of the
     * following values:</p> <ul> <li> <p> <code>NONE</code>: Do not encrypt the
     * records in the stream.</p> </li> <li> <p> <code>KMS</code>: Use server-side
     * encryption on the records in the stream using a customer-managed KMS key.</p>
     * </li> </ul>
     */
    inline void SetEncryptionType(EncryptionType&& value) { m_encryptionType = std::move(value); }

    /**
     * <p>The encryption type to use on the record. This parameter can be one of the
     * following values:</p> <ul> <li> <p> <code>NONE</code>: Do not encrypt the
     * records in the stream.</p> </li> <li> <p> <code>KMS</code>: Use server-side
     * encryption on the records in the stream using a customer-managed KMS key.</p>
     * </li> </ul>
     */
    inline PutRecordResult& WithEncryptionType(const EncryptionType& value) { SetEncryptionType(value); return *this;}

    /**
     * <p>The encryption type to use on the record. This parameter can be one of the
     * following values:</p> <ul> <li> <p> <code>NONE</code>: Do not encrypt the
     * records in the stream.</p> </li> <li> <p> <code>KMS</code>: Use server-side
     * encryption on the records in the stream using a customer-managed KMS key.</p>
     * </li> </ul>
     */
    inline PutRecordResult& WithEncryptionType(EncryptionType&& value) { SetEncryptionType(std::move(value)); return *this;}

  private:

    Aws::String m_shardId;

    Aws::String m_sequenceNumber;

    EncryptionType m_encryptionType;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
