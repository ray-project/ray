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
   * <p>Represents the result of an individual record from a <code>PutRecords</code>
   * request. A record that is successfully added to a stream includes
   * <code>SequenceNumber</code> and <code>ShardId</code> in the result. A record
   * that fails to be added to the stream includes <code>ErrorCode</code> and
   * <code>ErrorMessage</code> in the result.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/PutRecordsResultEntry">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API PutRecordsResultEntry
  {
  public:
    PutRecordsResultEntry();
    PutRecordsResultEntry(const Aws::Utils::Json::JsonValue& jsonValue);
    PutRecordsResultEntry& operator=(const Aws::Utils::Json::JsonValue& jsonValue);
    Aws::Utils::Json::JsonValue Jsonize() const;


    /**
     * <p>The sequence number for an individual record result.</p>
     */
    inline const Aws::String& GetSequenceNumber() const{ return m_sequenceNumber; }

    /**
     * <p>The sequence number for an individual record result.</p>
     */
    inline void SetSequenceNumber(const Aws::String& value) { m_sequenceNumberHasBeenSet = true; m_sequenceNumber = value; }

    /**
     * <p>The sequence number for an individual record result.</p>
     */
    inline void SetSequenceNumber(Aws::String&& value) { m_sequenceNumberHasBeenSet = true; m_sequenceNumber = std::move(value); }

    /**
     * <p>The sequence number for an individual record result.</p>
     */
    inline void SetSequenceNumber(const char* value) { m_sequenceNumberHasBeenSet = true; m_sequenceNumber.assign(value); }

    /**
     * <p>The sequence number for an individual record result.</p>
     */
    inline PutRecordsResultEntry& WithSequenceNumber(const Aws::String& value) { SetSequenceNumber(value); return *this;}

    /**
     * <p>The sequence number for an individual record result.</p>
     */
    inline PutRecordsResultEntry& WithSequenceNumber(Aws::String&& value) { SetSequenceNumber(std::move(value)); return *this;}

    /**
     * <p>The sequence number for an individual record result.</p>
     */
    inline PutRecordsResultEntry& WithSequenceNumber(const char* value) { SetSequenceNumber(value); return *this;}


    /**
     * <p>The shard ID for an individual record result.</p>
     */
    inline const Aws::String& GetShardId() const{ return m_shardId; }

    /**
     * <p>The shard ID for an individual record result.</p>
     */
    inline void SetShardId(const Aws::String& value) { m_shardIdHasBeenSet = true; m_shardId = value; }

    /**
     * <p>The shard ID for an individual record result.</p>
     */
    inline void SetShardId(Aws::String&& value) { m_shardIdHasBeenSet = true; m_shardId = std::move(value); }

    /**
     * <p>The shard ID for an individual record result.</p>
     */
    inline void SetShardId(const char* value) { m_shardIdHasBeenSet = true; m_shardId.assign(value); }

    /**
     * <p>The shard ID for an individual record result.</p>
     */
    inline PutRecordsResultEntry& WithShardId(const Aws::String& value) { SetShardId(value); return *this;}

    /**
     * <p>The shard ID for an individual record result.</p>
     */
    inline PutRecordsResultEntry& WithShardId(Aws::String&& value) { SetShardId(std::move(value)); return *this;}

    /**
     * <p>The shard ID for an individual record result.</p>
     */
    inline PutRecordsResultEntry& WithShardId(const char* value) { SetShardId(value); return *this;}


    /**
     * <p>The error code for an individual record result. <code>ErrorCodes</code> can
     * be either <code>ProvisionedThroughputExceededException</code> or
     * <code>InternalFailure</code>.</p>
     */
    inline const Aws::String& GetErrorCode() const{ return m_errorCode; }

    /**
     * <p>The error code for an individual record result. <code>ErrorCodes</code> can
     * be either <code>ProvisionedThroughputExceededException</code> or
     * <code>InternalFailure</code>.</p>
     */
    inline void SetErrorCode(const Aws::String& value) { m_errorCodeHasBeenSet = true; m_errorCode = value; }

    /**
     * <p>The error code for an individual record result. <code>ErrorCodes</code> can
     * be either <code>ProvisionedThroughputExceededException</code> or
     * <code>InternalFailure</code>.</p>
     */
    inline void SetErrorCode(Aws::String&& value) { m_errorCodeHasBeenSet = true; m_errorCode = std::move(value); }

    /**
     * <p>The error code for an individual record result. <code>ErrorCodes</code> can
     * be either <code>ProvisionedThroughputExceededException</code> or
     * <code>InternalFailure</code>.</p>
     */
    inline void SetErrorCode(const char* value) { m_errorCodeHasBeenSet = true; m_errorCode.assign(value); }

    /**
     * <p>The error code for an individual record result. <code>ErrorCodes</code> can
     * be either <code>ProvisionedThroughputExceededException</code> or
     * <code>InternalFailure</code>.</p>
     */
    inline PutRecordsResultEntry& WithErrorCode(const Aws::String& value) { SetErrorCode(value); return *this;}

    /**
     * <p>The error code for an individual record result. <code>ErrorCodes</code> can
     * be either <code>ProvisionedThroughputExceededException</code> or
     * <code>InternalFailure</code>.</p>
     */
    inline PutRecordsResultEntry& WithErrorCode(Aws::String&& value) { SetErrorCode(std::move(value)); return *this;}

    /**
     * <p>The error code for an individual record result. <code>ErrorCodes</code> can
     * be either <code>ProvisionedThroughputExceededException</code> or
     * <code>InternalFailure</code>.</p>
     */
    inline PutRecordsResultEntry& WithErrorCode(const char* value) { SetErrorCode(value); return *this;}


    /**
     * <p>The error message for an individual record result. An <code>ErrorCode</code>
     * value of <code>ProvisionedThroughputExceededException</code> has an error
     * message that includes the account ID, stream name, and shard ID. An
     * <code>ErrorCode</code> value of <code>InternalFailure</code> has the error
     * message <code>"Internal Service Failure"</code>.</p>
     */
    inline const Aws::String& GetErrorMessage() const{ return m_errorMessage; }

    /**
     * <p>The error message for an individual record result. An <code>ErrorCode</code>
     * value of <code>ProvisionedThroughputExceededException</code> has an error
     * message that includes the account ID, stream name, and shard ID. An
     * <code>ErrorCode</code> value of <code>InternalFailure</code> has the error
     * message <code>"Internal Service Failure"</code>.</p>
     */
    inline void SetErrorMessage(const Aws::String& value) { m_errorMessageHasBeenSet = true; m_errorMessage = value; }

    /**
     * <p>The error message for an individual record result. An <code>ErrorCode</code>
     * value of <code>ProvisionedThroughputExceededException</code> has an error
     * message that includes the account ID, stream name, and shard ID. An
     * <code>ErrorCode</code> value of <code>InternalFailure</code> has the error
     * message <code>"Internal Service Failure"</code>.</p>
     */
    inline void SetErrorMessage(Aws::String&& value) { m_errorMessageHasBeenSet = true; m_errorMessage = std::move(value); }

    /**
     * <p>The error message for an individual record result. An <code>ErrorCode</code>
     * value of <code>ProvisionedThroughputExceededException</code> has an error
     * message that includes the account ID, stream name, and shard ID. An
     * <code>ErrorCode</code> value of <code>InternalFailure</code> has the error
     * message <code>"Internal Service Failure"</code>.</p>
     */
    inline void SetErrorMessage(const char* value) { m_errorMessageHasBeenSet = true; m_errorMessage.assign(value); }

    /**
     * <p>The error message for an individual record result. An <code>ErrorCode</code>
     * value of <code>ProvisionedThroughputExceededException</code> has an error
     * message that includes the account ID, stream name, and shard ID. An
     * <code>ErrorCode</code> value of <code>InternalFailure</code> has the error
     * message <code>"Internal Service Failure"</code>.</p>
     */
    inline PutRecordsResultEntry& WithErrorMessage(const Aws::String& value) { SetErrorMessage(value); return *this;}

    /**
     * <p>The error message for an individual record result. An <code>ErrorCode</code>
     * value of <code>ProvisionedThroughputExceededException</code> has an error
     * message that includes the account ID, stream name, and shard ID. An
     * <code>ErrorCode</code> value of <code>InternalFailure</code> has the error
     * message <code>"Internal Service Failure"</code>.</p>
     */
    inline PutRecordsResultEntry& WithErrorMessage(Aws::String&& value) { SetErrorMessage(std::move(value)); return *this;}

    /**
     * <p>The error message for an individual record result. An <code>ErrorCode</code>
     * value of <code>ProvisionedThroughputExceededException</code> has an error
     * message that includes the account ID, stream name, and shard ID. An
     * <code>ErrorCode</code> value of <code>InternalFailure</code> has the error
     * message <code>"Internal Service Failure"</code>.</p>
     */
    inline PutRecordsResultEntry& WithErrorMessage(const char* value) { SetErrorMessage(value); return *this;}

  private:

    Aws::String m_sequenceNumber;
    bool m_sequenceNumberHasBeenSet;

    Aws::String m_shardId;
    bool m_shardIdHasBeenSet;

    Aws::String m_errorCode;
    bool m_errorCodeHasBeenSet;

    Aws::String m_errorMessage;
    bool m_errorMessageHasBeenSet;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
