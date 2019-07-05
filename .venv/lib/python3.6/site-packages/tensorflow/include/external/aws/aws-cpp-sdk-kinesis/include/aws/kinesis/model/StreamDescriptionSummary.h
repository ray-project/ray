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
#include <aws/kinesis/model/StreamStatus.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/kinesis/model/EncryptionType.h>
#include <aws/kinesis/model/EnhancedMetrics.h>
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
   * <p>Represents the output for <a>DescribeStreamSummary</a> </p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/StreamDescriptionSummary">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API StreamDescriptionSummary
  {
  public:
    StreamDescriptionSummary();
    StreamDescriptionSummary(const Aws::Utils::Json::JsonValue& jsonValue);
    StreamDescriptionSummary& operator=(const Aws::Utils::Json::JsonValue& jsonValue);
    Aws::Utils::Json::JsonValue Jsonize() const;


    /**
     * <p>The name of the stream being described.</p>
     */
    inline const Aws::String& GetStreamName() const{ return m_streamName; }

    /**
     * <p>The name of the stream being described.</p>
     */
    inline void SetStreamName(const Aws::String& value) { m_streamNameHasBeenSet = true; m_streamName = value; }

    /**
     * <p>The name of the stream being described.</p>
     */
    inline void SetStreamName(Aws::String&& value) { m_streamNameHasBeenSet = true; m_streamName = std::move(value); }

    /**
     * <p>The name of the stream being described.</p>
     */
    inline void SetStreamName(const char* value) { m_streamNameHasBeenSet = true; m_streamName.assign(value); }

    /**
     * <p>The name of the stream being described.</p>
     */
    inline StreamDescriptionSummary& WithStreamName(const Aws::String& value) { SetStreamName(value); return *this;}

    /**
     * <p>The name of the stream being described.</p>
     */
    inline StreamDescriptionSummary& WithStreamName(Aws::String&& value) { SetStreamName(std::move(value)); return *this;}

    /**
     * <p>The name of the stream being described.</p>
     */
    inline StreamDescriptionSummary& WithStreamName(const char* value) { SetStreamName(value); return *this;}


    /**
     * <p>The Amazon Resource Name (ARN) for the stream being described.</p>
     */
    inline const Aws::String& GetStreamARN() const{ return m_streamARN; }

    /**
     * <p>The Amazon Resource Name (ARN) for the stream being described.</p>
     */
    inline void SetStreamARN(const Aws::String& value) { m_streamARNHasBeenSet = true; m_streamARN = value; }

    /**
     * <p>The Amazon Resource Name (ARN) for the stream being described.</p>
     */
    inline void SetStreamARN(Aws::String&& value) { m_streamARNHasBeenSet = true; m_streamARN = std::move(value); }

    /**
     * <p>The Amazon Resource Name (ARN) for the stream being described.</p>
     */
    inline void SetStreamARN(const char* value) { m_streamARNHasBeenSet = true; m_streamARN.assign(value); }

    /**
     * <p>The Amazon Resource Name (ARN) for the stream being described.</p>
     */
    inline StreamDescriptionSummary& WithStreamARN(const Aws::String& value) { SetStreamARN(value); return *this;}

    /**
     * <p>The Amazon Resource Name (ARN) for the stream being described.</p>
     */
    inline StreamDescriptionSummary& WithStreamARN(Aws::String&& value) { SetStreamARN(std::move(value)); return *this;}

    /**
     * <p>The Amazon Resource Name (ARN) for the stream being described.</p>
     */
    inline StreamDescriptionSummary& WithStreamARN(const char* value) { SetStreamARN(value); return *this;}


    /**
     * <p>The current status of the stream being described. The stream status is one of
     * the following states:</p> <ul> <li> <p> <code>CREATING</code> - The stream is
     * being created. Kinesis Streams immediately returns and sets
     * <code>StreamStatus</code> to <code>CREATING</code>.</p> </li> <li> <p>
     * <code>DELETING</code> - The stream is being deleted. The specified stream is in
     * the <code>DELETING</code> state until Kinesis Streams completes the
     * deletion.</p> </li> <li> <p> <code>ACTIVE</code> - The stream exists and is
     * ready for read and write operations or deletion. You should perform read and
     * write operations only on an <code>ACTIVE</code> stream.</p> </li> <li> <p>
     * <code>UPDATING</code> - Shards in the stream are being merged or split. Read and
     * write operations continue to work while the stream is in the
     * <code>UPDATING</code> state.</p> </li> </ul>
     */
    inline const StreamStatus& GetStreamStatus() const{ return m_streamStatus; }

    /**
     * <p>The current status of the stream being described. The stream status is one of
     * the following states:</p> <ul> <li> <p> <code>CREATING</code> - The stream is
     * being created. Kinesis Streams immediately returns and sets
     * <code>StreamStatus</code> to <code>CREATING</code>.</p> </li> <li> <p>
     * <code>DELETING</code> - The stream is being deleted. The specified stream is in
     * the <code>DELETING</code> state until Kinesis Streams completes the
     * deletion.</p> </li> <li> <p> <code>ACTIVE</code> - The stream exists and is
     * ready for read and write operations or deletion. You should perform read and
     * write operations only on an <code>ACTIVE</code> stream.</p> </li> <li> <p>
     * <code>UPDATING</code> - Shards in the stream are being merged or split. Read and
     * write operations continue to work while the stream is in the
     * <code>UPDATING</code> state.</p> </li> </ul>
     */
    inline void SetStreamStatus(const StreamStatus& value) { m_streamStatusHasBeenSet = true; m_streamStatus = value; }

    /**
     * <p>The current status of the stream being described. The stream status is one of
     * the following states:</p> <ul> <li> <p> <code>CREATING</code> - The stream is
     * being created. Kinesis Streams immediately returns and sets
     * <code>StreamStatus</code> to <code>CREATING</code>.</p> </li> <li> <p>
     * <code>DELETING</code> - The stream is being deleted. The specified stream is in
     * the <code>DELETING</code> state until Kinesis Streams completes the
     * deletion.</p> </li> <li> <p> <code>ACTIVE</code> - The stream exists and is
     * ready for read and write operations or deletion. You should perform read and
     * write operations only on an <code>ACTIVE</code> stream.</p> </li> <li> <p>
     * <code>UPDATING</code> - Shards in the stream are being merged or split. Read and
     * write operations continue to work while the stream is in the
     * <code>UPDATING</code> state.</p> </li> </ul>
     */
    inline void SetStreamStatus(StreamStatus&& value) { m_streamStatusHasBeenSet = true; m_streamStatus = std::move(value); }

    /**
     * <p>The current status of the stream being described. The stream status is one of
     * the following states:</p> <ul> <li> <p> <code>CREATING</code> - The stream is
     * being created. Kinesis Streams immediately returns and sets
     * <code>StreamStatus</code> to <code>CREATING</code>.</p> </li> <li> <p>
     * <code>DELETING</code> - The stream is being deleted. The specified stream is in
     * the <code>DELETING</code> state until Kinesis Streams completes the
     * deletion.</p> </li> <li> <p> <code>ACTIVE</code> - The stream exists and is
     * ready for read and write operations or deletion. You should perform read and
     * write operations only on an <code>ACTIVE</code> stream.</p> </li> <li> <p>
     * <code>UPDATING</code> - Shards in the stream are being merged or split. Read and
     * write operations continue to work while the stream is in the
     * <code>UPDATING</code> state.</p> </li> </ul>
     */
    inline StreamDescriptionSummary& WithStreamStatus(const StreamStatus& value) { SetStreamStatus(value); return *this;}

    /**
     * <p>The current status of the stream being described. The stream status is one of
     * the following states:</p> <ul> <li> <p> <code>CREATING</code> - The stream is
     * being created. Kinesis Streams immediately returns and sets
     * <code>StreamStatus</code> to <code>CREATING</code>.</p> </li> <li> <p>
     * <code>DELETING</code> - The stream is being deleted. The specified stream is in
     * the <code>DELETING</code> state until Kinesis Streams completes the
     * deletion.</p> </li> <li> <p> <code>ACTIVE</code> - The stream exists and is
     * ready for read and write operations or deletion. You should perform read and
     * write operations only on an <code>ACTIVE</code> stream.</p> </li> <li> <p>
     * <code>UPDATING</code> - Shards in the stream are being merged or split. Read and
     * write operations continue to work while the stream is in the
     * <code>UPDATING</code> state.</p> </li> </ul>
     */
    inline StreamDescriptionSummary& WithStreamStatus(StreamStatus&& value) { SetStreamStatus(std::move(value)); return *this;}


    /**
     * <p>The current retention period, in hours.</p>
     */
    inline int GetRetentionPeriodHours() const{ return m_retentionPeriodHours; }

    /**
     * <p>The current retention period, in hours.</p>
     */
    inline void SetRetentionPeriodHours(int value) { m_retentionPeriodHoursHasBeenSet = true; m_retentionPeriodHours = value; }

    /**
     * <p>The current retention period, in hours.</p>
     */
    inline StreamDescriptionSummary& WithRetentionPeriodHours(int value) { SetRetentionPeriodHours(value); return *this;}


    /**
     * <p>The approximate time that the stream was created.</p>
     */
    inline const Aws::Utils::DateTime& GetStreamCreationTimestamp() const{ return m_streamCreationTimestamp; }

    /**
     * <p>The approximate time that the stream was created.</p>
     */
    inline void SetStreamCreationTimestamp(const Aws::Utils::DateTime& value) { m_streamCreationTimestampHasBeenSet = true; m_streamCreationTimestamp = value; }

    /**
     * <p>The approximate time that the stream was created.</p>
     */
    inline void SetStreamCreationTimestamp(Aws::Utils::DateTime&& value) { m_streamCreationTimestampHasBeenSet = true; m_streamCreationTimestamp = std::move(value); }

    /**
     * <p>The approximate time that the stream was created.</p>
     */
    inline StreamDescriptionSummary& WithStreamCreationTimestamp(const Aws::Utils::DateTime& value) { SetStreamCreationTimestamp(value); return *this;}

    /**
     * <p>The approximate time that the stream was created.</p>
     */
    inline StreamDescriptionSummary& WithStreamCreationTimestamp(Aws::Utils::DateTime&& value) { SetStreamCreationTimestamp(std::move(value)); return *this;}


    /**
     * <p>Represents the current enhanced monitoring settings of the stream.</p>
     */
    inline const Aws::Vector<EnhancedMetrics>& GetEnhancedMonitoring() const{ return m_enhancedMonitoring; }

    /**
     * <p>Represents the current enhanced monitoring settings of the stream.</p>
     */
    inline void SetEnhancedMonitoring(const Aws::Vector<EnhancedMetrics>& value) { m_enhancedMonitoringHasBeenSet = true; m_enhancedMonitoring = value; }

    /**
     * <p>Represents the current enhanced monitoring settings of the stream.</p>
     */
    inline void SetEnhancedMonitoring(Aws::Vector<EnhancedMetrics>&& value) { m_enhancedMonitoringHasBeenSet = true; m_enhancedMonitoring = std::move(value); }

    /**
     * <p>Represents the current enhanced monitoring settings of the stream.</p>
     */
    inline StreamDescriptionSummary& WithEnhancedMonitoring(const Aws::Vector<EnhancedMetrics>& value) { SetEnhancedMonitoring(value); return *this;}

    /**
     * <p>Represents the current enhanced monitoring settings of the stream.</p>
     */
    inline StreamDescriptionSummary& WithEnhancedMonitoring(Aws::Vector<EnhancedMetrics>&& value) { SetEnhancedMonitoring(std::move(value)); return *this;}

    /**
     * <p>Represents the current enhanced monitoring settings of the stream.</p>
     */
    inline StreamDescriptionSummary& AddEnhancedMonitoring(const EnhancedMetrics& value) { m_enhancedMonitoringHasBeenSet = true; m_enhancedMonitoring.push_back(value); return *this; }

    /**
     * <p>Represents the current enhanced monitoring settings of the stream.</p>
     */
    inline StreamDescriptionSummary& AddEnhancedMonitoring(EnhancedMetrics&& value) { m_enhancedMonitoringHasBeenSet = true; m_enhancedMonitoring.push_back(std::move(value)); return *this; }


    /**
     * <p>The encryption type used. This value is one of the following:</p> <ul> <li>
     * <p> <code>KMS</code> </p> </li> <li> <p> <code>NONE</code> </p> </li> </ul>
     */
    inline const EncryptionType& GetEncryptionType() const{ return m_encryptionType; }

    /**
     * <p>The encryption type used. This value is one of the following:</p> <ul> <li>
     * <p> <code>KMS</code> </p> </li> <li> <p> <code>NONE</code> </p> </li> </ul>
     */
    inline void SetEncryptionType(const EncryptionType& value) { m_encryptionTypeHasBeenSet = true; m_encryptionType = value; }

    /**
     * <p>The encryption type used. This value is one of the following:</p> <ul> <li>
     * <p> <code>KMS</code> </p> </li> <li> <p> <code>NONE</code> </p> </li> </ul>
     */
    inline void SetEncryptionType(EncryptionType&& value) { m_encryptionTypeHasBeenSet = true; m_encryptionType = std::move(value); }

    /**
     * <p>The encryption type used. This value is one of the following:</p> <ul> <li>
     * <p> <code>KMS</code> </p> </li> <li> <p> <code>NONE</code> </p> </li> </ul>
     */
    inline StreamDescriptionSummary& WithEncryptionType(const EncryptionType& value) { SetEncryptionType(value); return *this;}

    /**
     * <p>The encryption type used. This value is one of the following:</p> <ul> <li>
     * <p> <code>KMS</code> </p> </li> <li> <p> <code>NONE</code> </p> </li> </ul>
     */
    inline StreamDescriptionSummary& WithEncryptionType(EncryptionType&& value) { SetEncryptionType(std::move(value)); return *this;}


    /**
     * <p>The GUID for the customer-managed KMS key to use for encryption. This value
     * can be a globally unique identifier, a fully specified ARN to either an alias or
     * a key, or an alias name prefixed by "alias/".You can also use a master key owned
     * by Kinesis Streams by specifying the alias <code>aws/kinesis</code>.</p> <ul>
     * <li> <p>Key ARN example:
     * <code>arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012</code>
     * </p> </li> <li> <p>Alias ARN example: <code>
     * arn:aws:kms:us-east-1:123456789012:alias/MyAliasName</code> </p> </li> <li>
     * <p>Globally unique key ID example:
     * <code>12345678-1234-1234-1234-123456789012</code> </p> </li> <li> <p>Alias name
     * example: <code>alias/MyAliasName</code> </p> </li> <li> <p>Master key owned by
     * Kinesis: <code>alias/aws/kinesis</code> </p> </li> </ul>
     */
    inline const Aws::String& GetKeyId() const{ return m_keyId; }

    /**
     * <p>The GUID for the customer-managed KMS key to use for encryption. This value
     * can be a globally unique identifier, a fully specified ARN to either an alias or
     * a key, or an alias name prefixed by "alias/".You can also use a master key owned
     * by Kinesis Streams by specifying the alias <code>aws/kinesis</code>.</p> <ul>
     * <li> <p>Key ARN example:
     * <code>arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012</code>
     * </p> </li> <li> <p>Alias ARN example: <code>
     * arn:aws:kms:us-east-1:123456789012:alias/MyAliasName</code> </p> </li> <li>
     * <p>Globally unique key ID example:
     * <code>12345678-1234-1234-1234-123456789012</code> </p> </li> <li> <p>Alias name
     * example: <code>alias/MyAliasName</code> </p> </li> <li> <p>Master key owned by
     * Kinesis: <code>alias/aws/kinesis</code> </p> </li> </ul>
     */
    inline void SetKeyId(const Aws::String& value) { m_keyIdHasBeenSet = true; m_keyId = value; }

    /**
     * <p>The GUID for the customer-managed KMS key to use for encryption. This value
     * can be a globally unique identifier, a fully specified ARN to either an alias or
     * a key, or an alias name prefixed by "alias/".You can also use a master key owned
     * by Kinesis Streams by specifying the alias <code>aws/kinesis</code>.</p> <ul>
     * <li> <p>Key ARN example:
     * <code>arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012</code>
     * </p> </li> <li> <p>Alias ARN example: <code>
     * arn:aws:kms:us-east-1:123456789012:alias/MyAliasName</code> </p> </li> <li>
     * <p>Globally unique key ID example:
     * <code>12345678-1234-1234-1234-123456789012</code> </p> </li> <li> <p>Alias name
     * example: <code>alias/MyAliasName</code> </p> </li> <li> <p>Master key owned by
     * Kinesis: <code>alias/aws/kinesis</code> </p> </li> </ul>
     */
    inline void SetKeyId(Aws::String&& value) { m_keyIdHasBeenSet = true; m_keyId = std::move(value); }

    /**
     * <p>The GUID for the customer-managed KMS key to use for encryption. This value
     * can be a globally unique identifier, a fully specified ARN to either an alias or
     * a key, or an alias name prefixed by "alias/".You can also use a master key owned
     * by Kinesis Streams by specifying the alias <code>aws/kinesis</code>.</p> <ul>
     * <li> <p>Key ARN example:
     * <code>arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012</code>
     * </p> </li> <li> <p>Alias ARN example: <code>
     * arn:aws:kms:us-east-1:123456789012:alias/MyAliasName</code> </p> </li> <li>
     * <p>Globally unique key ID example:
     * <code>12345678-1234-1234-1234-123456789012</code> </p> </li> <li> <p>Alias name
     * example: <code>alias/MyAliasName</code> </p> </li> <li> <p>Master key owned by
     * Kinesis: <code>alias/aws/kinesis</code> </p> </li> </ul>
     */
    inline void SetKeyId(const char* value) { m_keyIdHasBeenSet = true; m_keyId.assign(value); }

    /**
     * <p>The GUID for the customer-managed KMS key to use for encryption. This value
     * can be a globally unique identifier, a fully specified ARN to either an alias or
     * a key, or an alias name prefixed by "alias/".You can also use a master key owned
     * by Kinesis Streams by specifying the alias <code>aws/kinesis</code>.</p> <ul>
     * <li> <p>Key ARN example:
     * <code>arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012</code>
     * </p> </li> <li> <p>Alias ARN example: <code>
     * arn:aws:kms:us-east-1:123456789012:alias/MyAliasName</code> </p> </li> <li>
     * <p>Globally unique key ID example:
     * <code>12345678-1234-1234-1234-123456789012</code> </p> </li> <li> <p>Alias name
     * example: <code>alias/MyAliasName</code> </p> </li> <li> <p>Master key owned by
     * Kinesis: <code>alias/aws/kinesis</code> </p> </li> </ul>
     */
    inline StreamDescriptionSummary& WithKeyId(const Aws::String& value) { SetKeyId(value); return *this;}

    /**
     * <p>The GUID for the customer-managed KMS key to use for encryption. This value
     * can be a globally unique identifier, a fully specified ARN to either an alias or
     * a key, or an alias name prefixed by "alias/".You can also use a master key owned
     * by Kinesis Streams by specifying the alias <code>aws/kinesis</code>.</p> <ul>
     * <li> <p>Key ARN example:
     * <code>arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012</code>
     * </p> </li> <li> <p>Alias ARN example: <code>
     * arn:aws:kms:us-east-1:123456789012:alias/MyAliasName</code> </p> </li> <li>
     * <p>Globally unique key ID example:
     * <code>12345678-1234-1234-1234-123456789012</code> </p> </li> <li> <p>Alias name
     * example: <code>alias/MyAliasName</code> </p> </li> <li> <p>Master key owned by
     * Kinesis: <code>alias/aws/kinesis</code> </p> </li> </ul>
     */
    inline StreamDescriptionSummary& WithKeyId(Aws::String&& value) { SetKeyId(std::move(value)); return *this;}

    /**
     * <p>The GUID for the customer-managed KMS key to use for encryption. This value
     * can be a globally unique identifier, a fully specified ARN to either an alias or
     * a key, or an alias name prefixed by "alias/".You can also use a master key owned
     * by Kinesis Streams by specifying the alias <code>aws/kinesis</code>.</p> <ul>
     * <li> <p>Key ARN example:
     * <code>arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012</code>
     * </p> </li> <li> <p>Alias ARN example: <code>
     * arn:aws:kms:us-east-1:123456789012:alias/MyAliasName</code> </p> </li> <li>
     * <p>Globally unique key ID example:
     * <code>12345678-1234-1234-1234-123456789012</code> </p> </li> <li> <p>Alias name
     * example: <code>alias/MyAliasName</code> </p> </li> <li> <p>Master key owned by
     * Kinesis: <code>alias/aws/kinesis</code> </p> </li> </ul>
     */
    inline StreamDescriptionSummary& WithKeyId(const char* value) { SetKeyId(value); return *this;}


    /**
     * <p>The number of open shards in the stream.</p>
     */
    inline int GetOpenShardCount() const{ return m_openShardCount; }

    /**
     * <p>The number of open shards in the stream.</p>
     */
    inline void SetOpenShardCount(int value) { m_openShardCountHasBeenSet = true; m_openShardCount = value; }

    /**
     * <p>The number of open shards in the stream.</p>
     */
    inline StreamDescriptionSummary& WithOpenShardCount(int value) { SetOpenShardCount(value); return *this;}

  private:

    Aws::String m_streamName;
    bool m_streamNameHasBeenSet;

    Aws::String m_streamARN;
    bool m_streamARNHasBeenSet;

    StreamStatus m_streamStatus;
    bool m_streamStatusHasBeenSet;

    int m_retentionPeriodHours;
    bool m_retentionPeriodHoursHasBeenSet;

    Aws::Utils::DateTime m_streamCreationTimestamp;
    bool m_streamCreationTimestampHasBeenSet;

    Aws::Vector<EnhancedMetrics> m_enhancedMonitoring;
    bool m_enhancedMonitoringHasBeenSet;

    EncryptionType m_encryptionType;
    bool m_encryptionTypeHasBeenSet;

    Aws::String m_keyId;
    bool m_keyIdHasBeenSet;

    int m_openShardCount;
    bool m_openShardCountHasBeenSet;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
