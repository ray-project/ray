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
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/kinesis/model/MetricsName.h>
#include <utility>

namespace Aws
{
namespace Kinesis
{
namespace Model
{

  /**
   * <p>Represents the input for <a>DisableEnhancedMonitoring</a>.</p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DisableEnhancedMonitoringInput">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API DisableEnhancedMonitoringRequest : public KinesisRequest
  {
  public:
    DisableEnhancedMonitoringRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "DisableEnhancedMonitoring"; }

    Aws::String SerializePayload() const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    /**
     * <p>The name of the Kinesis stream for which to disable enhanced monitoring.</p>
     */
    inline const Aws::String& GetStreamName() const{ return m_streamName; }

    /**
     * <p>The name of the Kinesis stream for which to disable enhanced monitoring.</p>
     */
    inline void SetStreamName(const Aws::String& value) { m_streamNameHasBeenSet = true; m_streamName = value; }

    /**
     * <p>The name of the Kinesis stream for which to disable enhanced monitoring.</p>
     */
    inline void SetStreamName(Aws::String&& value) { m_streamNameHasBeenSet = true; m_streamName = std::move(value); }

    /**
     * <p>The name of the Kinesis stream for which to disable enhanced monitoring.</p>
     */
    inline void SetStreamName(const char* value) { m_streamNameHasBeenSet = true; m_streamName.assign(value); }

    /**
     * <p>The name of the Kinesis stream for which to disable enhanced monitoring.</p>
     */
    inline DisableEnhancedMonitoringRequest& WithStreamName(const Aws::String& value) { SetStreamName(value); return *this;}

    /**
     * <p>The name of the Kinesis stream for which to disable enhanced monitoring.</p>
     */
    inline DisableEnhancedMonitoringRequest& WithStreamName(Aws::String&& value) { SetStreamName(std::move(value)); return *this;}

    /**
     * <p>The name of the Kinesis stream for which to disable enhanced monitoring.</p>
     */
    inline DisableEnhancedMonitoringRequest& WithStreamName(const char* value) { SetStreamName(value); return *this;}


    /**
     * <p>List of shard-level metrics to disable.</p> <p>The following are the valid
     * shard-level metrics. The value "<code>ALL</code>" disables every metric.</p>
     * <ul> <li> <p> <code>IncomingBytes</code> </p> </li> <li> <p>
     * <code>IncomingRecords</code> </p> </li> <li> <p> <code>OutgoingBytes</code> </p>
     * </li> <li> <p> <code>OutgoingRecords</code> </p> </li> <li> <p>
     * <code>WriteProvisionedThroughputExceeded</code> </p> </li> <li> <p>
     * <code>ReadProvisionedThroughputExceeded</code> </p> </li> <li> <p>
     * <code>IteratorAgeMilliseconds</code> </p> </li> <li> <p> <code>ALL</code> </p>
     * </li> </ul> <p>For more information, see <a
     * href="http://docs.aws.amazon.com/kinesis/latest/dev/monitoring-with-cloudwatch.html">Monitoring
     * the Amazon Kinesis Streams Service with Amazon CloudWatch</a> in the <i>Amazon
     * Kinesis Streams Developer Guide</i>.</p>
     */
    inline const Aws::Vector<MetricsName>& GetShardLevelMetrics() const{ return m_shardLevelMetrics; }

    /**
     * <p>List of shard-level metrics to disable.</p> <p>The following are the valid
     * shard-level metrics. The value "<code>ALL</code>" disables every metric.</p>
     * <ul> <li> <p> <code>IncomingBytes</code> </p> </li> <li> <p>
     * <code>IncomingRecords</code> </p> </li> <li> <p> <code>OutgoingBytes</code> </p>
     * </li> <li> <p> <code>OutgoingRecords</code> </p> </li> <li> <p>
     * <code>WriteProvisionedThroughputExceeded</code> </p> </li> <li> <p>
     * <code>ReadProvisionedThroughputExceeded</code> </p> </li> <li> <p>
     * <code>IteratorAgeMilliseconds</code> </p> </li> <li> <p> <code>ALL</code> </p>
     * </li> </ul> <p>For more information, see <a
     * href="http://docs.aws.amazon.com/kinesis/latest/dev/monitoring-with-cloudwatch.html">Monitoring
     * the Amazon Kinesis Streams Service with Amazon CloudWatch</a> in the <i>Amazon
     * Kinesis Streams Developer Guide</i>.</p>
     */
    inline void SetShardLevelMetrics(const Aws::Vector<MetricsName>& value) { m_shardLevelMetricsHasBeenSet = true; m_shardLevelMetrics = value; }

    /**
     * <p>List of shard-level metrics to disable.</p> <p>The following are the valid
     * shard-level metrics. The value "<code>ALL</code>" disables every metric.</p>
     * <ul> <li> <p> <code>IncomingBytes</code> </p> </li> <li> <p>
     * <code>IncomingRecords</code> </p> </li> <li> <p> <code>OutgoingBytes</code> </p>
     * </li> <li> <p> <code>OutgoingRecords</code> </p> </li> <li> <p>
     * <code>WriteProvisionedThroughputExceeded</code> </p> </li> <li> <p>
     * <code>ReadProvisionedThroughputExceeded</code> </p> </li> <li> <p>
     * <code>IteratorAgeMilliseconds</code> </p> </li> <li> <p> <code>ALL</code> </p>
     * </li> </ul> <p>For more information, see <a
     * href="http://docs.aws.amazon.com/kinesis/latest/dev/monitoring-with-cloudwatch.html">Monitoring
     * the Amazon Kinesis Streams Service with Amazon CloudWatch</a> in the <i>Amazon
     * Kinesis Streams Developer Guide</i>.</p>
     */
    inline void SetShardLevelMetrics(Aws::Vector<MetricsName>&& value) { m_shardLevelMetricsHasBeenSet = true; m_shardLevelMetrics = std::move(value); }

    /**
     * <p>List of shard-level metrics to disable.</p> <p>The following are the valid
     * shard-level metrics. The value "<code>ALL</code>" disables every metric.</p>
     * <ul> <li> <p> <code>IncomingBytes</code> </p> </li> <li> <p>
     * <code>IncomingRecords</code> </p> </li> <li> <p> <code>OutgoingBytes</code> </p>
     * </li> <li> <p> <code>OutgoingRecords</code> </p> </li> <li> <p>
     * <code>WriteProvisionedThroughputExceeded</code> </p> </li> <li> <p>
     * <code>ReadProvisionedThroughputExceeded</code> </p> </li> <li> <p>
     * <code>IteratorAgeMilliseconds</code> </p> </li> <li> <p> <code>ALL</code> </p>
     * </li> </ul> <p>For more information, see <a
     * href="http://docs.aws.amazon.com/kinesis/latest/dev/monitoring-with-cloudwatch.html">Monitoring
     * the Amazon Kinesis Streams Service with Amazon CloudWatch</a> in the <i>Amazon
     * Kinesis Streams Developer Guide</i>.</p>
     */
    inline DisableEnhancedMonitoringRequest& WithShardLevelMetrics(const Aws::Vector<MetricsName>& value) { SetShardLevelMetrics(value); return *this;}

    /**
     * <p>List of shard-level metrics to disable.</p> <p>The following are the valid
     * shard-level metrics. The value "<code>ALL</code>" disables every metric.</p>
     * <ul> <li> <p> <code>IncomingBytes</code> </p> </li> <li> <p>
     * <code>IncomingRecords</code> </p> </li> <li> <p> <code>OutgoingBytes</code> </p>
     * </li> <li> <p> <code>OutgoingRecords</code> </p> </li> <li> <p>
     * <code>WriteProvisionedThroughputExceeded</code> </p> </li> <li> <p>
     * <code>ReadProvisionedThroughputExceeded</code> </p> </li> <li> <p>
     * <code>IteratorAgeMilliseconds</code> </p> </li> <li> <p> <code>ALL</code> </p>
     * </li> </ul> <p>For more information, see <a
     * href="http://docs.aws.amazon.com/kinesis/latest/dev/monitoring-with-cloudwatch.html">Monitoring
     * the Amazon Kinesis Streams Service with Amazon CloudWatch</a> in the <i>Amazon
     * Kinesis Streams Developer Guide</i>.</p>
     */
    inline DisableEnhancedMonitoringRequest& WithShardLevelMetrics(Aws::Vector<MetricsName>&& value) { SetShardLevelMetrics(std::move(value)); return *this;}

    /**
     * <p>List of shard-level metrics to disable.</p> <p>The following are the valid
     * shard-level metrics. The value "<code>ALL</code>" disables every metric.</p>
     * <ul> <li> <p> <code>IncomingBytes</code> </p> </li> <li> <p>
     * <code>IncomingRecords</code> </p> </li> <li> <p> <code>OutgoingBytes</code> </p>
     * </li> <li> <p> <code>OutgoingRecords</code> </p> </li> <li> <p>
     * <code>WriteProvisionedThroughputExceeded</code> </p> </li> <li> <p>
     * <code>ReadProvisionedThroughputExceeded</code> </p> </li> <li> <p>
     * <code>IteratorAgeMilliseconds</code> </p> </li> <li> <p> <code>ALL</code> </p>
     * </li> </ul> <p>For more information, see <a
     * href="http://docs.aws.amazon.com/kinesis/latest/dev/monitoring-with-cloudwatch.html">Monitoring
     * the Amazon Kinesis Streams Service with Amazon CloudWatch</a> in the <i>Amazon
     * Kinesis Streams Developer Guide</i>.</p>
     */
    inline DisableEnhancedMonitoringRequest& AddShardLevelMetrics(const MetricsName& value) { m_shardLevelMetricsHasBeenSet = true; m_shardLevelMetrics.push_back(value); return *this; }

    /**
     * <p>List of shard-level metrics to disable.</p> <p>The following are the valid
     * shard-level metrics. The value "<code>ALL</code>" disables every metric.</p>
     * <ul> <li> <p> <code>IncomingBytes</code> </p> </li> <li> <p>
     * <code>IncomingRecords</code> </p> </li> <li> <p> <code>OutgoingBytes</code> </p>
     * </li> <li> <p> <code>OutgoingRecords</code> </p> </li> <li> <p>
     * <code>WriteProvisionedThroughputExceeded</code> </p> </li> <li> <p>
     * <code>ReadProvisionedThroughputExceeded</code> </p> </li> <li> <p>
     * <code>IteratorAgeMilliseconds</code> </p> </li> <li> <p> <code>ALL</code> </p>
     * </li> </ul> <p>For more information, see <a
     * href="http://docs.aws.amazon.com/kinesis/latest/dev/monitoring-with-cloudwatch.html">Monitoring
     * the Amazon Kinesis Streams Service with Amazon CloudWatch</a> in the <i>Amazon
     * Kinesis Streams Developer Guide</i>.</p>
     */
    inline DisableEnhancedMonitoringRequest& AddShardLevelMetrics(MetricsName&& value) { m_shardLevelMetricsHasBeenSet = true; m_shardLevelMetrics.push_back(std::move(value)); return *this; }

  private:

    Aws::String m_streamName;
    bool m_streamNameHasBeenSet;

    Aws::Vector<MetricsName> m_shardLevelMetrics;
    bool m_shardLevelMetricsHasBeenSet;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
