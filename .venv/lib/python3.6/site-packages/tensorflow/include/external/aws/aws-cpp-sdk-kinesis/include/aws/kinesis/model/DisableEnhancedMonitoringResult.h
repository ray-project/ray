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
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/kinesis/model/MetricsName.h>
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
   * <p>Represents the output for <a>EnableEnhancedMonitoring</a> and
   * <a>DisableEnhancedMonitoring</a>.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/EnhancedMonitoringOutput">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API DisableEnhancedMonitoringResult
  {
  public:
    DisableEnhancedMonitoringResult();
    DisableEnhancedMonitoringResult(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);
    DisableEnhancedMonitoringResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);


    /**
     * <p>The name of the Kinesis stream.</p>
     */
    inline const Aws::String& GetStreamName() const{ return m_streamName; }

    /**
     * <p>The name of the Kinesis stream.</p>
     */
    inline void SetStreamName(const Aws::String& value) { m_streamName = value; }

    /**
     * <p>The name of the Kinesis stream.</p>
     */
    inline void SetStreamName(Aws::String&& value) { m_streamName = std::move(value); }

    /**
     * <p>The name of the Kinesis stream.</p>
     */
    inline void SetStreamName(const char* value) { m_streamName.assign(value); }

    /**
     * <p>The name of the Kinesis stream.</p>
     */
    inline DisableEnhancedMonitoringResult& WithStreamName(const Aws::String& value) { SetStreamName(value); return *this;}

    /**
     * <p>The name of the Kinesis stream.</p>
     */
    inline DisableEnhancedMonitoringResult& WithStreamName(Aws::String&& value) { SetStreamName(std::move(value)); return *this;}

    /**
     * <p>The name of the Kinesis stream.</p>
     */
    inline DisableEnhancedMonitoringResult& WithStreamName(const char* value) { SetStreamName(value); return *this;}


    /**
     * <p>Represents the current state of the metrics that are in the enhanced state
     * before the operation.</p>
     */
    inline const Aws::Vector<MetricsName>& GetCurrentShardLevelMetrics() const{ return m_currentShardLevelMetrics; }

    /**
     * <p>Represents the current state of the metrics that are in the enhanced state
     * before the operation.</p>
     */
    inline void SetCurrentShardLevelMetrics(const Aws::Vector<MetricsName>& value) { m_currentShardLevelMetrics = value; }

    /**
     * <p>Represents the current state of the metrics that are in the enhanced state
     * before the operation.</p>
     */
    inline void SetCurrentShardLevelMetrics(Aws::Vector<MetricsName>&& value) { m_currentShardLevelMetrics = std::move(value); }

    /**
     * <p>Represents the current state of the metrics that are in the enhanced state
     * before the operation.</p>
     */
    inline DisableEnhancedMonitoringResult& WithCurrentShardLevelMetrics(const Aws::Vector<MetricsName>& value) { SetCurrentShardLevelMetrics(value); return *this;}

    /**
     * <p>Represents the current state of the metrics that are in the enhanced state
     * before the operation.</p>
     */
    inline DisableEnhancedMonitoringResult& WithCurrentShardLevelMetrics(Aws::Vector<MetricsName>&& value) { SetCurrentShardLevelMetrics(std::move(value)); return *this;}

    /**
     * <p>Represents the current state of the metrics that are in the enhanced state
     * before the operation.</p>
     */
    inline DisableEnhancedMonitoringResult& AddCurrentShardLevelMetrics(const MetricsName& value) { m_currentShardLevelMetrics.push_back(value); return *this; }

    /**
     * <p>Represents the current state of the metrics that are in the enhanced state
     * before the operation.</p>
     */
    inline DisableEnhancedMonitoringResult& AddCurrentShardLevelMetrics(MetricsName&& value) { m_currentShardLevelMetrics.push_back(std::move(value)); return *this; }


    /**
     * <p>Represents the list of all the metrics that would be in the enhanced state
     * after the operation.</p>
     */
    inline const Aws::Vector<MetricsName>& GetDesiredShardLevelMetrics() const{ return m_desiredShardLevelMetrics; }

    /**
     * <p>Represents the list of all the metrics that would be in the enhanced state
     * after the operation.</p>
     */
    inline void SetDesiredShardLevelMetrics(const Aws::Vector<MetricsName>& value) { m_desiredShardLevelMetrics = value; }

    /**
     * <p>Represents the list of all the metrics that would be in the enhanced state
     * after the operation.</p>
     */
    inline void SetDesiredShardLevelMetrics(Aws::Vector<MetricsName>&& value) { m_desiredShardLevelMetrics = std::move(value); }

    /**
     * <p>Represents the list of all the metrics that would be in the enhanced state
     * after the operation.</p>
     */
    inline DisableEnhancedMonitoringResult& WithDesiredShardLevelMetrics(const Aws::Vector<MetricsName>& value) { SetDesiredShardLevelMetrics(value); return *this;}

    /**
     * <p>Represents the list of all the metrics that would be in the enhanced state
     * after the operation.</p>
     */
    inline DisableEnhancedMonitoringResult& WithDesiredShardLevelMetrics(Aws::Vector<MetricsName>&& value) { SetDesiredShardLevelMetrics(std::move(value)); return *this;}

    /**
     * <p>Represents the list of all the metrics that would be in the enhanced state
     * after the operation.</p>
     */
    inline DisableEnhancedMonitoringResult& AddDesiredShardLevelMetrics(const MetricsName& value) { m_desiredShardLevelMetrics.push_back(value); return *this; }

    /**
     * <p>Represents the list of all the metrics that would be in the enhanced state
     * after the operation.</p>
     */
    inline DisableEnhancedMonitoringResult& AddDesiredShardLevelMetrics(MetricsName&& value) { m_desiredShardLevelMetrics.push_back(std::move(value)); return *this; }

  private:

    Aws::String m_streamName;

    Aws::Vector<MetricsName> m_currentShardLevelMetrics;

    Aws::Vector<MetricsName> m_desiredShardLevelMetrics;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
