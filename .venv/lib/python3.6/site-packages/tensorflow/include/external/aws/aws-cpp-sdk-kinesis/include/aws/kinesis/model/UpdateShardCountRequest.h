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
#include <aws/kinesis/model/ScalingType.h>
#include <utility>

namespace Aws
{
namespace Kinesis
{
namespace Model
{

  /**
   */
  class AWS_KINESIS_API UpdateShardCountRequest : public KinesisRequest
  {
  public:
    UpdateShardCountRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "UpdateShardCount"; }

    Aws::String SerializePayload() const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    /**
     * <p>The name of the stream.</p>
     */
    inline const Aws::String& GetStreamName() const{ return m_streamName; }

    /**
     * <p>The name of the stream.</p>
     */
    inline void SetStreamName(const Aws::String& value) { m_streamNameHasBeenSet = true; m_streamName = value; }

    /**
     * <p>The name of the stream.</p>
     */
    inline void SetStreamName(Aws::String&& value) { m_streamNameHasBeenSet = true; m_streamName = std::move(value); }

    /**
     * <p>The name of the stream.</p>
     */
    inline void SetStreamName(const char* value) { m_streamNameHasBeenSet = true; m_streamName.assign(value); }

    /**
     * <p>The name of the stream.</p>
     */
    inline UpdateShardCountRequest& WithStreamName(const Aws::String& value) { SetStreamName(value); return *this;}

    /**
     * <p>The name of the stream.</p>
     */
    inline UpdateShardCountRequest& WithStreamName(Aws::String&& value) { SetStreamName(std::move(value)); return *this;}

    /**
     * <p>The name of the stream.</p>
     */
    inline UpdateShardCountRequest& WithStreamName(const char* value) { SetStreamName(value); return *this;}


    /**
     * <p>The new number of shards.</p>
     */
    inline int GetTargetShardCount() const{ return m_targetShardCount; }

    /**
     * <p>The new number of shards.</p>
     */
    inline void SetTargetShardCount(int value) { m_targetShardCountHasBeenSet = true; m_targetShardCount = value; }

    /**
     * <p>The new number of shards.</p>
     */
    inline UpdateShardCountRequest& WithTargetShardCount(int value) { SetTargetShardCount(value); return *this;}


    /**
     * <p>The scaling type. Uniform scaling creates shards of equal size.</p>
     */
    inline const ScalingType& GetScalingType() const{ return m_scalingType; }

    /**
     * <p>The scaling type. Uniform scaling creates shards of equal size.</p>
     */
    inline void SetScalingType(const ScalingType& value) { m_scalingTypeHasBeenSet = true; m_scalingType = value; }

    /**
     * <p>The scaling type. Uniform scaling creates shards of equal size.</p>
     */
    inline void SetScalingType(ScalingType&& value) { m_scalingTypeHasBeenSet = true; m_scalingType = std::move(value); }

    /**
     * <p>The scaling type. Uniform scaling creates shards of equal size.</p>
     */
    inline UpdateShardCountRequest& WithScalingType(const ScalingType& value) { SetScalingType(value); return *this;}

    /**
     * <p>The scaling type. Uniform scaling creates shards of equal size.</p>
     */
    inline UpdateShardCountRequest& WithScalingType(ScalingType&& value) { SetScalingType(std::move(value)); return *this;}

  private:

    Aws::String m_streamName;
    bool m_streamNameHasBeenSet;

    int m_targetShardCount;
    bool m_targetShardCountHasBeenSet;

    ScalingType m_scalingType;
    bool m_scalingTypeHasBeenSet;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
