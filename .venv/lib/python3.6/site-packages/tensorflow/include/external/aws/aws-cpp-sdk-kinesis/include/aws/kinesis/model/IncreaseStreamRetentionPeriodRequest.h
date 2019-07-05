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
#include <utility>

namespace Aws
{
namespace Kinesis
{
namespace Model
{

  /**
   * <p>Represents the input for <a>IncreaseStreamRetentionPeriod</a>.</p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/IncreaseStreamRetentionPeriodInput">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API IncreaseStreamRetentionPeriodRequest : public KinesisRequest
  {
  public:
    IncreaseStreamRetentionPeriodRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "IncreaseStreamRetentionPeriod"; }

    Aws::String SerializePayload() const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    /**
     * <p>The name of the stream to modify.</p>
     */
    inline const Aws::String& GetStreamName() const{ return m_streamName; }

    /**
     * <p>The name of the stream to modify.</p>
     */
    inline void SetStreamName(const Aws::String& value) { m_streamNameHasBeenSet = true; m_streamName = value; }

    /**
     * <p>The name of the stream to modify.</p>
     */
    inline void SetStreamName(Aws::String&& value) { m_streamNameHasBeenSet = true; m_streamName = std::move(value); }

    /**
     * <p>The name of the stream to modify.</p>
     */
    inline void SetStreamName(const char* value) { m_streamNameHasBeenSet = true; m_streamName.assign(value); }

    /**
     * <p>The name of the stream to modify.</p>
     */
    inline IncreaseStreamRetentionPeriodRequest& WithStreamName(const Aws::String& value) { SetStreamName(value); return *this;}

    /**
     * <p>The name of the stream to modify.</p>
     */
    inline IncreaseStreamRetentionPeriodRequest& WithStreamName(Aws::String&& value) { SetStreamName(std::move(value)); return *this;}

    /**
     * <p>The name of the stream to modify.</p>
     */
    inline IncreaseStreamRetentionPeriodRequest& WithStreamName(const char* value) { SetStreamName(value); return *this;}


    /**
     * <p>The new retention period of the stream, in hours. Must be more than the
     * current retention period.</p>
     */
    inline int GetRetentionPeriodHours() const{ return m_retentionPeriodHours; }

    /**
     * <p>The new retention period of the stream, in hours. Must be more than the
     * current retention period.</p>
     */
    inline void SetRetentionPeriodHours(int value) { m_retentionPeriodHoursHasBeenSet = true; m_retentionPeriodHours = value; }

    /**
     * <p>The new retention period of the stream, in hours. Must be more than the
     * current retention period.</p>
     */
    inline IncreaseStreamRetentionPeriodRequest& WithRetentionPeriodHours(int value) { SetRetentionPeriodHours(value); return *this;}

  private:

    Aws::String m_streamName;
    bool m_streamNameHasBeenSet;

    int m_retentionPeriodHours;
    bool m_retentionPeriodHoursHasBeenSet;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
