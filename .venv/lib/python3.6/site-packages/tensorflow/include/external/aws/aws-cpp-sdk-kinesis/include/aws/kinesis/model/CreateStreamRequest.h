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
   * <p>Represents the input for <code>CreateStream</code>.</p><p><h3>See Also:</h3> 
   * <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/CreateStreamInput">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API CreateStreamRequest : public KinesisRequest
  {
  public:
    CreateStreamRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "CreateStream"; }

    Aws::String SerializePayload() const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    /**
     * <p>A name to identify the stream. The stream name is scoped to the AWS account
     * used by the application that creates the stream. It is also scoped by region.
     * That is, two streams in two different AWS accounts can have the same name. Two
     * streams in the same AWS account but in two different regions can also have the
     * same name.</p>
     */
    inline const Aws::String& GetStreamName() const{ return m_streamName; }

    /**
     * <p>A name to identify the stream. The stream name is scoped to the AWS account
     * used by the application that creates the stream. It is also scoped by region.
     * That is, two streams in two different AWS accounts can have the same name. Two
     * streams in the same AWS account but in two different regions can also have the
     * same name.</p>
     */
    inline void SetStreamName(const Aws::String& value) { m_streamNameHasBeenSet = true; m_streamName = value; }

    /**
     * <p>A name to identify the stream. The stream name is scoped to the AWS account
     * used by the application that creates the stream. It is also scoped by region.
     * That is, two streams in two different AWS accounts can have the same name. Two
     * streams in the same AWS account but in two different regions can also have the
     * same name.</p>
     */
    inline void SetStreamName(Aws::String&& value) { m_streamNameHasBeenSet = true; m_streamName = std::move(value); }

    /**
     * <p>A name to identify the stream. The stream name is scoped to the AWS account
     * used by the application that creates the stream. It is also scoped by region.
     * That is, two streams in two different AWS accounts can have the same name. Two
     * streams in the same AWS account but in two different regions can also have the
     * same name.</p>
     */
    inline void SetStreamName(const char* value) { m_streamNameHasBeenSet = true; m_streamName.assign(value); }

    /**
     * <p>A name to identify the stream. The stream name is scoped to the AWS account
     * used by the application that creates the stream. It is also scoped by region.
     * That is, two streams in two different AWS accounts can have the same name. Two
     * streams in the same AWS account but in two different regions can also have the
     * same name.</p>
     */
    inline CreateStreamRequest& WithStreamName(const Aws::String& value) { SetStreamName(value); return *this;}

    /**
     * <p>A name to identify the stream. The stream name is scoped to the AWS account
     * used by the application that creates the stream. It is also scoped by region.
     * That is, two streams in two different AWS accounts can have the same name. Two
     * streams in the same AWS account but in two different regions can also have the
     * same name.</p>
     */
    inline CreateStreamRequest& WithStreamName(Aws::String&& value) { SetStreamName(std::move(value)); return *this;}

    /**
     * <p>A name to identify the stream. The stream name is scoped to the AWS account
     * used by the application that creates the stream. It is also scoped by region.
     * That is, two streams in two different AWS accounts can have the same name. Two
     * streams in the same AWS account but in two different regions can also have the
     * same name.</p>
     */
    inline CreateStreamRequest& WithStreamName(const char* value) { SetStreamName(value); return *this;}


    /**
     * <p>The number of shards that the stream will use. The throughput of the stream
     * is a function of the number of shards; more shards are required for greater
     * provisioned throughput.</p> <p>DefaultShardLimit;</p>
     */
    inline int GetShardCount() const{ return m_shardCount; }

    /**
     * <p>The number of shards that the stream will use. The throughput of the stream
     * is a function of the number of shards; more shards are required for greater
     * provisioned throughput.</p> <p>DefaultShardLimit;</p>
     */
    inline void SetShardCount(int value) { m_shardCountHasBeenSet = true; m_shardCount = value; }

    /**
     * <p>The number of shards that the stream will use. The throughput of the stream
     * is a function of the number of shards; more shards are required for greater
     * provisioned throughput.</p> <p>DefaultShardLimit;</p>
     */
    inline CreateStreamRequest& WithShardCount(int value) { SetShardCount(value); return *this;}

  private:

    Aws::String m_streamName;
    bool m_streamNameHasBeenSet;

    int m_shardCount;
    bool m_shardCountHasBeenSet;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
