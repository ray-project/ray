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
   * <p>Represents the input for <code>ListStreams</code>.</p><p><h3>See Also:</h3>  
   * <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/ListStreamsInput">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API ListStreamsRequest : public KinesisRequest
  {
  public:
    ListStreamsRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "ListStreams"; }

    Aws::String SerializePayload() const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    /**
     * <p>The maximum number of streams to list.</p>
     */
    inline int GetLimit() const{ return m_limit; }

    /**
     * <p>The maximum number of streams to list.</p>
     */
    inline void SetLimit(int value) { m_limitHasBeenSet = true; m_limit = value; }

    /**
     * <p>The maximum number of streams to list.</p>
     */
    inline ListStreamsRequest& WithLimit(int value) { SetLimit(value); return *this;}


    /**
     * <p>The name of the stream to start the list with.</p>
     */
    inline const Aws::String& GetExclusiveStartStreamName() const{ return m_exclusiveStartStreamName; }

    /**
     * <p>The name of the stream to start the list with.</p>
     */
    inline void SetExclusiveStartStreamName(const Aws::String& value) { m_exclusiveStartStreamNameHasBeenSet = true; m_exclusiveStartStreamName = value; }

    /**
     * <p>The name of the stream to start the list with.</p>
     */
    inline void SetExclusiveStartStreamName(Aws::String&& value) { m_exclusiveStartStreamNameHasBeenSet = true; m_exclusiveStartStreamName = std::move(value); }

    /**
     * <p>The name of the stream to start the list with.</p>
     */
    inline void SetExclusiveStartStreamName(const char* value) { m_exclusiveStartStreamNameHasBeenSet = true; m_exclusiveStartStreamName.assign(value); }

    /**
     * <p>The name of the stream to start the list with.</p>
     */
    inline ListStreamsRequest& WithExclusiveStartStreamName(const Aws::String& value) { SetExclusiveStartStreamName(value); return *this;}

    /**
     * <p>The name of the stream to start the list with.</p>
     */
    inline ListStreamsRequest& WithExclusiveStartStreamName(Aws::String&& value) { SetExclusiveStartStreamName(std::move(value)); return *this;}

    /**
     * <p>The name of the stream to start the list with.</p>
     */
    inline ListStreamsRequest& WithExclusiveStartStreamName(const char* value) { SetExclusiveStartStreamName(value); return *this;}

  private:

    int m_limit;
    bool m_limitHasBeenSet;

    Aws::String m_exclusiveStartStreamName;
    bool m_exclusiveStartStreamNameHasBeenSet;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
