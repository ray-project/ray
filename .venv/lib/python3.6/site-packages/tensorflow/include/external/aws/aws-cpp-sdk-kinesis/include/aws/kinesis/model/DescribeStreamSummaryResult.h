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
#include <aws/kinesis/model/StreamDescriptionSummary.h>
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
  class AWS_KINESIS_API DescribeStreamSummaryResult
  {
  public:
    DescribeStreamSummaryResult();
    DescribeStreamSummaryResult(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);
    DescribeStreamSummaryResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);


    /**
     * <p>A <a>StreamDescriptionSummary</a> containing information about the
     * stream.</p>
     */
    inline const StreamDescriptionSummary& GetStreamDescriptionSummary() const{ return m_streamDescriptionSummary; }

    /**
     * <p>A <a>StreamDescriptionSummary</a> containing information about the
     * stream.</p>
     */
    inline void SetStreamDescriptionSummary(const StreamDescriptionSummary& value) { m_streamDescriptionSummary = value; }

    /**
     * <p>A <a>StreamDescriptionSummary</a> containing information about the
     * stream.</p>
     */
    inline void SetStreamDescriptionSummary(StreamDescriptionSummary&& value) { m_streamDescriptionSummary = std::move(value); }

    /**
     * <p>A <a>StreamDescriptionSummary</a> containing information about the
     * stream.</p>
     */
    inline DescribeStreamSummaryResult& WithStreamDescriptionSummary(const StreamDescriptionSummary& value) { SetStreamDescriptionSummary(value); return *this;}

    /**
     * <p>A <a>StreamDescriptionSummary</a> containing information about the
     * stream.</p>
     */
    inline DescribeStreamSummaryResult& WithStreamDescriptionSummary(StreamDescriptionSummary&& value) { SetStreamDescriptionSummary(std::move(value)); return *this;}

  private:

    StreamDescriptionSummary m_streamDescriptionSummary;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
