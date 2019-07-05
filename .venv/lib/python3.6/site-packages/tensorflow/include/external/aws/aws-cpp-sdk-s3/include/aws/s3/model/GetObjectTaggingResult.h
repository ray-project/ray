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
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/Tag.h>
#include <utility>

namespace Aws
{
template<typename RESULT_TYPE>
class AmazonWebServiceResult;

namespace Utils
{
namespace Xml
{
  class XmlDocument;
} // namespace Xml
} // namespace Utils
namespace S3
{
namespace Model
{
  class AWS_S3_API GetObjectTaggingResult
  {
  public:
    GetObjectTaggingResult();
    GetObjectTaggingResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    GetObjectTaggingResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    
    inline void SetVersionId(const Aws::String& value) { m_versionId = value; }

    
    inline void SetVersionId(Aws::String&& value) { m_versionId = std::move(value); }

    
    inline void SetVersionId(const char* value) { m_versionId.assign(value); }

    
    inline GetObjectTaggingResult& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    
    inline GetObjectTaggingResult& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    
    inline GetObjectTaggingResult& WithVersionId(const char* value) { SetVersionId(value); return *this;}


    
    inline const Aws::Vector<Tag>& GetTagSet() const{ return m_tagSet; }

    
    inline void SetTagSet(const Aws::Vector<Tag>& value) { m_tagSet = value; }

    
    inline void SetTagSet(Aws::Vector<Tag>&& value) { m_tagSet = std::move(value); }

    
    inline GetObjectTaggingResult& WithTagSet(const Aws::Vector<Tag>& value) { SetTagSet(value); return *this;}

    
    inline GetObjectTaggingResult& WithTagSet(Aws::Vector<Tag>&& value) { SetTagSet(std::move(value)); return *this;}

    
    inline GetObjectTaggingResult& AddTagSet(const Tag& value) { m_tagSet.push_back(value); return *this; }

    
    inline GetObjectTaggingResult& AddTagSet(Tag&& value) { m_tagSet.push_back(std::move(value)); return *this; }

  private:

    Aws::String m_versionId;

    Aws::Vector<Tag> m_tagSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
