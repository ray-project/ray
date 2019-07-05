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
#include <aws/s3/model/EncodingType.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/CommonPrefix.h>
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
  class AWS_S3_API ListObjectsResult
  {
  public:
    ListObjectsResult();
    ListObjectsResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    ListObjectsResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * A flag that indicates whether or not Amazon S3 returned all of the results that
     * satisfied the search criteria.
     */
    inline bool GetIsTruncated() const{ return m_isTruncated; }

    /**
     * A flag that indicates whether or not Amazon S3 returned all of the results that
     * satisfied the search criteria.
     */
    inline void SetIsTruncated(bool value) { m_isTruncated = value; }

    /**
     * A flag that indicates whether or not Amazon S3 returned all of the results that
     * satisfied the search criteria.
     */
    inline ListObjectsResult& WithIsTruncated(bool value) { SetIsTruncated(value); return *this;}


    
    inline const Aws::String& GetMarker() const{ return m_marker; }

    
    inline void SetMarker(const Aws::String& value) { m_marker = value; }

    
    inline void SetMarker(Aws::String&& value) { m_marker = std::move(value); }

    
    inline void SetMarker(const char* value) { m_marker.assign(value); }

    
    inline ListObjectsResult& WithMarker(const Aws::String& value) { SetMarker(value); return *this;}

    
    inline ListObjectsResult& WithMarker(Aws::String&& value) { SetMarker(std::move(value)); return *this;}

    
    inline ListObjectsResult& WithMarker(const char* value) { SetMarker(value); return *this;}


    /**
     * When response is truncated (the IsTruncated element value in the response is
     * true), you can use the key name in this field as marker in the subsequent
     * request to get next set of objects. Amazon S3 lists objects in alphabetical
     * order Note: This element is returned only if you have delimiter request
     * parameter specified. If response does not include the NextMaker and it is
     * truncated, you can use the value of the last Key in the response as the marker
     * in the subsequent request to get the next set of object keys.
     */
    inline const Aws::String& GetNextMarker() const{ return m_nextMarker; }

    /**
     * When response is truncated (the IsTruncated element value in the response is
     * true), you can use the key name in this field as marker in the subsequent
     * request to get next set of objects. Amazon S3 lists objects in alphabetical
     * order Note: This element is returned only if you have delimiter request
     * parameter specified. If response does not include the NextMaker and it is
     * truncated, you can use the value of the last Key in the response as the marker
     * in the subsequent request to get the next set of object keys.
     */
    inline void SetNextMarker(const Aws::String& value) { m_nextMarker = value; }

    /**
     * When response is truncated (the IsTruncated element value in the response is
     * true), you can use the key name in this field as marker in the subsequent
     * request to get next set of objects. Amazon S3 lists objects in alphabetical
     * order Note: This element is returned only if you have delimiter request
     * parameter specified. If response does not include the NextMaker and it is
     * truncated, you can use the value of the last Key in the response as the marker
     * in the subsequent request to get the next set of object keys.
     */
    inline void SetNextMarker(Aws::String&& value) { m_nextMarker = std::move(value); }

    /**
     * When response is truncated (the IsTruncated element value in the response is
     * true), you can use the key name in this field as marker in the subsequent
     * request to get next set of objects. Amazon S3 lists objects in alphabetical
     * order Note: This element is returned only if you have delimiter request
     * parameter specified. If response does not include the NextMaker and it is
     * truncated, you can use the value of the last Key in the response as the marker
     * in the subsequent request to get the next set of object keys.
     */
    inline void SetNextMarker(const char* value) { m_nextMarker.assign(value); }

    /**
     * When response is truncated (the IsTruncated element value in the response is
     * true), you can use the key name in this field as marker in the subsequent
     * request to get next set of objects. Amazon S3 lists objects in alphabetical
     * order Note: This element is returned only if you have delimiter request
     * parameter specified. If response does not include the NextMaker and it is
     * truncated, you can use the value of the last Key in the response as the marker
     * in the subsequent request to get the next set of object keys.
     */
    inline ListObjectsResult& WithNextMarker(const Aws::String& value) { SetNextMarker(value); return *this;}

    /**
     * When response is truncated (the IsTruncated element value in the response is
     * true), you can use the key name in this field as marker in the subsequent
     * request to get next set of objects. Amazon S3 lists objects in alphabetical
     * order Note: This element is returned only if you have delimiter request
     * parameter specified. If response does not include the NextMaker and it is
     * truncated, you can use the value of the last Key in the response as the marker
     * in the subsequent request to get the next set of object keys.
     */
    inline ListObjectsResult& WithNextMarker(Aws::String&& value) { SetNextMarker(std::move(value)); return *this;}

    /**
     * When response is truncated (the IsTruncated element value in the response is
     * true), you can use the key name in this field as marker in the subsequent
     * request to get next set of objects. Amazon S3 lists objects in alphabetical
     * order Note: This element is returned only if you have delimiter request
     * parameter specified. If response does not include the NextMaker and it is
     * truncated, you can use the value of the last Key in the response as the marker
     * in the subsequent request to get the next set of object keys.
     */
    inline ListObjectsResult& WithNextMarker(const char* value) { SetNextMarker(value); return *this;}


    
    inline const Aws::Vector<Object>& GetContents() const{ return m_contents; }

    
    inline void SetContents(const Aws::Vector<Object>& value) { m_contents = value; }

    
    inline void SetContents(Aws::Vector<Object>&& value) { m_contents = std::move(value); }

    
    inline ListObjectsResult& WithContents(const Aws::Vector<Object>& value) { SetContents(value); return *this;}

    
    inline ListObjectsResult& WithContents(Aws::Vector<Object>&& value) { SetContents(std::move(value)); return *this;}

    
    inline ListObjectsResult& AddContents(const Object& value) { m_contents.push_back(value); return *this; }

    
    inline ListObjectsResult& AddContents(Object&& value) { m_contents.push_back(std::move(value)); return *this; }


    
    inline const Aws::String& GetName() const{ return m_name; }

    
    inline void SetName(const Aws::String& value) { m_name = value; }

    
    inline void SetName(Aws::String&& value) { m_name = std::move(value); }

    
    inline void SetName(const char* value) { m_name.assign(value); }

    
    inline ListObjectsResult& WithName(const Aws::String& value) { SetName(value); return *this;}

    
    inline ListObjectsResult& WithName(Aws::String&& value) { SetName(std::move(value)); return *this;}

    
    inline ListObjectsResult& WithName(const char* value) { SetName(value); return *this;}


    
    inline const Aws::String& GetPrefix() const{ return m_prefix; }

    
    inline void SetPrefix(const Aws::String& value) { m_prefix = value; }

    
    inline void SetPrefix(Aws::String&& value) { m_prefix = std::move(value); }

    
    inline void SetPrefix(const char* value) { m_prefix.assign(value); }

    
    inline ListObjectsResult& WithPrefix(const Aws::String& value) { SetPrefix(value); return *this;}

    
    inline ListObjectsResult& WithPrefix(Aws::String&& value) { SetPrefix(std::move(value)); return *this;}

    
    inline ListObjectsResult& WithPrefix(const char* value) { SetPrefix(value); return *this;}


    
    inline const Aws::String& GetDelimiter() const{ return m_delimiter; }

    
    inline void SetDelimiter(const Aws::String& value) { m_delimiter = value; }

    
    inline void SetDelimiter(Aws::String&& value) { m_delimiter = std::move(value); }

    
    inline void SetDelimiter(const char* value) { m_delimiter.assign(value); }

    
    inline ListObjectsResult& WithDelimiter(const Aws::String& value) { SetDelimiter(value); return *this;}

    
    inline ListObjectsResult& WithDelimiter(Aws::String&& value) { SetDelimiter(std::move(value)); return *this;}

    
    inline ListObjectsResult& WithDelimiter(const char* value) { SetDelimiter(value); return *this;}


    
    inline int GetMaxKeys() const{ return m_maxKeys; }

    
    inline void SetMaxKeys(int value) { m_maxKeys = value; }

    
    inline ListObjectsResult& WithMaxKeys(int value) { SetMaxKeys(value); return *this;}


    
    inline const Aws::Vector<CommonPrefix>& GetCommonPrefixes() const{ return m_commonPrefixes; }

    
    inline void SetCommonPrefixes(const Aws::Vector<CommonPrefix>& value) { m_commonPrefixes = value; }

    
    inline void SetCommonPrefixes(Aws::Vector<CommonPrefix>&& value) { m_commonPrefixes = std::move(value); }

    
    inline ListObjectsResult& WithCommonPrefixes(const Aws::Vector<CommonPrefix>& value) { SetCommonPrefixes(value); return *this;}

    
    inline ListObjectsResult& WithCommonPrefixes(Aws::Vector<CommonPrefix>&& value) { SetCommonPrefixes(std::move(value)); return *this;}

    
    inline ListObjectsResult& AddCommonPrefixes(const CommonPrefix& value) { m_commonPrefixes.push_back(value); return *this; }

    
    inline ListObjectsResult& AddCommonPrefixes(CommonPrefix&& value) { m_commonPrefixes.push_back(std::move(value)); return *this; }


    /**
     * Encoding type used by Amazon S3 to encode object keys in the response.
     */
    inline const EncodingType& GetEncodingType() const{ return m_encodingType; }

    /**
     * Encoding type used by Amazon S3 to encode object keys in the response.
     */
    inline void SetEncodingType(const EncodingType& value) { m_encodingType = value; }

    /**
     * Encoding type used by Amazon S3 to encode object keys in the response.
     */
    inline void SetEncodingType(EncodingType&& value) { m_encodingType = std::move(value); }

    /**
     * Encoding type used by Amazon S3 to encode object keys in the response.
     */
    inline ListObjectsResult& WithEncodingType(const EncodingType& value) { SetEncodingType(value); return *this;}

    /**
     * Encoding type used by Amazon S3 to encode object keys in the response.
     */
    inline ListObjectsResult& WithEncodingType(EncodingType&& value) { SetEncodingType(std::move(value)); return *this;}

  private:

    bool m_isTruncated;

    Aws::String m_marker;

    Aws::String m_nextMarker;

    Aws::Vector<Object> m_contents;

    Aws::String m_name;

    Aws::String m_prefix;

    Aws::String m_delimiter;

    int m_maxKeys;

    Aws::Vector<CommonPrefix> m_commonPrefixes;

    EncodingType m_encodingType;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
