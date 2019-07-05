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
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/core/utils/memory/stl/AWSString.h>
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
  class AWS_S3_API ListObjectsV2Result
  {
  public:
    ListObjectsV2Result();
    ListObjectsV2Result(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    ListObjectsV2Result& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


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
    inline ListObjectsV2Result& WithIsTruncated(bool value) { SetIsTruncated(value); return *this;}


    /**
     * Metadata about each object returned.
     */
    inline const Aws::Vector<Object>& GetContents() const{ return m_contents; }

    /**
     * Metadata about each object returned.
     */
    inline void SetContents(const Aws::Vector<Object>& value) { m_contents = value; }

    /**
     * Metadata about each object returned.
     */
    inline void SetContents(Aws::Vector<Object>&& value) { m_contents = std::move(value); }

    /**
     * Metadata about each object returned.
     */
    inline ListObjectsV2Result& WithContents(const Aws::Vector<Object>& value) { SetContents(value); return *this;}

    /**
     * Metadata about each object returned.
     */
    inline ListObjectsV2Result& WithContents(Aws::Vector<Object>&& value) { SetContents(std::move(value)); return *this;}

    /**
     * Metadata about each object returned.
     */
    inline ListObjectsV2Result& AddContents(const Object& value) { m_contents.push_back(value); return *this; }

    /**
     * Metadata about each object returned.
     */
    inline ListObjectsV2Result& AddContents(Object&& value) { m_contents.push_back(std::move(value)); return *this; }


    /**
     * Name of the bucket to list.
     */
    inline const Aws::String& GetName() const{ return m_name; }

    /**
     * Name of the bucket to list.
     */
    inline void SetName(const Aws::String& value) { m_name = value; }

    /**
     * Name of the bucket to list.
     */
    inline void SetName(Aws::String&& value) { m_name = std::move(value); }

    /**
     * Name of the bucket to list.
     */
    inline void SetName(const char* value) { m_name.assign(value); }

    /**
     * Name of the bucket to list.
     */
    inline ListObjectsV2Result& WithName(const Aws::String& value) { SetName(value); return *this;}

    /**
     * Name of the bucket to list.
     */
    inline ListObjectsV2Result& WithName(Aws::String&& value) { SetName(std::move(value)); return *this;}

    /**
     * Name of the bucket to list.
     */
    inline ListObjectsV2Result& WithName(const char* value) { SetName(value); return *this;}


    /**
     * Limits the response to keys that begin with the specified prefix.
     */
    inline const Aws::String& GetPrefix() const{ return m_prefix; }

    /**
     * Limits the response to keys that begin with the specified prefix.
     */
    inline void SetPrefix(const Aws::String& value) { m_prefix = value; }

    /**
     * Limits the response to keys that begin with the specified prefix.
     */
    inline void SetPrefix(Aws::String&& value) { m_prefix = std::move(value); }

    /**
     * Limits the response to keys that begin with the specified prefix.
     */
    inline void SetPrefix(const char* value) { m_prefix.assign(value); }

    /**
     * Limits the response to keys that begin with the specified prefix.
     */
    inline ListObjectsV2Result& WithPrefix(const Aws::String& value) { SetPrefix(value); return *this;}

    /**
     * Limits the response to keys that begin with the specified prefix.
     */
    inline ListObjectsV2Result& WithPrefix(Aws::String&& value) { SetPrefix(std::move(value)); return *this;}

    /**
     * Limits the response to keys that begin with the specified prefix.
     */
    inline ListObjectsV2Result& WithPrefix(const char* value) { SetPrefix(value); return *this;}


    /**
     * A delimiter is a character you use to group keys.
     */
    inline const Aws::String& GetDelimiter() const{ return m_delimiter; }

    /**
     * A delimiter is a character you use to group keys.
     */
    inline void SetDelimiter(const Aws::String& value) { m_delimiter = value; }

    /**
     * A delimiter is a character you use to group keys.
     */
    inline void SetDelimiter(Aws::String&& value) { m_delimiter = std::move(value); }

    /**
     * A delimiter is a character you use to group keys.
     */
    inline void SetDelimiter(const char* value) { m_delimiter.assign(value); }

    /**
     * A delimiter is a character you use to group keys.
     */
    inline ListObjectsV2Result& WithDelimiter(const Aws::String& value) { SetDelimiter(value); return *this;}

    /**
     * A delimiter is a character you use to group keys.
     */
    inline ListObjectsV2Result& WithDelimiter(Aws::String&& value) { SetDelimiter(std::move(value)); return *this;}

    /**
     * A delimiter is a character you use to group keys.
     */
    inline ListObjectsV2Result& WithDelimiter(const char* value) { SetDelimiter(value); return *this;}


    /**
     * Sets the maximum number of keys returned in the response. The response might
     * contain fewer keys but will never contain more.
     */
    inline int GetMaxKeys() const{ return m_maxKeys; }

    /**
     * Sets the maximum number of keys returned in the response. The response might
     * contain fewer keys but will never contain more.
     */
    inline void SetMaxKeys(int value) { m_maxKeys = value; }

    /**
     * Sets the maximum number of keys returned in the response. The response might
     * contain fewer keys but will never contain more.
     */
    inline ListObjectsV2Result& WithMaxKeys(int value) { SetMaxKeys(value); return *this;}


    /**
     * CommonPrefixes contains all (if there are any) keys between Prefix and the next
     * occurrence of the string specified by delimiter
     */
    inline const Aws::Vector<CommonPrefix>& GetCommonPrefixes() const{ return m_commonPrefixes; }

    /**
     * CommonPrefixes contains all (if there are any) keys between Prefix and the next
     * occurrence of the string specified by delimiter
     */
    inline void SetCommonPrefixes(const Aws::Vector<CommonPrefix>& value) { m_commonPrefixes = value; }

    /**
     * CommonPrefixes contains all (if there are any) keys between Prefix and the next
     * occurrence of the string specified by delimiter
     */
    inline void SetCommonPrefixes(Aws::Vector<CommonPrefix>&& value) { m_commonPrefixes = std::move(value); }

    /**
     * CommonPrefixes contains all (if there are any) keys between Prefix and the next
     * occurrence of the string specified by delimiter
     */
    inline ListObjectsV2Result& WithCommonPrefixes(const Aws::Vector<CommonPrefix>& value) { SetCommonPrefixes(value); return *this;}

    /**
     * CommonPrefixes contains all (if there are any) keys between Prefix and the next
     * occurrence of the string specified by delimiter
     */
    inline ListObjectsV2Result& WithCommonPrefixes(Aws::Vector<CommonPrefix>&& value) { SetCommonPrefixes(std::move(value)); return *this;}

    /**
     * CommonPrefixes contains all (if there are any) keys between Prefix and the next
     * occurrence of the string specified by delimiter
     */
    inline ListObjectsV2Result& AddCommonPrefixes(const CommonPrefix& value) { m_commonPrefixes.push_back(value); return *this; }

    /**
     * CommonPrefixes contains all (if there are any) keys between Prefix and the next
     * occurrence of the string specified by delimiter
     */
    inline ListObjectsV2Result& AddCommonPrefixes(CommonPrefix&& value) { m_commonPrefixes.push_back(std::move(value)); return *this; }


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
    inline ListObjectsV2Result& WithEncodingType(const EncodingType& value) { SetEncodingType(value); return *this;}

    /**
     * Encoding type used by Amazon S3 to encode object keys in the response.
     */
    inline ListObjectsV2Result& WithEncodingType(EncodingType&& value) { SetEncodingType(std::move(value)); return *this;}


    /**
     * KeyCount is the number of keys returned with this request. KeyCount will always
     * be less than equals to MaxKeys field. Say you ask for 50 keys, your result will
     * include less than equals 50 keys
     */
    inline int GetKeyCount() const{ return m_keyCount; }

    /**
     * KeyCount is the number of keys returned with this request. KeyCount will always
     * be less than equals to MaxKeys field. Say you ask for 50 keys, your result will
     * include less than equals 50 keys
     */
    inline void SetKeyCount(int value) { m_keyCount = value; }

    /**
     * KeyCount is the number of keys returned with this request. KeyCount will always
     * be less than equals to MaxKeys field. Say you ask for 50 keys, your result will
     * include less than equals 50 keys
     */
    inline ListObjectsV2Result& WithKeyCount(int value) { SetKeyCount(value); return *this;}


    /**
     * ContinuationToken indicates Amazon S3 that the list is being continued on this
     * bucket with a token. ContinuationToken is obfuscated and is not a real key
     */
    inline const Aws::String& GetContinuationToken() const{ return m_continuationToken; }

    /**
     * ContinuationToken indicates Amazon S3 that the list is being continued on this
     * bucket with a token. ContinuationToken is obfuscated and is not a real key
     */
    inline void SetContinuationToken(const Aws::String& value) { m_continuationToken = value; }

    /**
     * ContinuationToken indicates Amazon S3 that the list is being continued on this
     * bucket with a token. ContinuationToken is obfuscated and is not a real key
     */
    inline void SetContinuationToken(Aws::String&& value) { m_continuationToken = std::move(value); }

    /**
     * ContinuationToken indicates Amazon S3 that the list is being continued on this
     * bucket with a token. ContinuationToken is obfuscated and is not a real key
     */
    inline void SetContinuationToken(const char* value) { m_continuationToken.assign(value); }

    /**
     * ContinuationToken indicates Amazon S3 that the list is being continued on this
     * bucket with a token. ContinuationToken is obfuscated and is not a real key
     */
    inline ListObjectsV2Result& WithContinuationToken(const Aws::String& value) { SetContinuationToken(value); return *this;}

    /**
     * ContinuationToken indicates Amazon S3 that the list is being continued on this
     * bucket with a token. ContinuationToken is obfuscated and is not a real key
     */
    inline ListObjectsV2Result& WithContinuationToken(Aws::String&& value) { SetContinuationToken(std::move(value)); return *this;}

    /**
     * ContinuationToken indicates Amazon S3 that the list is being continued on this
     * bucket with a token. ContinuationToken is obfuscated and is not a real key
     */
    inline ListObjectsV2Result& WithContinuationToken(const char* value) { SetContinuationToken(value); return *this;}


    /**
     * NextContinuationToken is sent when isTruncated is true which means there are
     * more keys in the bucket that can be listed. The next list requests to Amazon S3
     * can be continued with this NextContinuationToken. NextContinuationToken is
     * obfuscated and is not a real key
     */
    inline const Aws::String& GetNextContinuationToken() const{ return m_nextContinuationToken; }

    /**
     * NextContinuationToken is sent when isTruncated is true which means there are
     * more keys in the bucket that can be listed. The next list requests to Amazon S3
     * can be continued with this NextContinuationToken. NextContinuationToken is
     * obfuscated and is not a real key
     */
    inline void SetNextContinuationToken(const Aws::String& value) { m_nextContinuationToken = value; }

    /**
     * NextContinuationToken is sent when isTruncated is true which means there are
     * more keys in the bucket that can be listed. The next list requests to Amazon S3
     * can be continued with this NextContinuationToken. NextContinuationToken is
     * obfuscated and is not a real key
     */
    inline void SetNextContinuationToken(Aws::String&& value) { m_nextContinuationToken = std::move(value); }

    /**
     * NextContinuationToken is sent when isTruncated is true which means there are
     * more keys in the bucket that can be listed. The next list requests to Amazon S3
     * can be continued with this NextContinuationToken. NextContinuationToken is
     * obfuscated and is not a real key
     */
    inline void SetNextContinuationToken(const char* value) { m_nextContinuationToken.assign(value); }

    /**
     * NextContinuationToken is sent when isTruncated is true which means there are
     * more keys in the bucket that can be listed. The next list requests to Amazon S3
     * can be continued with this NextContinuationToken. NextContinuationToken is
     * obfuscated and is not a real key
     */
    inline ListObjectsV2Result& WithNextContinuationToken(const Aws::String& value) { SetNextContinuationToken(value); return *this;}

    /**
     * NextContinuationToken is sent when isTruncated is true which means there are
     * more keys in the bucket that can be listed. The next list requests to Amazon S3
     * can be continued with this NextContinuationToken. NextContinuationToken is
     * obfuscated and is not a real key
     */
    inline ListObjectsV2Result& WithNextContinuationToken(Aws::String&& value) { SetNextContinuationToken(std::move(value)); return *this;}

    /**
     * NextContinuationToken is sent when isTruncated is true which means there are
     * more keys in the bucket that can be listed. The next list requests to Amazon S3
     * can be continued with this NextContinuationToken. NextContinuationToken is
     * obfuscated and is not a real key
     */
    inline ListObjectsV2Result& WithNextContinuationToken(const char* value) { SetNextContinuationToken(value); return *this;}


    /**
     * StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. StartAfter can be any key in the bucket
     */
    inline const Aws::String& GetStartAfter() const{ return m_startAfter; }

    /**
     * StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. StartAfter can be any key in the bucket
     */
    inline void SetStartAfter(const Aws::String& value) { m_startAfter = value; }

    /**
     * StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. StartAfter can be any key in the bucket
     */
    inline void SetStartAfter(Aws::String&& value) { m_startAfter = std::move(value); }

    /**
     * StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. StartAfter can be any key in the bucket
     */
    inline void SetStartAfter(const char* value) { m_startAfter.assign(value); }

    /**
     * StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. StartAfter can be any key in the bucket
     */
    inline ListObjectsV2Result& WithStartAfter(const Aws::String& value) { SetStartAfter(value); return *this;}

    /**
     * StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. StartAfter can be any key in the bucket
     */
    inline ListObjectsV2Result& WithStartAfter(Aws::String&& value) { SetStartAfter(std::move(value)); return *this;}

    /**
     * StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. StartAfter can be any key in the bucket
     */
    inline ListObjectsV2Result& WithStartAfter(const char* value) { SetStartAfter(value); return *this;}

  private:

    bool m_isTruncated;

    Aws::Vector<Object> m_contents;

    Aws::String m_name;

    Aws::String m_prefix;

    Aws::String m_delimiter;

    int m_maxKeys;

    Aws::Vector<CommonPrefix> m_commonPrefixes;

    EncodingType m_encodingType;

    int m_keyCount;

    Aws::String m_continuationToken;

    Aws::String m_nextContinuationToken;

    Aws::String m_startAfter;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
