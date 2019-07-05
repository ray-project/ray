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
#include <aws/s3/S3Request.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/EncodingType.h>
#include <aws/s3/model/RequestPayer.h>
#include <utility>

namespace Aws
{
namespace Http
{
    class URI;
} //namespace Http
namespace S3
{
namespace Model
{

  /**
   */
  class AWS_S3_API ListObjectsV2Request : public S3Request
  {
  public:
    ListObjectsV2Request();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "ListObjectsV2"; }

    Aws::String SerializePayload() const override;

    void AddQueryStringParameters(Aws::Http::URI& uri) const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    /**
     * Name of the bucket to list.
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * Name of the bucket to list.
     */
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /**
     * Name of the bucket to list.
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /**
     * Name of the bucket to list.
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /**
     * Name of the bucket to list.
     */
    inline ListObjectsV2Request& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * Name of the bucket to list.
     */
    inline ListObjectsV2Request& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * Name of the bucket to list.
     */
    inline ListObjectsV2Request& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * A delimiter is a character you use to group keys.
     */
    inline const Aws::String& GetDelimiter() const{ return m_delimiter; }

    /**
     * A delimiter is a character you use to group keys.
     */
    inline void SetDelimiter(const Aws::String& value) { m_delimiterHasBeenSet = true; m_delimiter = value; }

    /**
     * A delimiter is a character you use to group keys.
     */
    inline void SetDelimiter(Aws::String&& value) { m_delimiterHasBeenSet = true; m_delimiter = std::move(value); }

    /**
     * A delimiter is a character you use to group keys.
     */
    inline void SetDelimiter(const char* value) { m_delimiterHasBeenSet = true; m_delimiter.assign(value); }

    /**
     * A delimiter is a character you use to group keys.
     */
    inline ListObjectsV2Request& WithDelimiter(const Aws::String& value) { SetDelimiter(value); return *this;}

    /**
     * A delimiter is a character you use to group keys.
     */
    inline ListObjectsV2Request& WithDelimiter(Aws::String&& value) { SetDelimiter(std::move(value)); return *this;}

    /**
     * A delimiter is a character you use to group keys.
     */
    inline ListObjectsV2Request& WithDelimiter(const char* value) { SetDelimiter(value); return *this;}


    /**
     * Encoding type used by Amazon S3 to encode object keys in the response.
     */
    inline const EncodingType& GetEncodingType() const{ return m_encodingType; }

    /**
     * Encoding type used by Amazon S3 to encode object keys in the response.
     */
    inline void SetEncodingType(const EncodingType& value) { m_encodingTypeHasBeenSet = true; m_encodingType = value; }

    /**
     * Encoding type used by Amazon S3 to encode object keys in the response.
     */
    inline void SetEncodingType(EncodingType&& value) { m_encodingTypeHasBeenSet = true; m_encodingType = std::move(value); }

    /**
     * Encoding type used by Amazon S3 to encode object keys in the response.
     */
    inline ListObjectsV2Request& WithEncodingType(const EncodingType& value) { SetEncodingType(value); return *this;}

    /**
     * Encoding type used by Amazon S3 to encode object keys in the response.
     */
    inline ListObjectsV2Request& WithEncodingType(EncodingType&& value) { SetEncodingType(std::move(value)); return *this;}


    /**
     * Sets the maximum number of keys returned in the response. The response might
     * contain fewer keys but will never contain more.
     */
    inline int GetMaxKeys() const{ return m_maxKeys; }

    /**
     * Sets the maximum number of keys returned in the response. The response might
     * contain fewer keys but will never contain more.
     */
    inline void SetMaxKeys(int value) { m_maxKeysHasBeenSet = true; m_maxKeys = value; }

    /**
     * Sets the maximum number of keys returned in the response. The response might
     * contain fewer keys but will never contain more.
     */
    inline ListObjectsV2Request& WithMaxKeys(int value) { SetMaxKeys(value); return *this;}


    /**
     * Limits the response to keys that begin with the specified prefix.
     */
    inline const Aws::String& GetPrefix() const{ return m_prefix; }

    /**
     * Limits the response to keys that begin with the specified prefix.
     */
    inline void SetPrefix(const Aws::String& value) { m_prefixHasBeenSet = true; m_prefix = value; }

    /**
     * Limits the response to keys that begin with the specified prefix.
     */
    inline void SetPrefix(Aws::String&& value) { m_prefixHasBeenSet = true; m_prefix = std::move(value); }

    /**
     * Limits the response to keys that begin with the specified prefix.
     */
    inline void SetPrefix(const char* value) { m_prefixHasBeenSet = true; m_prefix.assign(value); }

    /**
     * Limits the response to keys that begin with the specified prefix.
     */
    inline ListObjectsV2Request& WithPrefix(const Aws::String& value) { SetPrefix(value); return *this;}

    /**
     * Limits the response to keys that begin with the specified prefix.
     */
    inline ListObjectsV2Request& WithPrefix(Aws::String&& value) { SetPrefix(std::move(value)); return *this;}

    /**
     * Limits the response to keys that begin with the specified prefix.
     */
    inline ListObjectsV2Request& WithPrefix(const char* value) { SetPrefix(value); return *this;}


    /**
     * ContinuationToken indicates Amazon S3 that the list is being continued on this
     * bucket with a token. ContinuationToken is obfuscated and is not a real key
     */
    inline const Aws::String& GetContinuationToken() const{ return m_continuationToken; }

    /**
     * ContinuationToken indicates Amazon S3 that the list is being continued on this
     * bucket with a token. ContinuationToken is obfuscated and is not a real key
     */
    inline void SetContinuationToken(const Aws::String& value) { m_continuationTokenHasBeenSet = true; m_continuationToken = value; }

    /**
     * ContinuationToken indicates Amazon S3 that the list is being continued on this
     * bucket with a token. ContinuationToken is obfuscated and is not a real key
     */
    inline void SetContinuationToken(Aws::String&& value) { m_continuationTokenHasBeenSet = true; m_continuationToken = std::move(value); }

    /**
     * ContinuationToken indicates Amazon S3 that the list is being continued on this
     * bucket with a token. ContinuationToken is obfuscated and is not a real key
     */
    inline void SetContinuationToken(const char* value) { m_continuationTokenHasBeenSet = true; m_continuationToken.assign(value); }

    /**
     * ContinuationToken indicates Amazon S3 that the list is being continued on this
     * bucket with a token. ContinuationToken is obfuscated and is not a real key
     */
    inline ListObjectsV2Request& WithContinuationToken(const Aws::String& value) { SetContinuationToken(value); return *this;}

    /**
     * ContinuationToken indicates Amazon S3 that the list is being continued on this
     * bucket with a token. ContinuationToken is obfuscated and is not a real key
     */
    inline ListObjectsV2Request& WithContinuationToken(Aws::String&& value) { SetContinuationToken(std::move(value)); return *this;}

    /**
     * ContinuationToken indicates Amazon S3 that the list is being continued on this
     * bucket with a token. ContinuationToken is obfuscated and is not a real key
     */
    inline ListObjectsV2Request& WithContinuationToken(const char* value) { SetContinuationToken(value); return *this;}


    /**
     * The owner field is not present in listV2 by default, if you want to return owner
     * field with each key in the result then set the fetch owner field to true
     */
    inline bool GetFetchOwner() const{ return m_fetchOwner; }

    /**
     * The owner field is not present in listV2 by default, if you want to return owner
     * field with each key in the result then set the fetch owner field to true
     */
    inline void SetFetchOwner(bool value) { m_fetchOwnerHasBeenSet = true; m_fetchOwner = value; }

    /**
     * The owner field is not present in listV2 by default, if you want to return owner
     * field with each key in the result then set the fetch owner field to true
     */
    inline ListObjectsV2Request& WithFetchOwner(bool value) { SetFetchOwner(value); return *this;}


    /**
     * StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. StartAfter can be any key in the bucket
     */
    inline const Aws::String& GetStartAfter() const{ return m_startAfter; }

    /**
     * StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. StartAfter can be any key in the bucket
     */
    inline void SetStartAfter(const Aws::String& value) { m_startAfterHasBeenSet = true; m_startAfter = value; }

    /**
     * StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. StartAfter can be any key in the bucket
     */
    inline void SetStartAfter(Aws::String&& value) { m_startAfterHasBeenSet = true; m_startAfter = std::move(value); }

    /**
     * StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. StartAfter can be any key in the bucket
     */
    inline void SetStartAfter(const char* value) { m_startAfterHasBeenSet = true; m_startAfter.assign(value); }

    /**
     * StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. StartAfter can be any key in the bucket
     */
    inline ListObjectsV2Request& WithStartAfter(const Aws::String& value) { SetStartAfter(value); return *this;}

    /**
     * StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. StartAfter can be any key in the bucket
     */
    inline ListObjectsV2Request& WithStartAfter(Aws::String&& value) { SetStartAfter(std::move(value)); return *this;}

    /**
     * StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts
     * listing after this specified key. StartAfter can be any key in the bucket
     */
    inline ListObjectsV2Request& WithStartAfter(const char* value) { SetStartAfter(value); return *this;}


    /**
     * Confirms that the requester knows that she or he will be charged for the list
     * objects request in V2 style. Bucket owners need not specify this parameter in
     * their requests.
     */
    inline const RequestPayer& GetRequestPayer() const{ return m_requestPayer; }

    /**
     * Confirms that the requester knows that she or he will be charged for the list
     * objects request in V2 style. Bucket owners need not specify this parameter in
     * their requests.
     */
    inline void SetRequestPayer(const RequestPayer& value) { m_requestPayerHasBeenSet = true; m_requestPayer = value; }

    /**
     * Confirms that the requester knows that she or he will be charged for the list
     * objects request in V2 style. Bucket owners need not specify this parameter in
     * their requests.
     */
    inline void SetRequestPayer(RequestPayer&& value) { m_requestPayerHasBeenSet = true; m_requestPayer = std::move(value); }

    /**
     * Confirms that the requester knows that she or he will be charged for the list
     * objects request in V2 style. Bucket owners need not specify this parameter in
     * their requests.
     */
    inline ListObjectsV2Request& WithRequestPayer(const RequestPayer& value) { SetRequestPayer(value); return *this;}

    /**
     * Confirms that the requester knows that she or he will be charged for the list
     * objects request in V2 style. Bucket owners need not specify this parameter in
     * their requests.
     */
    inline ListObjectsV2Request& WithRequestPayer(RequestPayer&& value) { SetRequestPayer(std::move(value)); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_delimiter;
    bool m_delimiterHasBeenSet;

    EncodingType m_encodingType;
    bool m_encodingTypeHasBeenSet;

    int m_maxKeys;
    bool m_maxKeysHasBeenSet;

    Aws::String m_prefix;
    bool m_prefixHasBeenSet;

    Aws::String m_continuationToken;
    bool m_continuationTokenHasBeenSet;

    bool m_fetchOwner;
    bool m_fetchOwnerHasBeenSet;

    Aws::String m_startAfter;
    bool m_startAfterHasBeenSet;

    RequestPayer m_requestPayer;
    bool m_requestPayerHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
