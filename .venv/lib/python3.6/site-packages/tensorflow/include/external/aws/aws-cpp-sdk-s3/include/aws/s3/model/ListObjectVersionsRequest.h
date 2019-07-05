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
  class AWS_S3_API ListObjectVersionsRequest : public S3Request
  {
  public:
    ListObjectVersionsRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "ListObjectVersions"; }

    Aws::String SerializePayload() const override;

    void AddQueryStringParameters(Aws::Http::URI& uri) const override;


    
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    
    inline ListObjectVersionsRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    
    inline ListObjectVersionsRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    
    inline ListObjectVersionsRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


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
    inline ListObjectVersionsRequest& WithDelimiter(const Aws::String& value) { SetDelimiter(value); return *this;}

    /**
     * A delimiter is a character you use to group keys.
     */
    inline ListObjectVersionsRequest& WithDelimiter(Aws::String&& value) { SetDelimiter(std::move(value)); return *this;}

    /**
     * A delimiter is a character you use to group keys.
     */
    inline ListObjectVersionsRequest& WithDelimiter(const char* value) { SetDelimiter(value); return *this;}


    
    inline const EncodingType& GetEncodingType() const{ return m_encodingType; }

    
    inline void SetEncodingType(const EncodingType& value) { m_encodingTypeHasBeenSet = true; m_encodingType = value; }

    
    inline void SetEncodingType(EncodingType&& value) { m_encodingTypeHasBeenSet = true; m_encodingType = std::move(value); }

    
    inline ListObjectVersionsRequest& WithEncodingType(const EncodingType& value) { SetEncodingType(value); return *this;}

    
    inline ListObjectVersionsRequest& WithEncodingType(EncodingType&& value) { SetEncodingType(std::move(value)); return *this;}


    /**
     * Specifies the key to start with when listing objects in a bucket.
     */
    inline const Aws::String& GetKeyMarker() const{ return m_keyMarker; }

    /**
     * Specifies the key to start with when listing objects in a bucket.
     */
    inline void SetKeyMarker(const Aws::String& value) { m_keyMarkerHasBeenSet = true; m_keyMarker = value; }

    /**
     * Specifies the key to start with when listing objects in a bucket.
     */
    inline void SetKeyMarker(Aws::String&& value) { m_keyMarkerHasBeenSet = true; m_keyMarker = std::move(value); }

    /**
     * Specifies the key to start with when listing objects in a bucket.
     */
    inline void SetKeyMarker(const char* value) { m_keyMarkerHasBeenSet = true; m_keyMarker.assign(value); }

    /**
     * Specifies the key to start with when listing objects in a bucket.
     */
    inline ListObjectVersionsRequest& WithKeyMarker(const Aws::String& value) { SetKeyMarker(value); return *this;}

    /**
     * Specifies the key to start with when listing objects in a bucket.
     */
    inline ListObjectVersionsRequest& WithKeyMarker(Aws::String&& value) { SetKeyMarker(std::move(value)); return *this;}

    /**
     * Specifies the key to start with when listing objects in a bucket.
     */
    inline ListObjectVersionsRequest& WithKeyMarker(const char* value) { SetKeyMarker(value); return *this;}


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
    inline ListObjectVersionsRequest& WithMaxKeys(int value) { SetMaxKeys(value); return *this;}


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
    inline ListObjectVersionsRequest& WithPrefix(const Aws::String& value) { SetPrefix(value); return *this;}

    /**
     * Limits the response to keys that begin with the specified prefix.
     */
    inline ListObjectVersionsRequest& WithPrefix(Aws::String&& value) { SetPrefix(std::move(value)); return *this;}

    /**
     * Limits the response to keys that begin with the specified prefix.
     */
    inline ListObjectVersionsRequest& WithPrefix(const char* value) { SetPrefix(value); return *this;}


    /**
     * Specifies the object version you want to start listing from.
     */
    inline const Aws::String& GetVersionIdMarker() const{ return m_versionIdMarker; }

    /**
     * Specifies the object version you want to start listing from.
     */
    inline void SetVersionIdMarker(const Aws::String& value) { m_versionIdMarkerHasBeenSet = true; m_versionIdMarker = value; }

    /**
     * Specifies the object version you want to start listing from.
     */
    inline void SetVersionIdMarker(Aws::String&& value) { m_versionIdMarkerHasBeenSet = true; m_versionIdMarker = std::move(value); }

    /**
     * Specifies the object version you want to start listing from.
     */
    inline void SetVersionIdMarker(const char* value) { m_versionIdMarkerHasBeenSet = true; m_versionIdMarker.assign(value); }

    /**
     * Specifies the object version you want to start listing from.
     */
    inline ListObjectVersionsRequest& WithVersionIdMarker(const Aws::String& value) { SetVersionIdMarker(value); return *this;}

    /**
     * Specifies the object version you want to start listing from.
     */
    inline ListObjectVersionsRequest& WithVersionIdMarker(Aws::String&& value) { SetVersionIdMarker(std::move(value)); return *this;}

    /**
     * Specifies the object version you want to start listing from.
     */
    inline ListObjectVersionsRequest& WithVersionIdMarker(const char* value) { SetVersionIdMarker(value); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_delimiter;
    bool m_delimiterHasBeenSet;

    EncodingType m_encodingType;
    bool m_encodingTypeHasBeenSet;

    Aws::String m_keyMarker;
    bool m_keyMarkerHasBeenSet;

    int m_maxKeys;
    bool m_maxKeysHasBeenSet;

    Aws::String m_prefix;
    bool m_prefixHasBeenSet;

    Aws::String m_versionIdMarker;
    bool m_versionIdMarkerHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
