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
  class AWS_S3_API ListMultipartUploadsRequest : public S3Request
  {
  public:
    ListMultipartUploadsRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "ListMultipartUploads"; }

    Aws::String SerializePayload() const override;

    void AddQueryStringParameters(Aws::Http::URI& uri) const override;


    
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    
    inline ListMultipartUploadsRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    
    inline ListMultipartUploadsRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    
    inline ListMultipartUploadsRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * Character you use to group keys.
     */
    inline const Aws::String& GetDelimiter() const{ return m_delimiter; }

    /**
     * Character you use to group keys.
     */
    inline void SetDelimiter(const Aws::String& value) { m_delimiterHasBeenSet = true; m_delimiter = value; }

    /**
     * Character you use to group keys.
     */
    inline void SetDelimiter(Aws::String&& value) { m_delimiterHasBeenSet = true; m_delimiter = std::move(value); }

    /**
     * Character you use to group keys.
     */
    inline void SetDelimiter(const char* value) { m_delimiterHasBeenSet = true; m_delimiter.assign(value); }

    /**
     * Character you use to group keys.
     */
    inline ListMultipartUploadsRequest& WithDelimiter(const Aws::String& value) { SetDelimiter(value); return *this;}

    /**
     * Character you use to group keys.
     */
    inline ListMultipartUploadsRequest& WithDelimiter(Aws::String&& value) { SetDelimiter(std::move(value)); return *this;}

    /**
     * Character you use to group keys.
     */
    inline ListMultipartUploadsRequest& WithDelimiter(const char* value) { SetDelimiter(value); return *this;}


    
    inline const EncodingType& GetEncodingType() const{ return m_encodingType; }

    
    inline void SetEncodingType(const EncodingType& value) { m_encodingTypeHasBeenSet = true; m_encodingType = value; }

    
    inline void SetEncodingType(EncodingType&& value) { m_encodingTypeHasBeenSet = true; m_encodingType = std::move(value); }

    
    inline ListMultipartUploadsRequest& WithEncodingType(const EncodingType& value) { SetEncodingType(value); return *this;}

    
    inline ListMultipartUploadsRequest& WithEncodingType(EncodingType&& value) { SetEncodingType(std::move(value)); return *this;}


    /**
     * Together with upload-id-marker, this parameter specifies the multipart upload
     * after which listing should begin.
     */
    inline const Aws::String& GetKeyMarker() const{ return m_keyMarker; }

    /**
     * Together with upload-id-marker, this parameter specifies the multipart upload
     * after which listing should begin.
     */
    inline void SetKeyMarker(const Aws::String& value) { m_keyMarkerHasBeenSet = true; m_keyMarker = value; }

    /**
     * Together with upload-id-marker, this parameter specifies the multipart upload
     * after which listing should begin.
     */
    inline void SetKeyMarker(Aws::String&& value) { m_keyMarkerHasBeenSet = true; m_keyMarker = std::move(value); }

    /**
     * Together with upload-id-marker, this parameter specifies the multipart upload
     * after which listing should begin.
     */
    inline void SetKeyMarker(const char* value) { m_keyMarkerHasBeenSet = true; m_keyMarker.assign(value); }

    /**
     * Together with upload-id-marker, this parameter specifies the multipart upload
     * after which listing should begin.
     */
    inline ListMultipartUploadsRequest& WithKeyMarker(const Aws::String& value) { SetKeyMarker(value); return *this;}

    /**
     * Together with upload-id-marker, this parameter specifies the multipart upload
     * after which listing should begin.
     */
    inline ListMultipartUploadsRequest& WithKeyMarker(Aws::String&& value) { SetKeyMarker(std::move(value)); return *this;}

    /**
     * Together with upload-id-marker, this parameter specifies the multipart upload
     * after which listing should begin.
     */
    inline ListMultipartUploadsRequest& WithKeyMarker(const char* value) { SetKeyMarker(value); return *this;}


    /**
     * Sets the maximum number of multipart uploads, from 1 to 1,000, to return in the
     * response body. 1,000 is the maximum number of uploads that can be returned in a
     * response.
     */
    inline int GetMaxUploads() const{ return m_maxUploads; }

    /**
     * Sets the maximum number of multipart uploads, from 1 to 1,000, to return in the
     * response body. 1,000 is the maximum number of uploads that can be returned in a
     * response.
     */
    inline void SetMaxUploads(int value) { m_maxUploadsHasBeenSet = true; m_maxUploads = value; }

    /**
     * Sets the maximum number of multipart uploads, from 1 to 1,000, to return in the
     * response body. 1,000 is the maximum number of uploads that can be returned in a
     * response.
     */
    inline ListMultipartUploadsRequest& WithMaxUploads(int value) { SetMaxUploads(value); return *this;}


    /**
     * Lists in-progress uploads only for those keys that begin with the specified
     * prefix.
     */
    inline const Aws::String& GetPrefix() const{ return m_prefix; }

    /**
     * Lists in-progress uploads only for those keys that begin with the specified
     * prefix.
     */
    inline void SetPrefix(const Aws::String& value) { m_prefixHasBeenSet = true; m_prefix = value; }

    /**
     * Lists in-progress uploads only for those keys that begin with the specified
     * prefix.
     */
    inline void SetPrefix(Aws::String&& value) { m_prefixHasBeenSet = true; m_prefix = std::move(value); }

    /**
     * Lists in-progress uploads only for those keys that begin with the specified
     * prefix.
     */
    inline void SetPrefix(const char* value) { m_prefixHasBeenSet = true; m_prefix.assign(value); }

    /**
     * Lists in-progress uploads only for those keys that begin with the specified
     * prefix.
     */
    inline ListMultipartUploadsRequest& WithPrefix(const Aws::String& value) { SetPrefix(value); return *this;}

    /**
     * Lists in-progress uploads only for those keys that begin with the specified
     * prefix.
     */
    inline ListMultipartUploadsRequest& WithPrefix(Aws::String&& value) { SetPrefix(std::move(value)); return *this;}

    /**
     * Lists in-progress uploads only for those keys that begin with the specified
     * prefix.
     */
    inline ListMultipartUploadsRequest& WithPrefix(const char* value) { SetPrefix(value); return *this;}


    /**
     * Together with key-marker, specifies the multipart upload after which listing
     * should begin. If key-marker is not specified, the upload-id-marker parameter is
     * ignored.
     */
    inline const Aws::String& GetUploadIdMarker() const{ return m_uploadIdMarker; }

    /**
     * Together with key-marker, specifies the multipart upload after which listing
     * should begin. If key-marker is not specified, the upload-id-marker parameter is
     * ignored.
     */
    inline void SetUploadIdMarker(const Aws::String& value) { m_uploadIdMarkerHasBeenSet = true; m_uploadIdMarker = value; }

    /**
     * Together with key-marker, specifies the multipart upload after which listing
     * should begin. If key-marker is not specified, the upload-id-marker parameter is
     * ignored.
     */
    inline void SetUploadIdMarker(Aws::String&& value) { m_uploadIdMarkerHasBeenSet = true; m_uploadIdMarker = std::move(value); }

    /**
     * Together with key-marker, specifies the multipart upload after which listing
     * should begin. If key-marker is not specified, the upload-id-marker parameter is
     * ignored.
     */
    inline void SetUploadIdMarker(const char* value) { m_uploadIdMarkerHasBeenSet = true; m_uploadIdMarker.assign(value); }

    /**
     * Together with key-marker, specifies the multipart upload after which listing
     * should begin. If key-marker is not specified, the upload-id-marker parameter is
     * ignored.
     */
    inline ListMultipartUploadsRequest& WithUploadIdMarker(const Aws::String& value) { SetUploadIdMarker(value); return *this;}

    /**
     * Together with key-marker, specifies the multipart upload after which listing
     * should begin. If key-marker is not specified, the upload-id-marker parameter is
     * ignored.
     */
    inline ListMultipartUploadsRequest& WithUploadIdMarker(Aws::String&& value) { SetUploadIdMarker(std::move(value)); return *this;}

    /**
     * Together with key-marker, specifies the multipart upload after which listing
     * should begin. If key-marker is not specified, the upload-id-marker parameter is
     * ignored.
     */
    inline ListMultipartUploadsRequest& WithUploadIdMarker(const char* value) { SetUploadIdMarker(value); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_delimiter;
    bool m_delimiterHasBeenSet;

    EncodingType m_encodingType;
    bool m_encodingTypeHasBeenSet;

    Aws::String m_keyMarker;
    bool m_keyMarkerHasBeenSet;

    int m_maxUploads;
    bool m_maxUploadsHasBeenSet;

    Aws::String m_prefix;
    bool m_prefixHasBeenSet;

    Aws::String m_uploadIdMarker;
    bool m_uploadIdMarkerHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
