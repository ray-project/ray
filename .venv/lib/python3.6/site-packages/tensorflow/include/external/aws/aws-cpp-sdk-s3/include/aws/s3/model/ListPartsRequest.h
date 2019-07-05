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
  class AWS_S3_API ListPartsRequest : public S3Request
  {
  public:
    ListPartsRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "ListParts"; }

    Aws::String SerializePayload() const override;

    void AddQueryStringParameters(Aws::Http::URI& uri) const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    
    inline ListPartsRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    
    inline ListPartsRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    
    inline ListPartsRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    
    inline const Aws::String& GetKey() const{ return m_key; }

    
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    
    inline ListPartsRequest& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    
    inline ListPartsRequest& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    
    inline ListPartsRequest& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * Sets the maximum number of parts to return.
     */
    inline int GetMaxParts() const{ return m_maxParts; }

    /**
     * Sets the maximum number of parts to return.
     */
    inline void SetMaxParts(int value) { m_maxPartsHasBeenSet = true; m_maxParts = value; }

    /**
     * Sets the maximum number of parts to return.
     */
    inline ListPartsRequest& WithMaxParts(int value) { SetMaxParts(value); return *this;}


    /**
     * Specifies the part after which listing should begin. Only parts with higher part
     * numbers will be listed.
     */
    inline int GetPartNumberMarker() const{ return m_partNumberMarker; }

    /**
     * Specifies the part after which listing should begin. Only parts with higher part
     * numbers will be listed.
     */
    inline void SetPartNumberMarker(int value) { m_partNumberMarkerHasBeenSet = true; m_partNumberMarker = value; }

    /**
     * Specifies the part after which listing should begin. Only parts with higher part
     * numbers will be listed.
     */
    inline ListPartsRequest& WithPartNumberMarker(int value) { SetPartNumberMarker(value); return *this;}


    /**
     * Upload ID identifying the multipart upload whose parts are being listed.
     */
    inline const Aws::String& GetUploadId() const{ return m_uploadId; }

    /**
     * Upload ID identifying the multipart upload whose parts are being listed.
     */
    inline void SetUploadId(const Aws::String& value) { m_uploadIdHasBeenSet = true; m_uploadId = value; }

    /**
     * Upload ID identifying the multipart upload whose parts are being listed.
     */
    inline void SetUploadId(Aws::String&& value) { m_uploadIdHasBeenSet = true; m_uploadId = std::move(value); }

    /**
     * Upload ID identifying the multipart upload whose parts are being listed.
     */
    inline void SetUploadId(const char* value) { m_uploadIdHasBeenSet = true; m_uploadId.assign(value); }

    /**
     * Upload ID identifying the multipart upload whose parts are being listed.
     */
    inline ListPartsRequest& WithUploadId(const Aws::String& value) { SetUploadId(value); return *this;}

    /**
     * Upload ID identifying the multipart upload whose parts are being listed.
     */
    inline ListPartsRequest& WithUploadId(Aws::String&& value) { SetUploadId(std::move(value)); return *this;}

    /**
     * Upload ID identifying the multipart upload whose parts are being listed.
     */
    inline ListPartsRequest& WithUploadId(const char* value) { SetUploadId(value); return *this;}


    
    inline const RequestPayer& GetRequestPayer() const{ return m_requestPayer; }

    
    inline void SetRequestPayer(const RequestPayer& value) { m_requestPayerHasBeenSet = true; m_requestPayer = value; }

    
    inline void SetRequestPayer(RequestPayer&& value) { m_requestPayerHasBeenSet = true; m_requestPayer = std::move(value); }

    
    inline ListPartsRequest& WithRequestPayer(const RequestPayer& value) { SetRequestPayer(value); return *this;}

    
    inline ListPartsRequest& WithRequestPayer(RequestPayer&& value) { SetRequestPayer(std::move(value)); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_key;
    bool m_keyHasBeenSet;

    int m_maxParts;
    bool m_maxPartsHasBeenSet;

    int m_partNumberMarker;
    bool m_partNumberMarkerHasBeenSet;

    Aws::String m_uploadId;
    bool m_uploadIdHasBeenSet;

    RequestPayer m_requestPayer;
    bool m_requestPayerHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
