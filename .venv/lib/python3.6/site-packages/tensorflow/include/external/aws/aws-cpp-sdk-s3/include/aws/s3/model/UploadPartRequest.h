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
#include <aws/core/utils/Array.h>
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
  class AWS_S3_API UploadPartRequest : public StreamingS3Request
  {
  public:
    UploadPartRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "UploadPart"; }

    void AddQueryStringParameters(Aws::Http::URI& uri) const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    /**
     * Name of the bucket to which the multipart upload was initiated.
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * Name of the bucket to which the multipart upload was initiated.
     */
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /**
     * Name of the bucket to which the multipart upload was initiated.
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /**
     * Name of the bucket to which the multipart upload was initiated.
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /**
     * Name of the bucket to which the multipart upload was initiated.
     */
    inline UploadPartRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * Name of the bucket to which the multipart upload was initiated.
     */
    inline UploadPartRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * Name of the bucket to which the multipart upload was initiated.
     */
    inline UploadPartRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * Size of the body in bytes. This parameter is useful when the size of the body
     * cannot be determined automatically.
     */
    inline long long GetContentLength() const{ return m_contentLength; }

    /**
     * Size of the body in bytes. This parameter is useful when the size of the body
     * cannot be determined automatically.
     */
    inline void SetContentLength(long long value) { m_contentLengthHasBeenSet = true; m_contentLength = value; }

    /**
     * Size of the body in bytes. This parameter is useful when the size of the body
     * cannot be determined automatically.
     */
    inline UploadPartRequest& WithContentLength(long long value) { SetContentLength(value); return *this;}


    /**
     * The base64-encoded 128-bit MD5 digest of the part data.
     */
    inline const Aws::String& GetContentMD5() const{ return m_contentMD5; }

    /**
     * The base64-encoded 128-bit MD5 digest of the part data.
     */
    inline void SetContentMD5(const Aws::String& value) { m_contentMD5HasBeenSet = true; m_contentMD5 = value; }

    /**
     * The base64-encoded 128-bit MD5 digest of the part data.
     */
    inline void SetContentMD5(Aws::String&& value) { m_contentMD5HasBeenSet = true; m_contentMD5 = std::move(value); }

    /**
     * The base64-encoded 128-bit MD5 digest of the part data.
     */
    inline void SetContentMD5(const char* value) { m_contentMD5HasBeenSet = true; m_contentMD5.assign(value); }

    /**
     * The base64-encoded 128-bit MD5 digest of the part data.
     */
    inline UploadPartRequest& WithContentMD5(const Aws::String& value) { SetContentMD5(value); return *this;}

    /**
     * The base64-encoded 128-bit MD5 digest of the part data.
     */
    inline UploadPartRequest& WithContentMD5(Aws::String&& value) { SetContentMD5(std::move(value)); return *this;}

    /**
     * The base64-encoded 128-bit MD5 digest of the part data.
     */
    inline UploadPartRequest& WithContentMD5(const char* value) { SetContentMD5(value); return *this;}


    /**
     * Object key for which the multipart upload was initiated.
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * Object key for which the multipart upload was initiated.
     */
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    /**
     * Object key for which the multipart upload was initiated.
     */
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    /**
     * Object key for which the multipart upload was initiated.
     */
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    /**
     * Object key for which the multipart upload was initiated.
     */
    inline UploadPartRequest& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * Object key for which the multipart upload was initiated.
     */
    inline UploadPartRequest& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * Object key for which the multipart upload was initiated.
     */
    inline UploadPartRequest& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * Part number of part being uploaded. This is a positive integer between 1 and
     * 10,000.
     */
    inline int GetPartNumber() const{ return m_partNumber; }

    /**
     * Part number of part being uploaded. This is a positive integer between 1 and
     * 10,000.
     */
    inline void SetPartNumber(int value) { m_partNumberHasBeenSet = true; m_partNumber = value; }

    /**
     * Part number of part being uploaded. This is a positive integer between 1 and
     * 10,000.
     */
    inline UploadPartRequest& WithPartNumber(int value) { SetPartNumber(value); return *this;}


    /**
     * Upload ID identifying the multipart upload whose part is being uploaded.
     */
    inline const Aws::String& GetUploadId() const{ return m_uploadId; }

    /**
     * Upload ID identifying the multipart upload whose part is being uploaded.
     */
    inline void SetUploadId(const Aws::String& value) { m_uploadIdHasBeenSet = true; m_uploadId = value; }

    /**
     * Upload ID identifying the multipart upload whose part is being uploaded.
     */
    inline void SetUploadId(Aws::String&& value) { m_uploadIdHasBeenSet = true; m_uploadId = std::move(value); }

    /**
     * Upload ID identifying the multipart upload whose part is being uploaded.
     */
    inline void SetUploadId(const char* value) { m_uploadIdHasBeenSet = true; m_uploadId.assign(value); }

    /**
     * Upload ID identifying the multipart upload whose part is being uploaded.
     */
    inline UploadPartRequest& WithUploadId(const Aws::String& value) { SetUploadId(value); return *this;}

    /**
     * Upload ID identifying the multipart upload whose part is being uploaded.
     */
    inline UploadPartRequest& WithUploadId(Aws::String&& value) { SetUploadId(std::move(value)); return *this;}

    /**
     * Upload ID identifying the multipart upload whose part is being uploaded.
     */
    inline UploadPartRequest& WithUploadId(const char* value) { SetUploadId(value); return *this;}


    /**
     * Specifies the algorithm to use to when encrypting the object (e.g., AES256).
     */
    inline const Aws::String& GetSSECustomerAlgorithm() const{ return m_sSECustomerAlgorithm; }

    /**
     * Specifies the algorithm to use to when encrypting the object (e.g., AES256).
     */
    inline void SetSSECustomerAlgorithm(const Aws::String& value) { m_sSECustomerAlgorithmHasBeenSet = true; m_sSECustomerAlgorithm = value; }

    /**
     * Specifies the algorithm to use to when encrypting the object (e.g., AES256).
     */
    inline void SetSSECustomerAlgorithm(Aws::String&& value) { m_sSECustomerAlgorithmHasBeenSet = true; m_sSECustomerAlgorithm = std::move(value); }

    /**
     * Specifies the algorithm to use to when encrypting the object (e.g., AES256).
     */
    inline void SetSSECustomerAlgorithm(const char* value) { m_sSECustomerAlgorithmHasBeenSet = true; m_sSECustomerAlgorithm.assign(value); }

    /**
     * Specifies the algorithm to use to when encrypting the object (e.g., AES256).
     */
    inline UploadPartRequest& WithSSECustomerAlgorithm(const Aws::String& value) { SetSSECustomerAlgorithm(value); return *this;}

    /**
     * Specifies the algorithm to use to when encrypting the object (e.g., AES256).
     */
    inline UploadPartRequest& WithSSECustomerAlgorithm(Aws::String&& value) { SetSSECustomerAlgorithm(std::move(value)); return *this;}

    /**
     * Specifies the algorithm to use to when encrypting the object (e.g., AES256).
     */
    inline UploadPartRequest& WithSSECustomerAlgorithm(const char* value) { SetSSECustomerAlgorithm(value); return *this;}


    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon does not store the encryption key. The key must be appropriate
     * for use with the algorithm specified in the
     * x-amz-server-side​-encryption​-customer-algorithm header. This must be the same
     * encryption key specified in the initiate multipart upload request.
     */
    inline const Aws::String& GetSSECustomerKey() const{ return m_sSECustomerKey; }

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon does not store the encryption key. The key must be appropriate
     * for use with the algorithm specified in the
     * x-amz-server-side​-encryption​-customer-algorithm header. This must be the same
     * encryption key specified in the initiate multipart upload request.
     */
    inline void SetSSECustomerKey(const Aws::String& value) { m_sSECustomerKeyHasBeenSet = true; m_sSECustomerKey = value; }

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon does not store the encryption key. The key must be appropriate
     * for use with the algorithm specified in the
     * x-amz-server-side​-encryption​-customer-algorithm header. This must be the same
     * encryption key specified in the initiate multipart upload request.
     */
    inline void SetSSECustomerKey(Aws::String&& value) { m_sSECustomerKeyHasBeenSet = true; m_sSECustomerKey = std::move(value); }

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon does not store the encryption key. The key must be appropriate
     * for use with the algorithm specified in the
     * x-amz-server-side​-encryption​-customer-algorithm header. This must be the same
     * encryption key specified in the initiate multipart upload request.
     */
    inline void SetSSECustomerKey(const char* value) { m_sSECustomerKeyHasBeenSet = true; m_sSECustomerKey.assign(value); }

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon does not store the encryption key. The key must be appropriate
     * for use with the algorithm specified in the
     * x-amz-server-side​-encryption​-customer-algorithm header. This must be the same
     * encryption key specified in the initiate multipart upload request.
     */
    inline UploadPartRequest& WithSSECustomerKey(const Aws::String& value) { SetSSECustomerKey(value); return *this;}

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon does not store the encryption key. The key must be appropriate
     * for use with the algorithm specified in the
     * x-amz-server-side​-encryption​-customer-algorithm header. This must be the same
     * encryption key specified in the initiate multipart upload request.
     */
    inline UploadPartRequest& WithSSECustomerKey(Aws::String&& value) { SetSSECustomerKey(std::move(value)); return *this;}

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon does not store the encryption key. The key must be appropriate
     * for use with the algorithm specified in the
     * x-amz-server-side​-encryption​-customer-algorithm header. This must be the same
     * encryption key specified in the initiate multipart upload request.
     */
    inline UploadPartRequest& WithSSECustomerKey(const char* value) { SetSSECustomerKey(value); return *this;}


    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline const Aws::String& GetSSECustomerKeyMD5() const{ return m_sSECustomerKeyMD5; }

    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline void SetSSECustomerKeyMD5(const Aws::String& value) { m_sSECustomerKeyMD5HasBeenSet = true; m_sSECustomerKeyMD5 = value; }

    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline void SetSSECustomerKeyMD5(Aws::String&& value) { m_sSECustomerKeyMD5HasBeenSet = true; m_sSECustomerKeyMD5 = std::move(value); }

    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline void SetSSECustomerKeyMD5(const char* value) { m_sSECustomerKeyMD5HasBeenSet = true; m_sSECustomerKeyMD5.assign(value); }

    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline UploadPartRequest& WithSSECustomerKeyMD5(const Aws::String& value) { SetSSECustomerKeyMD5(value); return *this;}

    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline UploadPartRequest& WithSSECustomerKeyMD5(Aws::String&& value) { SetSSECustomerKeyMD5(std::move(value)); return *this;}

    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline UploadPartRequest& WithSSECustomerKeyMD5(const char* value) { SetSSECustomerKeyMD5(value); return *this;}


    
    inline const RequestPayer& GetRequestPayer() const{ return m_requestPayer; }

    
    inline void SetRequestPayer(const RequestPayer& value) { m_requestPayerHasBeenSet = true; m_requestPayer = value; }

    
    inline void SetRequestPayer(RequestPayer&& value) { m_requestPayerHasBeenSet = true; m_requestPayer = std::move(value); }

    
    inline UploadPartRequest& WithRequestPayer(const RequestPayer& value) { SetRequestPayer(value); return *this;}

    
    inline UploadPartRequest& WithRequestPayer(RequestPayer&& value) { SetRequestPayer(std::move(value)); return *this;}

  private:


    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    long long m_contentLength;
    bool m_contentLengthHasBeenSet;

    Aws::String m_contentMD5;
    bool m_contentMD5HasBeenSet;

    Aws::String m_key;
    bool m_keyHasBeenSet;

    int m_partNumber;
    bool m_partNumberHasBeenSet;

    Aws::String m_uploadId;
    bool m_uploadIdHasBeenSet;

    Aws::String m_sSECustomerAlgorithm;
    bool m_sSECustomerAlgorithmHasBeenSet;

    Aws::String m_sSECustomerKey;
    bool m_sSECustomerKeyHasBeenSet;

    Aws::String m_sSECustomerKeyMD5;
    bool m_sSECustomerKeyMD5HasBeenSet;

    RequestPayer m_requestPayer;
    bool m_requestPayerHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
