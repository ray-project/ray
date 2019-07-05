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
#include <aws/core/utils/DateTime.h>
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
  class AWS_S3_API GetObjectRequest : public S3Request
  {
  public:
    GetObjectRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "GetObject"; }

    Aws::String SerializePayload() const override;

    void AddQueryStringParameters(Aws::Http::URI& uri) const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    
    inline GetObjectRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    
    inline GetObjectRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    
    inline GetObjectRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * Return the object only if its entity tag (ETag) is the same as the one
     * specified, otherwise return a 412 (precondition failed).
     */
    inline const Aws::String& GetIfMatch() const{ return m_ifMatch; }

    /**
     * Return the object only if its entity tag (ETag) is the same as the one
     * specified, otherwise return a 412 (precondition failed).
     */
    inline void SetIfMatch(const Aws::String& value) { m_ifMatchHasBeenSet = true; m_ifMatch = value; }

    /**
     * Return the object only if its entity tag (ETag) is the same as the one
     * specified, otherwise return a 412 (precondition failed).
     */
    inline void SetIfMatch(Aws::String&& value) { m_ifMatchHasBeenSet = true; m_ifMatch = std::move(value); }

    /**
     * Return the object only if its entity tag (ETag) is the same as the one
     * specified, otherwise return a 412 (precondition failed).
     */
    inline void SetIfMatch(const char* value) { m_ifMatchHasBeenSet = true; m_ifMatch.assign(value); }

    /**
     * Return the object only if its entity tag (ETag) is the same as the one
     * specified, otherwise return a 412 (precondition failed).
     */
    inline GetObjectRequest& WithIfMatch(const Aws::String& value) { SetIfMatch(value); return *this;}

    /**
     * Return the object only if its entity tag (ETag) is the same as the one
     * specified, otherwise return a 412 (precondition failed).
     */
    inline GetObjectRequest& WithIfMatch(Aws::String&& value) { SetIfMatch(std::move(value)); return *this;}

    /**
     * Return the object only if its entity tag (ETag) is the same as the one
     * specified, otherwise return a 412 (precondition failed).
     */
    inline GetObjectRequest& WithIfMatch(const char* value) { SetIfMatch(value); return *this;}


    /**
     * Return the object only if it has been modified since the specified time,
     * otherwise return a 304 (not modified).
     */
    inline const Aws::Utils::DateTime& GetIfModifiedSince() const{ return m_ifModifiedSince; }

    /**
     * Return the object only if it has been modified since the specified time,
     * otherwise return a 304 (not modified).
     */
    inline void SetIfModifiedSince(const Aws::Utils::DateTime& value) { m_ifModifiedSinceHasBeenSet = true; m_ifModifiedSince = value; }

    /**
     * Return the object only if it has been modified since the specified time,
     * otherwise return a 304 (not modified).
     */
    inline void SetIfModifiedSince(Aws::Utils::DateTime&& value) { m_ifModifiedSinceHasBeenSet = true; m_ifModifiedSince = std::move(value); }

    /**
     * Return the object only if it has been modified since the specified time,
     * otherwise return a 304 (not modified).
     */
    inline GetObjectRequest& WithIfModifiedSince(const Aws::Utils::DateTime& value) { SetIfModifiedSince(value); return *this;}

    /**
     * Return the object only if it has been modified since the specified time,
     * otherwise return a 304 (not modified).
     */
    inline GetObjectRequest& WithIfModifiedSince(Aws::Utils::DateTime&& value) { SetIfModifiedSince(std::move(value)); return *this;}


    /**
     * Return the object only if its entity tag (ETag) is different from the one
     * specified, otherwise return a 304 (not modified).
     */
    inline const Aws::String& GetIfNoneMatch() const{ return m_ifNoneMatch; }

    /**
     * Return the object only if its entity tag (ETag) is different from the one
     * specified, otherwise return a 304 (not modified).
     */
    inline void SetIfNoneMatch(const Aws::String& value) { m_ifNoneMatchHasBeenSet = true; m_ifNoneMatch = value; }

    /**
     * Return the object only if its entity tag (ETag) is different from the one
     * specified, otherwise return a 304 (not modified).
     */
    inline void SetIfNoneMatch(Aws::String&& value) { m_ifNoneMatchHasBeenSet = true; m_ifNoneMatch = std::move(value); }

    /**
     * Return the object only if its entity tag (ETag) is different from the one
     * specified, otherwise return a 304 (not modified).
     */
    inline void SetIfNoneMatch(const char* value) { m_ifNoneMatchHasBeenSet = true; m_ifNoneMatch.assign(value); }

    /**
     * Return the object only if its entity tag (ETag) is different from the one
     * specified, otherwise return a 304 (not modified).
     */
    inline GetObjectRequest& WithIfNoneMatch(const Aws::String& value) { SetIfNoneMatch(value); return *this;}

    /**
     * Return the object only if its entity tag (ETag) is different from the one
     * specified, otherwise return a 304 (not modified).
     */
    inline GetObjectRequest& WithIfNoneMatch(Aws::String&& value) { SetIfNoneMatch(std::move(value)); return *this;}

    /**
     * Return the object only if its entity tag (ETag) is different from the one
     * specified, otherwise return a 304 (not modified).
     */
    inline GetObjectRequest& WithIfNoneMatch(const char* value) { SetIfNoneMatch(value); return *this;}


    /**
     * Return the object only if it has not been modified since the specified time,
     * otherwise return a 412 (precondition failed).
     */
    inline const Aws::Utils::DateTime& GetIfUnmodifiedSince() const{ return m_ifUnmodifiedSince; }

    /**
     * Return the object only if it has not been modified since the specified time,
     * otherwise return a 412 (precondition failed).
     */
    inline void SetIfUnmodifiedSince(const Aws::Utils::DateTime& value) { m_ifUnmodifiedSinceHasBeenSet = true; m_ifUnmodifiedSince = value; }

    /**
     * Return the object only if it has not been modified since the specified time,
     * otherwise return a 412 (precondition failed).
     */
    inline void SetIfUnmodifiedSince(Aws::Utils::DateTime&& value) { m_ifUnmodifiedSinceHasBeenSet = true; m_ifUnmodifiedSince = std::move(value); }

    /**
     * Return the object only if it has not been modified since the specified time,
     * otherwise return a 412 (precondition failed).
     */
    inline GetObjectRequest& WithIfUnmodifiedSince(const Aws::Utils::DateTime& value) { SetIfUnmodifiedSince(value); return *this;}

    /**
     * Return the object only if it has not been modified since the specified time,
     * otherwise return a 412 (precondition failed).
     */
    inline GetObjectRequest& WithIfUnmodifiedSince(Aws::Utils::DateTime&& value) { SetIfUnmodifiedSince(std::move(value)); return *this;}


    
    inline const Aws::String& GetKey() const{ return m_key; }

    
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    
    inline GetObjectRequest& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    
    inline GetObjectRequest& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    
    inline GetObjectRequest& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * Downloads the specified range bytes of an object. For more information about the
     * HTTP Range header, go to
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35.
     */
    inline const Aws::String& GetRange() const{ return m_range; }

    /**
     * Downloads the specified range bytes of an object. For more information about the
     * HTTP Range header, go to
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35.
     */
    inline void SetRange(const Aws::String& value) { m_rangeHasBeenSet = true; m_range = value; }

    /**
     * Downloads the specified range bytes of an object. For more information about the
     * HTTP Range header, go to
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35.
     */
    inline void SetRange(Aws::String&& value) { m_rangeHasBeenSet = true; m_range = std::move(value); }

    /**
     * Downloads the specified range bytes of an object. For more information about the
     * HTTP Range header, go to
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35.
     */
    inline void SetRange(const char* value) { m_rangeHasBeenSet = true; m_range.assign(value); }

    /**
     * Downloads the specified range bytes of an object. For more information about the
     * HTTP Range header, go to
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35.
     */
    inline GetObjectRequest& WithRange(const Aws::String& value) { SetRange(value); return *this;}

    /**
     * Downloads the specified range bytes of an object. For more information about the
     * HTTP Range header, go to
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35.
     */
    inline GetObjectRequest& WithRange(Aws::String&& value) { SetRange(std::move(value)); return *this;}

    /**
     * Downloads the specified range bytes of an object. For more information about the
     * HTTP Range header, go to
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35.
     */
    inline GetObjectRequest& WithRange(const char* value) { SetRange(value); return *this;}


    /**
     * Sets the Cache-Control header of the response.
     */
    inline const Aws::String& GetResponseCacheControl() const{ return m_responseCacheControl; }

    /**
     * Sets the Cache-Control header of the response.
     */
    inline void SetResponseCacheControl(const Aws::String& value) { m_responseCacheControlHasBeenSet = true; m_responseCacheControl = value; }

    /**
     * Sets the Cache-Control header of the response.
     */
    inline void SetResponseCacheControl(Aws::String&& value) { m_responseCacheControlHasBeenSet = true; m_responseCacheControl = std::move(value); }

    /**
     * Sets the Cache-Control header of the response.
     */
    inline void SetResponseCacheControl(const char* value) { m_responseCacheControlHasBeenSet = true; m_responseCacheControl.assign(value); }

    /**
     * Sets the Cache-Control header of the response.
     */
    inline GetObjectRequest& WithResponseCacheControl(const Aws::String& value) { SetResponseCacheControl(value); return *this;}

    /**
     * Sets the Cache-Control header of the response.
     */
    inline GetObjectRequest& WithResponseCacheControl(Aws::String&& value) { SetResponseCacheControl(std::move(value)); return *this;}

    /**
     * Sets the Cache-Control header of the response.
     */
    inline GetObjectRequest& WithResponseCacheControl(const char* value) { SetResponseCacheControl(value); return *this;}


    /**
     * Sets the Content-Disposition header of the response
     */
    inline const Aws::String& GetResponseContentDisposition() const{ return m_responseContentDisposition; }

    /**
     * Sets the Content-Disposition header of the response
     */
    inline void SetResponseContentDisposition(const Aws::String& value) { m_responseContentDispositionHasBeenSet = true; m_responseContentDisposition = value; }

    /**
     * Sets the Content-Disposition header of the response
     */
    inline void SetResponseContentDisposition(Aws::String&& value) { m_responseContentDispositionHasBeenSet = true; m_responseContentDisposition = std::move(value); }

    /**
     * Sets the Content-Disposition header of the response
     */
    inline void SetResponseContentDisposition(const char* value) { m_responseContentDispositionHasBeenSet = true; m_responseContentDisposition.assign(value); }

    /**
     * Sets the Content-Disposition header of the response
     */
    inline GetObjectRequest& WithResponseContentDisposition(const Aws::String& value) { SetResponseContentDisposition(value); return *this;}

    /**
     * Sets the Content-Disposition header of the response
     */
    inline GetObjectRequest& WithResponseContentDisposition(Aws::String&& value) { SetResponseContentDisposition(std::move(value)); return *this;}

    /**
     * Sets the Content-Disposition header of the response
     */
    inline GetObjectRequest& WithResponseContentDisposition(const char* value) { SetResponseContentDisposition(value); return *this;}


    /**
     * Sets the Content-Encoding header of the response.
     */
    inline const Aws::String& GetResponseContentEncoding() const{ return m_responseContentEncoding; }

    /**
     * Sets the Content-Encoding header of the response.
     */
    inline void SetResponseContentEncoding(const Aws::String& value) { m_responseContentEncodingHasBeenSet = true; m_responseContentEncoding = value; }

    /**
     * Sets the Content-Encoding header of the response.
     */
    inline void SetResponseContentEncoding(Aws::String&& value) { m_responseContentEncodingHasBeenSet = true; m_responseContentEncoding = std::move(value); }

    /**
     * Sets the Content-Encoding header of the response.
     */
    inline void SetResponseContentEncoding(const char* value) { m_responseContentEncodingHasBeenSet = true; m_responseContentEncoding.assign(value); }

    /**
     * Sets the Content-Encoding header of the response.
     */
    inline GetObjectRequest& WithResponseContentEncoding(const Aws::String& value) { SetResponseContentEncoding(value); return *this;}

    /**
     * Sets the Content-Encoding header of the response.
     */
    inline GetObjectRequest& WithResponseContentEncoding(Aws::String&& value) { SetResponseContentEncoding(std::move(value)); return *this;}

    /**
     * Sets the Content-Encoding header of the response.
     */
    inline GetObjectRequest& WithResponseContentEncoding(const char* value) { SetResponseContentEncoding(value); return *this;}


    /**
     * Sets the Content-Language header of the response.
     */
    inline const Aws::String& GetResponseContentLanguage() const{ return m_responseContentLanguage; }

    /**
     * Sets the Content-Language header of the response.
     */
    inline void SetResponseContentLanguage(const Aws::String& value) { m_responseContentLanguageHasBeenSet = true; m_responseContentLanguage = value; }

    /**
     * Sets the Content-Language header of the response.
     */
    inline void SetResponseContentLanguage(Aws::String&& value) { m_responseContentLanguageHasBeenSet = true; m_responseContentLanguage = std::move(value); }

    /**
     * Sets the Content-Language header of the response.
     */
    inline void SetResponseContentLanguage(const char* value) { m_responseContentLanguageHasBeenSet = true; m_responseContentLanguage.assign(value); }

    /**
     * Sets the Content-Language header of the response.
     */
    inline GetObjectRequest& WithResponseContentLanguage(const Aws::String& value) { SetResponseContentLanguage(value); return *this;}

    /**
     * Sets the Content-Language header of the response.
     */
    inline GetObjectRequest& WithResponseContentLanguage(Aws::String&& value) { SetResponseContentLanguage(std::move(value)); return *this;}

    /**
     * Sets the Content-Language header of the response.
     */
    inline GetObjectRequest& WithResponseContentLanguage(const char* value) { SetResponseContentLanguage(value); return *this;}


    /**
     * Sets the Content-Type header of the response.
     */
    inline const Aws::String& GetResponseContentType() const{ return m_responseContentType; }

    /**
     * Sets the Content-Type header of the response.
     */
    inline void SetResponseContentType(const Aws::String& value) { m_responseContentTypeHasBeenSet = true; m_responseContentType = value; }

    /**
     * Sets the Content-Type header of the response.
     */
    inline void SetResponseContentType(Aws::String&& value) { m_responseContentTypeHasBeenSet = true; m_responseContentType = std::move(value); }

    /**
     * Sets the Content-Type header of the response.
     */
    inline void SetResponseContentType(const char* value) { m_responseContentTypeHasBeenSet = true; m_responseContentType.assign(value); }

    /**
     * Sets the Content-Type header of the response.
     */
    inline GetObjectRequest& WithResponseContentType(const Aws::String& value) { SetResponseContentType(value); return *this;}

    /**
     * Sets the Content-Type header of the response.
     */
    inline GetObjectRequest& WithResponseContentType(Aws::String&& value) { SetResponseContentType(std::move(value)); return *this;}

    /**
     * Sets the Content-Type header of the response.
     */
    inline GetObjectRequest& WithResponseContentType(const char* value) { SetResponseContentType(value); return *this;}


    /**
     * Sets the Expires header of the response.
     */
    inline const Aws::Utils::DateTime& GetResponseExpires() const{ return m_responseExpires; }

    /**
     * Sets the Expires header of the response.
     */
    inline void SetResponseExpires(const Aws::Utils::DateTime& value) { m_responseExpiresHasBeenSet = true; m_responseExpires = value; }

    /**
     * Sets the Expires header of the response.
     */
    inline void SetResponseExpires(Aws::Utils::DateTime&& value) { m_responseExpiresHasBeenSet = true; m_responseExpires = std::move(value); }

    /**
     * Sets the Expires header of the response.
     */
    inline GetObjectRequest& WithResponseExpires(const Aws::Utils::DateTime& value) { SetResponseExpires(value); return *this;}

    /**
     * Sets the Expires header of the response.
     */
    inline GetObjectRequest& WithResponseExpires(Aws::Utils::DateTime&& value) { SetResponseExpires(std::move(value)); return *this;}


    /**
     * VersionId used to reference a specific version of the object.
     */
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    /**
     * VersionId used to reference a specific version of the object.
     */
    inline void SetVersionId(const Aws::String& value) { m_versionIdHasBeenSet = true; m_versionId = value; }

    /**
     * VersionId used to reference a specific version of the object.
     */
    inline void SetVersionId(Aws::String&& value) { m_versionIdHasBeenSet = true; m_versionId = std::move(value); }

    /**
     * VersionId used to reference a specific version of the object.
     */
    inline void SetVersionId(const char* value) { m_versionIdHasBeenSet = true; m_versionId.assign(value); }

    /**
     * VersionId used to reference a specific version of the object.
     */
    inline GetObjectRequest& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    /**
     * VersionId used to reference a specific version of the object.
     */
    inline GetObjectRequest& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    /**
     * VersionId used to reference a specific version of the object.
     */
    inline GetObjectRequest& WithVersionId(const char* value) { SetVersionId(value); return *this;}


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
    inline GetObjectRequest& WithSSECustomerAlgorithm(const Aws::String& value) { SetSSECustomerAlgorithm(value); return *this;}

    /**
     * Specifies the algorithm to use to when encrypting the object (e.g., AES256).
     */
    inline GetObjectRequest& WithSSECustomerAlgorithm(Aws::String&& value) { SetSSECustomerAlgorithm(std::move(value)); return *this;}

    /**
     * Specifies the algorithm to use to when encrypting the object (e.g., AES256).
     */
    inline GetObjectRequest& WithSSECustomerAlgorithm(const char* value) { SetSSECustomerAlgorithm(value); return *this;}


    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon does not store the encryption key. The key must be appropriate
     * for use with the algorithm specified in the
     * x-amz-server-side​-encryption​-customer-algorithm header.
     */
    inline const Aws::String& GetSSECustomerKey() const{ return m_sSECustomerKey; }

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon does not store the encryption key. The key must be appropriate
     * for use with the algorithm specified in the
     * x-amz-server-side​-encryption​-customer-algorithm header.
     */
    inline void SetSSECustomerKey(const Aws::String& value) { m_sSECustomerKeyHasBeenSet = true; m_sSECustomerKey = value; }

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon does not store the encryption key. The key must be appropriate
     * for use with the algorithm specified in the
     * x-amz-server-side​-encryption​-customer-algorithm header.
     */
    inline void SetSSECustomerKey(Aws::String&& value) { m_sSECustomerKeyHasBeenSet = true; m_sSECustomerKey = std::move(value); }

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon does not store the encryption key. The key must be appropriate
     * for use with the algorithm specified in the
     * x-amz-server-side​-encryption​-customer-algorithm header.
     */
    inline void SetSSECustomerKey(const char* value) { m_sSECustomerKeyHasBeenSet = true; m_sSECustomerKey.assign(value); }

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon does not store the encryption key. The key must be appropriate
     * for use with the algorithm specified in the
     * x-amz-server-side​-encryption​-customer-algorithm header.
     */
    inline GetObjectRequest& WithSSECustomerKey(const Aws::String& value) { SetSSECustomerKey(value); return *this;}

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon does not store the encryption key. The key must be appropriate
     * for use with the algorithm specified in the
     * x-amz-server-side​-encryption​-customer-algorithm header.
     */
    inline GetObjectRequest& WithSSECustomerKey(Aws::String&& value) { SetSSECustomerKey(std::move(value)); return *this;}

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon does not store the encryption key. The key must be appropriate
     * for use with the algorithm specified in the
     * x-amz-server-side​-encryption​-customer-algorithm header.
     */
    inline GetObjectRequest& WithSSECustomerKey(const char* value) { SetSSECustomerKey(value); return *this;}


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
    inline GetObjectRequest& WithSSECustomerKeyMD5(const Aws::String& value) { SetSSECustomerKeyMD5(value); return *this;}

    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline GetObjectRequest& WithSSECustomerKeyMD5(Aws::String&& value) { SetSSECustomerKeyMD5(std::move(value)); return *this;}

    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline GetObjectRequest& WithSSECustomerKeyMD5(const char* value) { SetSSECustomerKeyMD5(value); return *this;}


    
    inline const RequestPayer& GetRequestPayer() const{ return m_requestPayer; }

    
    inline void SetRequestPayer(const RequestPayer& value) { m_requestPayerHasBeenSet = true; m_requestPayer = value; }

    
    inline void SetRequestPayer(RequestPayer&& value) { m_requestPayerHasBeenSet = true; m_requestPayer = std::move(value); }

    
    inline GetObjectRequest& WithRequestPayer(const RequestPayer& value) { SetRequestPayer(value); return *this;}

    
    inline GetObjectRequest& WithRequestPayer(RequestPayer&& value) { SetRequestPayer(std::move(value)); return *this;}


    /**
     * Part number of the object being read. This is a positive integer between 1 and
     * 10,000. Effectively performs a 'ranged' GET request for the part specified.
     * Useful for downloading just a part of an object.
     */
    inline int GetPartNumber() const{ return m_partNumber; }

    /**
     * Part number of the object being read. This is a positive integer between 1 and
     * 10,000. Effectively performs a 'ranged' GET request for the part specified.
     * Useful for downloading just a part of an object.
     */
    inline void SetPartNumber(int value) { m_partNumberHasBeenSet = true; m_partNumber = value; }

    /**
     * Part number of the object being read. This is a positive integer between 1 and
     * 10,000. Effectively performs a 'ranged' GET request for the part specified.
     * Useful for downloading just a part of an object.
     */
    inline GetObjectRequest& WithPartNumber(int value) { SetPartNumber(value); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_ifMatch;
    bool m_ifMatchHasBeenSet;

    Aws::Utils::DateTime m_ifModifiedSince;
    bool m_ifModifiedSinceHasBeenSet;

    Aws::String m_ifNoneMatch;
    bool m_ifNoneMatchHasBeenSet;

    Aws::Utils::DateTime m_ifUnmodifiedSince;
    bool m_ifUnmodifiedSinceHasBeenSet;

    Aws::String m_key;
    bool m_keyHasBeenSet;

    Aws::String m_range;
    bool m_rangeHasBeenSet;

    Aws::String m_responseCacheControl;
    bool m_responseCacheControlHasBeenSet;

    Aws::String m_responseContentDisposition;
    bool m_responseContentDispositionHasBeenSet;

    Aws::String m_responseContentEncoding;
    bool m_responseContentEncodingHasBeenSet;

    Aws::String m_responseContentLanguage;
    bool m_responseContentLanguageHasBeenSet;

    Aws::String m_responseContentType;
    bool m_responseContentTypeHasBeenSet;

    Aws::Utils::DateTime m_responseExpires;
    bool m_responseExpiresHasBeenSet;

    Aws::String m_versionId;
    bool m_versionIdHasBeenSet;

    Aws::String m_sSECustomerAlgorithm;
    bool m_sSECustomerAlgorithmHasBeenSet;

    Aws::String m_sSECustomerKey;
    bool m_sSECustomerKeyHasBeenSet;

    Aws::String m_sSECustomerKeyMD5;
    bool m_sSECustomerKeyMD5HasBeenSet;

    RequestPayer m_requestPayer;
    bool m_requestPayerHasBeenSet;

    int m_partNumber;
    bool m_partNumberHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
