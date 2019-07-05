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
#include <aws/core/utils/stream/ResponseStream.h>
#include <aws/core/utils/Array.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/DateTime.h>
#include <aws/s3/model/ServerSideEncryption.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/s3/model/StorageClass.h>
#include <aws/s3/model/RequestCharged.h>
#include <aws/s3/model/ReplicationStatus.h>
#include <utility>

namespace Aws
{
template<typename RESULT_TYPE>
class AmazonWebServiceResult;

namespace S3
{
namespace Model
{
  class AWS_S3_API GetObjectResult
  {
  public:
    GetObjectResult();
    //We have to define these because Microsoft doesn't auto generate them
    GetObjectResult(GetObjectResult&&);
    GetObjectResult& operator=(GetObjectResult&&);
    //we delete these because Microsoft doesn't handle move generation correctly
    //and we therefore don't trust them to get it right here either.
    GetObjectResult(const GetObjectResult&) = delete;
    GetObjectResult& operator=(const GetObjectResult&) = delete;


    GetObjectResult(Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream>&& result);
    GetObjectResult& operator=(Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream>&& result);



    /**
     * Object data.
     */
    inline Aws::IOStream& GetBody() { return m_body.GetUnderlyingStream(); }

    /**
     * Object data.
     */
    inline void ReplaceBody(Aws::IOStream* body) { m_body = Aws::Utils::Stream::ResponseStream(body); }
    

    /**
     * Specifies whether the object retrieved was (true) or was not (false) a Delete
     * Marker. If false, this response header does not appear in the response.
     */
    inline bool GetDeleteMarker() const{ return m_deleteMarker; }

    /**
     * Specifies whether the object retrieved was (true) or was not (false) a Delete
     * Marker. If false, this response header does not appear in the response.
     */
    inline void SetDeleteMarker(bool value) { m_deleteMarker = value; }

    /**
     * Specifies whether the object retrieved was (true) or was not (false) a Delete
     * Marker. If false, this response header does not appear in the response.
     */
    inline GetObjectResult& WithDeleteMarker(bool value) { SetDeleteMarker(value); return *this;}


    
    inline const Aws::String& GetAcceptRanges() const{ return m_acceptRanges; }

    
    inline void SetAcceptRanges(const Aws::String& value) { m_acceptRanges = value; }

    
    inline void SetAcceptRanges(Aws::String&& value) { m_acceptRanges = std::move(value); }

    
    inline void SetAcceptRanges(const char* value) { m_acceptRanges.assign(value); }

    
    inline GetObjectResult& WithAcceptRanges(const Aws::String& value) { SetAcceptRanges(value); return *this;}

    
    inline GetObjectResult& WithAcceptRanges(Aws::String&& value) { SetAcceptRanges(std::move(value)); return *this;}

    
    inline GetObjectResult& WithAcceptRanges(const char* value) { SetAcceptRanges(value); return *this;}


    /**
     * If the object expiration is configured (see PUT Bucket lifecycle), the response
     * includes this header. It includes the expiry-date and rule-id key value pairs
     * providing object expiration information. The value of the rule-id is URL
     * encoded.
     */
    inline const Aws::String& GetExpiration() const{ return m_expiration; }

    /**
     * If the object expiration is configured (see PUT Bucket lifecycle), the response
     * includes this header. It includes the expiry-date and rule-id key value pairs
     * providing object expiration information. The value of the rule-id is URL
     * encoded.
     */
    inline void SetExpiration(const Aws::String& value) { m_expiration = value; }

    /**
     * If the object expiration is configured (see PUT Bucket lifecycle), the response
     * includes this header. It includes the expiry-date and rule-id key value pairs
     * providing object expiration information. The value of the rule-id is URL
     * encoded.
     */
    inline void SetExpiration(Aws::String&& value) { m_expiration = std::move(value); }

    /**
     * If the object expiration is configured (see PUT Bucket lifecycle), the response
     * includes this header. It includes the expiry-date and rule-id key value pairs
     * providing object expiration information. The value of the rule-id is URL
     * encoded.
     */
    inline void SetExpiration(const char* value) { m_expiration.assign(value); }

    /**
     * If the object expiration is configured (see PUT Bucket lifecycle), the response
     * includes this header. It includes the expiry-date and rule-id key value pairs
     * providing object expiration information. The value of the rule-id is URL
     * encoded.
     */
    inline GetObjectResult& WithExpiration(const Aws::String& value) { SetExpiration(value); return *this;}

    /**
     * If the object expiration is configured (see PUT Bucket lifecycle), the response
     * includes this header. It includes the expiry-date and rule-id key value pairs
     * providing object expiration information. The value of the rule-id is URL
     * encoded.
     */
    inline GetObjectResult& WithExpiration(Aws::String&& value) { SetExpiration(std::move(value)); return *this;}

    /**
     * If the object expiration is configured (see PUT Bucket lifecycle), the response
     * includes this header. It includes the expiry-date and rule-id key value pairs
     * providing object expiration information. The value of the rule-id is URL
     * encoded.
     */
    inline GetObjectResult& WithExpiration(const char* value) { SetExpiration(value); return *this;}


    /**
     * Provides information about object restoration operation and expiration time of
     * the restored object copy.
     */
    inline const Aws::String& GetRestore() const{ return m_restore; }

    /**
     * Provides information about object restoration operation and expiration time of
     * the restored object copy.
     */
    inline void SetRestore(const Aws::String& value) { m_restore = value; }

    /**
     * Provides information about object restoration operation and expiration time of
     * the restored object copy.
     */
    inline void SetRestore(Aws::String&& value) { m_restore = std::move(value); }

    /**
     * Provides information about object restoration operation and expiration time of
     * the restored object copy.
     */
    inline void SetRestore(const char* value) { m_restore.assign(value); }

    /**
     * Provides information about object restoration operation and expiration time of
     * the restored object copy.
     */
    inline GetObjectResult& WithRestore(const Aws::String& value) { SetRestore(value); return *this;}

    /**
     * Provides information about object restoration operation and expiration time of
     * the restored object copy.
     */
    inline GetObjectResult& WithRestore(Aws::String&& value) { SetRestore(std::move(value)); return *this;}

    /**
     * Provides information about object restoration operation and expiration time of
     * the restored object copy.
     */
    inline GetObjectResult& WithRestore(const char* value) { SetRestore(value); return *this;}


    /**
     * Last modified date of the object
     */
    inline const Aws::Utils::DateTime& GetLastModified() const{ return m_lastModified; }

    /**
     * Last modified date of the object
     */
    inline void SetLastModified(const Aws::Utils::DateTime& value) { m_lastModified = value; }

    /**
     * Last modified date of the object
     */
    inline void SetLastModified(Aws::Utils::DateTime&& value) { m_lastModified = std::move(value); }

    /**
     * Last modified date of the object
     */
    inline GetObjectResult& WithLastModified(const Aws::Utils::DateTime& value) { SetLastModified(value); return *this;}

    /**
     * Last modified date of the object
     */
    inline GetObjectResult& WithLastModified(Aws::Utils::DateTime&& value) { SetLastModified(std::move(value)); return *this;}


    /**
     * Size of the body in bytes.
     */
    inline long long GetContentLength() const{ return m_contentLength; }

    /**
     * Size of the body in bytes.
     */
    inline void SetContentLength(long long value) { m_contentLength = value; }

    /**
     * Size of the body in bytes.
     */
    inline GetObjectResult& WithContentLength(long long value) { SetContentLength(value); return *this;}


    /**
     * An ETag is an opaque identifier assigned by a web server to a specific version
     * of a resource found at a URL
     */
    inline const Aws::String& GetETag() const{ return m_eTag; }

    /**
     * An ETag is an opaque identifier assigned by a web server to a specific version
     * of a resource found at a URL
     */
    inline void SetETag(const Aws::String& value) { m_eTag = value; }

    /**
     * An ETag is an opaque identifier assigned by a web server to a specific version
     * of a resource found at a URL
     */
    inline void SetETag(Aws::String&& value) { m_eTag = std::move(value); }

    /**
     * An ETag is an opaque identifier assigned by a web server to a specific version
     * of a resource found at a URL
     */
    inline void SetETag(const char* value) { m_eTag.assign(value); }

    /**
     * An ETag is an opaque identifier assigned by a web server to a specific version
     * of a resource found at a URL
     */
    inline GetObjectResult& WithETag(const Aws::String& value) { SetETag(value); return *this;}

    /**
     * An ETag is an opaque identifier assigned by a web server to a specific version
     * of a resource found at a URL
     */
    inline GetObjectResult& WithETag(Aws::String&& value) { SetETag(std::move(value)); return *this;}

    /**
     * An ETag is an opaque identifier assigned by a web server to a specific version
     * of a resource found at a URL
     */
    inline GetObjectResult& WithETag(const char* value) { SetETag(value); return *this;}


    /**
     * This is set to the number of metadata entries not returned in x-amz-meta
     * headers. This can happen if you create metadata using an API like SOAP that
     * supports more flexible metadata than the REST API. For example, using SOAP, you
     * can create metadata whose values are not legal HTTP headers.
     */
    inline int GetMissingMeta() const{ return m_missingMeta; }

    /**
     * This is set to the number of metadata entries not returned in x-amz-meta
     * headers. This can happen if you create metadata using an API like SOAP that
     * supports more flexible metadata than the REST API. For example, using SOAP, you
     * can create metadata whose values are not legal HTTP headers.
     */
    inline void SetMissingMeta(int value) { m_missingMeta = value; }

    /**
     * This is set to the number of metadata entries not returned in x-amz-meta
     * headers. This can happen if you create metadata using an API like SOAP that
     * supports more flexible metadata than the REST API. For example, using SOAP, you
     * can create metadata whose values are not legal HTTP headers.
     */
    inline GetObjectResult& WithMissingMeta(int value) { SetMissingMeta(value); return *this;}


    /**
     * Version of the object.
     */
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    /**
     * Version of the object.
     */
    inline void SetVersionId(const Aws::String& value) { m_versionId = value; }

    /**
     * Version of the object.
     */
    inline void SetVersionId(Aws::String&& value) { m_versionId = std::move(value); }

    /**
     * Version of the object.
     */
    inline void SetVersionId(const char* value) { m_versionId.assign(value); }

    /**
     * Version of the object.
     */
    inline GetObjectResult& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    /**
     * Version of the object.
     */
    inline GetObjectResult& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    /**
     * Version of the object.
     */
    inline GetObjectResult& WithVersionId(const char* value) { SetVersionId(value); return *this;}


    /**
     * Specifies caching behavior along the request/reply chain.
     */
    inline const Aws::String& GetCacheControl() const{ return m_cacheControl; }

    /**
     * Specifies caching behavior along the request/reply chain.
     */
    inline void SetCacheControl(const Aws::String& value) { m_cacheControl = value; }

    /**
     * Specifies caching behavior along the request/reply chain.
     */
    inline void SetCacheControl(Aws::String&& value) { m_cacheControl = std::move(value); }

    /**
     * Specifies caching behavior along the request/reply chain.
     */
    inline void SetCacheControl(const char* value) { m_cacheControl.assign(value); }

    /**
     * Specifies caching behavior along the request/reply chain.
     */
    inline GetObjectResult& WithCacheControl(const Aws::String& value) { SetCacheControl(value); return *this;}

    /**
     * Specifies caching behavior along the request/reply chain.
     */
    inline GetObjectResult& WithCacheControl(Aws::String&& value) { SetCacheControl(std::move(value)); return *this;}

    /**
     * Specifies caching behavior along the request/reply chain.
     */
    inline GetObjectResult& WithCacheControl(const char* value) { SetCacheControl(value); return *this;}


    /**
     * Specifies presentational information for the object.
     */
    inline const Aws::String& GetContentDisposition() const{ return m_contentDisposition; }

    /**
     * Specifies presentational information for the object.
     */
    inline void SetContentDisposition(const Aws::String& value) { m_contentDisposition = value; }

    /**
     * Specifies presentational information for the object.
     */
    inline void SetContentDisposition(Aws::String&& value) { m_contentDisposition = std::move(value); }

    /**
     * Specifies presentational information for the object.
     */
    inline void SetContentDisposition(const char* value) { m_contentDisposition.assign(value); }

    /**
     * Specifies presentational information for the object.
     */
    inline GetObjectResult& WithContentDisposition(const Aws::String& value) { SetContentDisposition(value); return *this;}

    /**
     * Specifies presentational information for the object.
     */
    inline GetObjectResult& WithContentDisposition(Aws::String&& value) { SetContentDisposition(std::move(value)); return *this;}

    /**
     * Specifies presentational information for the object.
     */
    inline GetObjectResult& WithContentDisposition(const char* value) { SetContentDisposition(value); return *this;}


    /**
     * Specifies what content encodings have been applied to the object and thus what
     * decoding mechanisms must be applied to obtain the media-type referenced by the
     * Content-Type header field.
     */
    inline const Aws::String& GetContentEncoding() const{ return m_contentEncoding; }

    /**
     * Specifies what content encodings have been applied to the object and thus what
     * decoding mechanisms must be applied to obtain the media-type referenced by the
     * Content-Type header field.
     */
    inline void SetContentEncoding(const Aws::String& value) { m_contentEncoding = value; }

    /**
     * Specifies what content encodings have been applied to the object and thus what
     * decoding mechanisms must be applied to obtain the media-type referenced by the
     * Content-Type header field.
     */
    inline void SetContentEncoding(Aws::String&& value) { m_contentEncoding = std::move(value); }

    /**
     * Specifies what content encodings have been applied to the object and thus what
     * decoding mechanisms must be applied to obtain the media-type referenced by the
     * Content-Type header field.
     */
    inline void SetContentEncoding(const char* value) { m_contentEncoding.assign(value); }

    /**
     * Specifies what content encodings have been applied to the object and thus what
     * decoding mechanisms must be applied to obtain the media-type referenced by the
     * Content-Type header field.
     */
    inline GetObjectResult& WithContentEncoding(const Aws::String& value) { SetContentEncoding(value); return *this;}

    /**
     * Specifies what content encodings have been applied to the object and thus what
     * decoding mechanisms must be applied to obtain the media-type referenced by the
     * Content-Type header field.
     */
    inline GetObjectResult& WithContentEncoding(Aws::String&& value) { SetContentEncoding(std::move(value)); return *this;}

    /**
     * Specifies what content encodings have been applied to the object and thus what
     * decoding mechanisms must be applied to obtain the media-type referenced by the
     * Content-Type header field.
     */
    inline GetObjectResult& WithContentEncoding(const char* value) { SetContentEncoding(value); return *this;}


    /**
     * The language the content is in.
     */
    inline const Aws::String& GetContentLanguage() const{ return m_contentLanguage; }

    /**
     * The language the content is in.
     */
    inline void SetContentLanguage(const Aws::String& value) { m_contentLanguage = value; }

    /**
     * The language the content is in.
     */
    inline void SetContentLanguage(Aws::String&& value) { m_contentLanguage = std::move(value); }

    /**
     * The language the content is in.
     */
    inline void SetContentLanguage(const char* value) { m_contentLanguage.assign(value); }

    /**
     * The language the content is in.
     */
    inline GetObjectResult& WithContentLanguage(const Aws::String& value) { SetContentLanguage(value); return *this;}

    /**
     * The language the content is in.
     */
    inline GetObjectResult& WithContentLanguage(Aws::String&& value) { SetContentLanguage(std::move(value)); return *this;}

    /**
     * The language the content is in.
     */
    inline GetObjectResult& WithContentLanguage(const char* value) { SetContentLanguage(value); return *this;}


    /**
     * The portion of the object returned in the response.
     */
    inline const Aws::String& GetContentRange() const{ return m_contentRange; }

    /**
     * The portion of the object returned in the response.
     */
    inline void SetContentRange(const Aws::String& value) { m_contentRange = value; }

    /**
     * The portion of the object returned in the response.
     */
    inline void SetContentRange(Aws::String&& value) { m_contentRange = std::move(value); }

    /**
     * The portion of the object returned in the response.
     */
    inline void SetContentRange(const char* value) { m_contentRange.assign(value); }

    /**
     * The portion of the object returned in the response.
     */
    inline GetObjectResult& WithContentRange(const Aws::String& value) { SetContentRange(value); return *this;}

    /**
     * The portion of the object returned in the response.
     */
    inline GetObjectResult& WithContentRange(Aws::String&& value) { SetContentRange(std::move(value)); return *this;}

    /**
     * The portion of the object returned in the response.
     */
    inline GetObjectResult& WithContentRange(const char* value) { SetContentRange(value); return *this;}


    /**
     * A standard MIME type describing the format of the object data.
     */
    inline const Aws::String& GetContentType() const{ return m_contentType; }

    /**
     * A standard MIME type describing the format of the object data.
     */
    inline void SetContentType(const Aws::String& value) { m_contentType = value; }

    /**
     * A standard MIME type describing the format of the object data.
     */
    inline void SetContentType(Aws::String&& value) { m_contentType = std::move(value); }

    /**
     * A standard MIME type describing the format of the object data.
     */
    inline void SetContentType(const char* value) { m_contentType.assign(value); }

    /**
     * A standard MIME type describing the format of the object data.
     */
    inline GetObjectResult& WithContentType(const Aws::String& value) { SetContentType(value); return *this;}

    /**
     * A standard MIME type describing the format of the object data.
     */
    inline GetObjectResult& WithContentType(Aws::String&& value) { SetContentType(std::move(value)); return *this;}

    /**
     * A standard MIME type describing the format of the object data.
     */
    inline GetObjectResult& WithContentType(const char* value) { SetContentType(value); return *this;}


    /**
     * The date and time at which the object is no longer cacheable.
     */
    inline const Aws::Utils::DateTime& GetExpires() const{ return m_expires; }

    /**
     * The date and time at which the object is no longer cacheable.
     */
    inline void SetExpires(const Aws::Utils::DateTime& value) { m_expires = value; }

    /**
     * The date and time at which the object is no longer cacheable.
     */
    inline void SetExpires(Aws::Utils::DateTime&& value) { m_expires = std::move(value); }

    /**
     * The date and time at which the object is no longer cacheable.
     */
    inline GetObjectResult& WithExpires(const Aws::Utils::DateTime& value) { SetExpires(value); return *this;}

    /**
     * The date and time at which the object is no longer cacheable.
     */
    inline GetObjectResult& WithExpires(Aws::Utils::DateTime&& value) { SetExpires(std::move(value)); return *this;}


    /**
     * If the bucket is configured as a website, redirects requests for this object to
     * another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata.
     */
    inline const Aws::String& GetWebsiteRedirectLocation() const{ return m_websiteRedirectLocation; }

    /**
     * If the bucket is configured as a website, redirects requests for this object to
     * another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata.
     */
    inline void SetWebsiteRedirectLocation(const Aws::String& value) { m_websiteRedirectLocation = value; }

    /**
     * If the bucket is configured as a website, redirects requests for this object to
     * another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata.
     */
    inline void SetWebsiteRedirectLocation(Aws::String&& value) { m_websiteRedirectLocation = std::move(value); }

    /**
     * If the bucket is configured as a website, redirects requests for this object to
     * another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata.
     */
    inline void SetWebsiteRedirectLocation(const char* value) { m_websiteRedirectLocation.assign(value); }

    /**
     * If the bucket is configured as a website, redirects requests for this object to
     * another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata.
     */
    inline GetObjectResult& WithWebsiteRedirectLocation(const Aws::String& value) { SetWebsiteRedirectLocation(value); return *this;}

    /**
     * If the bucket is configured as a website, redirects requests for this object to
     * another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata.
     */
    inline GetObjectResult& WithWebsiteRedirectLocation(Aws::String&& value) { SetWebsiteRedirectLocation(std::move(value)); return *this;}

    /**
     * If the bucket is configured as a website, redirects requests for this object to
     * another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata.
     */
    inline GetObjectResult& WithWebsiteRedirectLocation(const char* value) { SetWebsiteRedirectLocation(value); return *this;}


    /**
     * The Server-side encryption algorithm used when storing this object in S3 (e.g.,
     * AES256, aws:kms).
     */
    inline const ServerSideEncryption& GetServerSideEncryption() const{ return m_serverSideEncryption; }

    /**
     * The Server-side encryption algorithm used when storing this object in S3 (e.g.,
     * AES256, aws:kms).
     */
    inline void SetServerSideEncryption(const ServerSideEncryption& value) { m_serverSideEncryption = value; }

    /**
     * The Server-side encryption algorithm used when storing this object in S3 (e.g.,
     * AES256, aws:kms).
     */
    inline void SetServerSideEncryption(ServerSideEncryption&& value) { m_serverSideEncryption = std::move(value); }

    /**
     * The Server-side encryption algorithm used when storing this object in S3 (e.g.,
     * AES256, aws:kms).
     */
    inline GetObjectResult& WithServerSideEncryption(const ServerSideEncryption& value) { SetServerSideEncryption(value); return *this;}

    /**
     * The Server-side encryption algorithm used when storing this object in S3 (e.g.,
     * AES256, aws:kms).
     */
    inline GetObjectResult& WithServerSideEncryption(ServerSideEncryption&& value) { SetServerSideEncryption(std::move(value)); return *this;}


    /**
     * A map of metadata to store with the object in S3.
     */
    inline const Aws::Map<Aws::String, Aws::String>& GetMetadata() const{ return m_metadata; }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline void SetMetadata(const Aws::Map<Aws::String, Aws::String>& value) { m_metadata = value; }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline void SetMetadata(Aws::Map<Aws::String, Aws::String>&& value) { m_metadata = std::move(value); }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline GetObjectResult& WithMetadata(const Aws::Map<Aws::String, Aws::String>& value) { SetMetadata(value); return *this;}

    /**
     * A map of metadata to store with the object in S3.
     */
    inline GetObjectResult& WithMetadata(Aws::Map<Aws::String, Aws::String>&& value) { SetMetadata(std::move(value)); return *this;}

    /**
     * A map of metadata to store with the object in S3.
     */
    inline GetObjectResult& AddMetadata(const Aws::String& key, const Aws::String& value) { m_metadata.emplace(key, value); return *this; }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline GetObjectResult& AddMetadata(Aws::String&& key, const Aws::String& value) { m_metadata.emplace(std::move(key), value); return *this; }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline GetObjectResult& AddMetadata(const Aws::String& key, Aws::String&& value) { m_metadata.emplace(key, std::move(value)); return *this; }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline GetObjectResult& AddMetadata(Aws::String&& key, Aws::String&& value) { m_metadata.emplace(std::move(key), std::move(value)); return *this; }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline GetObjectResult& AddMetadata(const char* key, Aws::String&& value) { m_metadata.emplace(key, std::move(value)); return *this; }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline GetObjectResult& AddMetadata(Aws::String&& key, const char* value) { m_metadata.emplace(std::move(key), value); return *this; }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline GetObjectResult& AddMetadata(const char* key, const char* value) { m_metadata.emplace(key, value); return *this; }


    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header confirming the encryption algorithm used.
     */
    inline const Aws::String& GetSSECustomerAlgorithm() const{ return m_sSECustomerAlgorithm; }

    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header confirming the encryption algorithm used.
     */
    inline void SetSSECustomerAlgorithm(const Aws::String& value) { m_sSECustomerAlgorithm = value; }

    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header confirming the encryption algorithm used.
     */
    inline void SetSSECustomerAlgorithm(Aws::String&& value) { m_sSECustomerAlgorithm = std::move(value); }

    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header confirming the encryption algorithm used.
     */
    inline void SetSSECustomerAlgorithm(const char* value) { m_sSECustomerAlgorithm.assign(value); }

    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header confirming the encryption algorithm used.
     */
    inline GetObjectResult& WithSSECustomerAlgorithm(const Aws::String& value) { SetSSECustomerAlgorithm(value); return *this;}

    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header confirming the encryption algorithm used.
     */
    inline GetObjectResult& WithSSECustomerAlgorithm(Aws::String&& value) { SetSSECustomerAlgorithm(std::move(value)); return *this;}

    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header confirming the encryption algorithm used.
     */
    inline GetObjectResult& WithSSECustomerAlgorithm(const char* value) { SetSSECustomerAlgorithm(value); return *this;}


    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header to provide round trip message integrity
     * verification of the customer-provided encryption key.
     */
    inline const Aws::String& GetSSECustomerKeyMD5() const{ return m_sSECustomerKeyMD5; }

    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header to provide round trip message integrity
     * verification of the customer-provided encryption key.
     */
    inline void SetSSECustomerKeyMD5(const Aws::String& value) { m_sSECustomerKeyMD5 = value; }

    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header to provide round trip message integrity
     * verification of the customer-provided encryption key.
     */
    inline void SetSSECustomerKeyMD5(Aws::String&& value) { m_sSECustomerKeyMD5 = std::move(value); }

    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header to provide round trip message integrity
     * verification of the customer-provided encryption key.
     */
    inline void SetSSECustomerKeyMD5(const char* value) { m_sSECustomerKeyMD5.assign(value); }

    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header to provide round trip message integrity
     * verification of the customer-provided encryption key.
     */
    inline GetObjectResult& WithSSECustomerKeyMD5(const Aws::String& value) { SetSSECustomerKeyMD5(value); return *this;}

    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header to provide round trip message integrity
     * verification of the customer-provided encryption key.
     */
    inline GetObjectResult& WithSSECustomerKeyMD5(Aws::String&& value) { SetSSECustomerKeyMD5(std::move(value)); return *this;}

    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header to provide round trip message integrity
     * verification of the customer-provided encryption key.
     */
    inline GetObjectResult& WithSSECustomerKeyMD5(const char* value) { SetSSECustomerKeyMD5(value); return *this;}


    /**
     * If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.
     */
    inline const Aws::String& GetSSEKMSKeyId() const{ return m_sSEKMSKeyId; }

    /**
     * If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.
     */
    inline void SetSSEKMSKeyId(const Aws::String& value) { m_sSEKMSKeyId = value; }

    /**
     * If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.
     */
    inline void SetSSEKMSKeyId(Aws::String&& value) { m_sSEKMSKeyId = std::move(value); }

    /**
     * If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.
     */
    inline void SetSSEKMSKeyId(const char* value) { m_sSEKMSKeyId.assign(value); }

    /**
     * If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.
     */
    inline GetObjectResult& WithSSEKMSKeyId(const Aws::String& value) { SetSSEKMSKeyId(value); return *this;}

    /**
     * If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.
     */
    inline GetObjectResult& WithSSEKMSKeyId(Aws::String&& value) { SetSSEKMSKeyId(std::move(value)); return *this;}

    /**
     * If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.
     */
    inline GetObjectResult& WithSSEKMSKeyId(const char* value) { SetSSEKMSKeyId(value); return *this;}


    
    inline const StorageClass& GetStorageClass() const{ return m_storageClass; }

    
    inline void SetStorageClass(const StorageClass& value) { m_storageClass = value; }

    
    inline void SetStorageClass(StorageClass&& value) { m_storageClass = std::move(value); }

    
    inline GetObjectResult& WithStorageClass(const StorageClass& value) { SetStorageClass(value); return *this;}

    
    inline GetObjectResult& WithStorageClass(StorageClass&& value) { SetStorageClass(std::move(value)); return *this;}


    
    inline const RequestCharged& GetRequestCharged() const{ return m_requestCharged; }

    
    inline void SetRequestCharged(const RequestCharged& value) { m_requestCharged = value; }

    
    inline void SetRequestCharged(RequestCharged&& value) { m_requestCharged = std::move(value); }

    
    inline GetObjectResult& WithRequestCharged(const RequestCharged& value) { SetRequestCharged(value); return *this;}

    
    inline GetObjectResult& WithRequestCharged(RequestCharged&& value) { SetRequestCharged(std::move(value)); return *this;}


    
    inline const ReplicationStatus& GetReplicationStatus() const{ return m_replicationStatus; }

    
    inline void SetReplicationStatus(const ReplicationStatus& value) { m_replicationStatus = value; }

    
    inline void SetReplicationStatus(ReplicationStatus&& value) { m_replicationStatus = std::move(value); }

    
    inline GetObjectResult& WithReplicationStatus(const ReplicationStatus& value) { SetReplicationStatus(value); return *this;}

    
    inline GetObjectResult& WithReplicationStatus(ReplicationStatus&& value) { SetReplicationStatus(std::move(value)); return *this;}


    /**
     * The count of parts this object has.
     */
    inline int GetPartsCount() const{ return m_partsCount; }

    /**
     * The count of parts this object has.
     */
    inline void SetPartsCount(int value) { m_partsCount = value; }

    /**
     * The count of parts this object has.
     */
    inline GetObjectResult& WithPartsCount(int value) { SetPartsCount(value); return *this;}


    /**
     * The number of tags, if any, on the object.
     */
    inline int GetTagCount() const{ return m_tagCount; }

    /**
     * The number of tags, if any, on the object.
     */
    inline void SetTagCount(int value) { m_tagCount = value; }

    /**
     * The number of tags, if any, on the object.
     */
    inline GetObjectResult& WithTagCount(int value) { SetTagCount(value); return *this;}


    
    inline const Aws::String& GetId2() const{ return m_id2; }

    
    inline void SetId2(const Aws::String& value) { m_id2 = value; }

    
    inline void SetId2(Aws::String&& value) { m_id2 = std::move(value); }

    
    inline void SetId2(const char* value) { m_id2.assign(value); }

    
    inline GetObjectResult& WithId2(const Aws::String& value) { SetId2(value); return *this;}

    
    inline GetObjectResult& WithId2(Aws::String&& value) { SetId2(std::move(value)); return *this;}

    
    inline GetObjectResult& WithId2(const char* value) { SetId2(value); return *this;}


    
    inline const Aws::String& GetRequestId() const{ return m_requestId; }

    
    inline void SetRequestId(const Aws::String& value) { m_requestId = value; }

    
    inline void SetRequestId(Aws::String&& value) { m_requestId = std::move(value); }

    
    inline void SetRequestId(const char* value) { m_requestId.assign(value); }

    
    inline GetObjectResult& WithRequestId(const Aws::String& value) { SetRequestId(value); return *this;}

    
    inline GetObjectResult& WithRequestId(Aws::String&& value) { SetRequestId(std::move(value)); return *this;}

    
    inline GetObjectResult& WithRequestId(const char* value) { SetRequestId(value); return *this;}

  private:

  Aws::Utils::Stream::ResponseStream m_body;

    bool m_deleteMarker;

    Aws::String m_acceptRanges;

    Aws::String m_expiration;

    Aws::String m_restore;

    Aws::Utils::DateTime m_lastModified;

    long long m_contentLength;

    Aws::String m_eTag;

    int m_missingMeta;

    Aws::String m_versionId;

    Aws::String m_cacheControl;

    Aws::String m_contentDisposition;

    Aws::String m_contentEncoding;

    Aws::String m_contentLanguage;

    Aws::String m_contentRange;

    Aws::String m_contentType;

    Aws::Utils::DateTime m_expires;

    Aws::String m_websiteRedirectLocation;

    ServerSideEncryption m_serverSideEncryption;

    Aws::Map<Aws::String, Aws::String> m_metadata;

    Aws::String m_sSECustomerAlgorithm;

    Aws::String m_sSECustomerKeyMD5;

    Aws::String m_sSEKMSKeyId;

    StorageClass m_storageClass;

    RequestCharged m_requestCharged;

    ReplicationStatus m_replicationStatus;

    int m_partsCount;

    int m_tagCount;

    Aws::String m_id2;

    Aws::String m_requestId;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
