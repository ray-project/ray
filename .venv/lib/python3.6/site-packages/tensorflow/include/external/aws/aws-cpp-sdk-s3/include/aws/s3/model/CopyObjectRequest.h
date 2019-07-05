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
#include <aws/s3/model/ObjectCannedACL.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/s3/model/MetadataDirective.h>
#include <aws/s3/model/TaggingDirective.h>
#include <aws/s3/model/ServerSideEncryption.h>
#include <aws/s3/model/StorageClass.h>
#include <aws/s3/model/RequestPayer.h>
#include <utility>

namespace Aws
{
namespace S3
{
namespace Model
{

  /**
   */
  class AWS_S3_API CopyObjectRequest : public S3Request
  {
  public:
    CopyObjectRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "CopyObject"; }

    Aws::String SerializePayload() const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    /**
     * The canned ACL to apply to the object.
     */
    inline const ObjectCannedACL& GetACL() const{ return m_aCL; }

    /**
     * The canned ACL to apply to the object.
     */
    inline void SetACL(const ObjectCannedACL& value) { m_aCLHasBeenSet = true; m_aCL = value; }

    /**
     * The canned ACL to apply to the object.
     */
    inline void SetACL(ObjectCannedACL&& value) { m_aCLHasBeenSet = true; m_aCL = std::move(value); }

    /**
     * The canned ACL to apply to the object.
     */
    inline CopyObjectRequest& WithACL(const ObjectCannedACL& value) { SetACL(value); return *this;}

    /**
     * The canned ACL to apply to the object.
     */
    inline CopyObjectRequest& WithACL(ObjectCannedACL&& value) { SetACL(std::move(value)); return *this;}


    
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    
    inline CopyObjectRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    
    inline CopyObjectRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    
    inline CopyObjectRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * Specifies caching behavior along the request/reply chain.
     */
    inline const Aws::String& GetCacheControl() const{ return m_cacheControl; }

    /**
     * Specifies caching behavior along the request/reply chain.
     */
    inline void SetCacheControl(const Aws::String& value) { m_cacheControlHasBeenSet = true; m_cacheControl = value; }

    /**
     * Specifies caching behavior along the request/reply chain.
     */
    inline void SetCacheControl(Aws::String&& value) { m_cacheControlHasBeenSet = true; m_cacheControl = std::move(value); }

    /**
     * Specifies caching behavior along the request/reply chain.
     */
    inline void SetCacheControl(const char* value) { m_cacheControlHasBeenSet = true; m_cacheControl.assign(value); }

    /**
     * Specifies caching behavior along the request/reply chain.
     */
    inline CopyObjectRequest& WithCacheControl(const Aws::String& value) { SetCacheControl(value); return *this;}

    /**
     * Specifies caching behavior along the request/reply chain.
     */
    inline CopyObjectRequest& WithCacheControl(Aws::String&& value) { SetCacheControl(std::move(value)); return *this;}

    /**
     * Specifies caching behavior along the request/reply chain.
     */
    inline CopyObjectRequest& WithCacheControl(const char* value) { SetCacheControl(value); return *this;}


    /**
     * Specifies presentational information for the object.
     */
    inline const Aws::String& GetContentDisposition() const{ return m_contentDisposition; }

    /**
     * Specifies presentational information for the object.
     */
    inline void SetContentDisposition(const Aws::String& value) { m_contentDispositionHasBeenSet = true; m_contentDisposition = value; }

    /**
     * Specifies presentational information for the object.
     */
    inline void SetContentDisposition(Aws::String&& value) { m_contentDispositionHasBeenSet = true; m_contentDisposition = std::move(value); }

    /**
     * Specifies presentational information for the object.
     */
    inline void SetContentDisposition(const char* value) { m_contentDispositionHasBeenSet = true; m_contentDisposition.assign(value); }

    /**
     * Specifies presentational information for the object.
     */
    inline CopyObjectRequest& WithContentDisposition(const Aws::String& value) { SetContentDisposition(value); return *this;}

    /**
     * Specifies presentational information for the object.
     */
    inline CopyObjectRequest& WithContentDisposition(Aws::String&& value) { SetContentDisposition(std::move(value)); return *this;}

    /**
     * Specifies presentational information for the object.
     */
    inline CopyObjectRequest& WithContentDisposition(const char* value) { SetContentDisposition(value); return *this;}


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
    inline void SetContentEncoding(const Aws::String& value) { m_contentEncodingHasBeenSet = true; m_contentEncoding = value; }

    /**
     * Specifies what content encodings have been applied to the object and thus what
     * decoding mechanisms must be applied to obtain the media-type referenced by the
     * Content-Type header field.
     */
    inline void SetContentEncoding(Aws::String&& value) { m_contentEncodingHasBeenSet = true; m_contentEncoding = std::move(value); }

    /**
     * Specifies what content encodings have been applied to the object and thus what
     * decoding mechanisms must be applied to obtain the media-type referenced by the
     * Content-Type header field.
     */
    inline void SetContentEncoding(const char* value) { m_contentEncodingHasBeenSet = true; m_contentEncoding.assign(value); }

    /**
     * Specifies what content encodings have been applied to the object and thus what
     * decoding mechanisms must be applied to obtain the media-type referenced by the
     * Content-Type header field.
     */
    inline CopyObjectRequest& WithContentEncoding(const Aws::String& value) { SetContentEncoding(value); return *this;}

    /**
     * Specifies what content encodings have been applied to the object and thus what
     * decoding mechanisms must be applied to obtain the media-type referenced by the
     * Content-Type header field.
     */
    inline CopyObjectRequest& WithContentEncoding(Aws::String&& value) { SetContentEncoding(std::move(value)); return *this;}

    /**
     * Specifies what content encodings have been applied to the object and thus what
     * decoding mechanisms must be applied to obtain the media-type referenced by the
     * Content-Type header field.
     */
    inline CopyObjectRequest& WithContentEncoding(const char* value) { SetContentEncoding(value); return *this;}


    /**
     * The language the content is in.
     */
    inline const Aws::String& GetContentLanguage() const{ return m_contentLanguage; }

    /**
     * The language the content is in.
     */
    inline void SetContentLanguage(const Aws::String& value) { m_contentLanguageHasBeenSet = true; m_contentLanguage = value; }

    /**
     * The language the content is in.
     */
    inline void SetContentLanguage(Aws::String&& value) { m_contentLanguageHasBeenSet = true; m_contentLanguage = std::move(value); }

    /**
     * The language the content is in.
     */
    inline void SetContentLanguage(const char* value) { m_contentLanguageHasBeenSet = true; m_contentLanguage.assign(value); }

    /**
     * The language the content is in.
     */
    inline CopyObjectRequest& WithContentLanguage(const Aws::String& value) { SetContentLanguage(value); return *this;}

    /**
     * The language the content is in.
     */
    inline CopyObjectRequest& WithContentLanguage(Aws::String&& value) { SetContentLanguage(std::move(value)); return *this;}

    /**
     * The language the content is in.
     */
    inline CopyObjectRequest& WithContentLanguage(const char* value) { SetContentLanguage(value); return *this;}


    /**
     * A standard MIME type describing the format of the object data.
     */
    inline const Aws::String& GetContentType() const{ return m_contentType; }

    /**
     * A standard MIME type describing the format of the object data.
     */
    inline void SetContentType(const Aws::String& value) { m_contentTypeHasBeenSet = true; m_contentType = value; }

    /**
     * A standard MIME type describing the format of the object data.
     */
    inline void SetContentType(Aws::String&& value) { m_contentTypeHasBeenSet = true; m_contentType = std::move(value); }

    /**
     * A standard MIME type describing the format of the object data.
     */
    inline void SetContentType(const char* value) { m_contentTypeHasBeenSet = true; m_contentType.assign(value); }

    /**
     * A standard MIME type describing the format of the object data.
     */
    inline CopyObjectRequest& WithContentType(const Aws::String& value) { SetContentType(value); return *this;}

    /**
     * A standard MIME type describing the format of the object data.
     */
    inline CopyObjectRequest& WithContentType(Aws::String&& value) { SetContentType(std::move(value)); return *this;}

    /**
     * A standard MIME type describing the format of the object data.
     */
    inline CopyObjectRequest& WithContentType(const char* value) { SetContentType(value); return *this;}


    /**
     * The name of the source bucket and key name of the source object, separated by a
     * slash (/). Must be URL-encoded.
     */
    inline const Aws::String& GetCopySource() const{ return m_copySource; }

    /**
     * The name of the source bucket and key name of the source object, separated by a
     * slash (/). Must be URL-encoded.
     */
    inline void SetCopySource(const Aws::String& value) { m_copySourceHasBeenSet = true; m_copySource = value; }

    /**
     * The name of the source bucket and key name of the source object, separated by a
     * slash (/). Must be URL-encoded.
     */
    inline void SetCopySource(Aws::String&& value) { m_copySourceHasBeenSet = true; m_copySource = std::move(value); }

    /**
     * The name of the source bucket and key name of the source object, separated by a
     * slash (/). Must be URL-encoded.
     */
    inline void SetCopySource(const char* value) { m_copySourceHasBeenSet = true; m_copySource.assign(value); }

    /**
     * The name of the source bucket and key name of the source object, separated by a
     * slash (/). Must be URL-encoded.
     */
    inline CopyObjectRequest& WithCopySource(const Aws::String& value) { SetCopySource(value); return *this;}

    /**
     * The name of the source bucket and key name of the source object, separated by a
     * slash (/). Must be URL-encoded.
     */
    inline CopyObjectRequest& WithCopySource(Aws::String&& value) { SetCopySource(std::move(value)); return *this;}

    /**
     * The name of the source bucket and key name of the source object, separated by a
     * slash (/). Must be URL-encoded.
     */
    inline CopyObjectRequest& WithCopySource(const char* value) { SetCopySource(value); return *this;}


    /**
     * Copies the object if its entity tag (ETag) matches the specified tag.
     */
    inline const Aws::String& GetCopySourceIfMatch() const{ return m_copySourceIfMatch; }

    /**
     * Copies the object if its entity tag (ETag) matches the specified tag.
     */
    inline void SetCopySourceIfMatch(const Aws::String& value) { m_copySourceIfMatchHasBeenSet = true; m_copySourceIfMatch = value; }

    /**
     * Copies the object if its entity tag (ETag) matches the specified tag.
     */
    inline void SetCopySourceIfMatch(Aws::String&& value) { m_copySourceIfMatchHasBeenSet = true; m_copySourceIfMatch = std::move(value); }

    /**
     * Copies the object if its entity tag (ETag) matches the specified tag.
     */
    inline void SetCopySourceIfMatch(const char* value) { m_copySourceIfMatchHasBeenSet = true; m_copySourceIfMatch.assign(value); }

    /**
     * Copies the object if its entity tag (ETag) matches the specified tag.
     */
    inline CopyObjectRequest& WithCopySourceIfMatch(const Aws::String& value) { SetCopySourceIfMatch(value); return *this;}

    /**
     * Copies the object if its entity tag (ETag) matches the specified tag.
     */
    inline CopyObjectRequest& WithCopySourceIfMatch(Aws::String&& value) { SetCopySourceIfMatch(std::move(value)); return *this;}

    /**
     * Copies the object if its entity tag (ETag) matches the specified tag.
     */
    inline CopyObjectRequest& WithCopySourceIfMatch(const char* value) { SetCopySourceIfMatch(value); return *this;}


    /**
     * Copies the object if it has been modified since the specified time.
     */
    inline const Aws::Utils::DateTime& GetCopySourceIfModifiedSince() const{ return m_copySourceIfModifiedSince; }

    /**
     * Copies the object if it has been modified since the specified time.
     */
    inline void SetCopySourceIfModifiedSince(const Aws::Utils::DateTime& value) { m_copySourceIfModifiedSinceHasBeenSet = true; m_copySourceIfModifiedSince = value; }

    /**
     * Copies the object if it has been modified since the specified time.
     */
    inline void SetCopySourceIfModifiedSince(Aws::Utils::DateTime&& value) { m_copySourceIfModifiedSinceHasBeenSet = true; m_copySourceIfModifiedSince = std::move(value); }

    /**
     * Copies the object if it has been modified since the specified time.
     */
    inline CopyObjectRequest& WithCopySourceIfModifiedSince(const Aws::Utils::DateTime& value) { SetCopySourceIfModifiedSince(value); return *this;}

    /**
     * Copies the object if it has been modified since the specified time.
     */
    inline CopyObjectRequest& WithCopySourceIfModifiedSince(Aws::Utils::DateTime&& value) { SetCopySourceIfModifiedSince(std::move(value)); return *this;}


    /**
     * Copies the object if its entity tag (ETag) is different than the specified ETag.
     */
    inline const Aws::String& GetCopySourceIfNoneMatch() const{ return m_copySourceIfNoneMatch; }

    /**
     * Copies the object if its entity tag (ETag) is different than the specified ETag.
     */
    inline void SetCopySourceIfNoneMatch(const Aws::String& value) { m_copySourceIfNoneMatchHasBeenSet = true; m_copySourceIfNoneMatch = value; }

    /**
     * Copies the object if its entity tag (ETag) is different than the specified ETag.
     */
    inline void SetCopySourceIfNoneMatch(Aws::String&& value) { m_copySourceIfNoneMatchHasBeenSet = true; m_copySourceIfNoneMatch = std::move(value); }

    /**
     * Copies the object if its entity tag (ETag) is different than the specified ETag.
     */
    inline void SetCopySourceIfNoneMatch(const char* value) { m_copySourceIfNoneMatchHasBeenSet = true; m_copySourceIfNoneMatch.assign(value); }

    /**
     * Copies the object if its entity tag (ETag) is different than the specified ETag.
     */
    inline CopyObjectRequest& WithCopySourceIfNoneMatch(const Aws::String& value) { SetCopySourceIfNoneMatch(value); return *this;}

    /**
     * Copies the object if its entity tag (ETag) is different than the specified ETag.
     */
    inline CopyObjectRequest& WithCopySourceIfNoneMatch(Aws::String&& value) { SetCopySourceIfNoneMatch(std::move(value)); return *this;}

    /**
     * Copies the object if its entity tag (ETag) is different than the specified ETag.
     */
    inline CopyObjectRequest& WithCopySourceIfNoneMatch(const char* value) { SetCopySourceIfNoneMatch(value); return *this;}


    /**
     * Copies the object if it hasn't been modified since the specified time.
     */
    inline const Aws::Utils::DateTime& GetCopySourceIfUnmodifiedSince() const{ return m_copySourceIfUnmodifiedSince; }

    /**
     * Copies the object if it hasn't been modified since the specified time.
     */
    inline void SetCopySourceIfUnmodifiedSince(const Aws::Utils::DateTime& value) { m_copySourceIfUnmodifiedSinceHasBeenSet = true; m_copySourceIfUnmodifiedSince = value; }

    /**
     * Copies the object if it hasn't been modified since the specified time.
     */
    inline void SetCopySourceIfUnmodifiedSince(Aws::Utils::DateTime&& value) { m_copySourceIfUnmodifiedSinceHasBeenSet = true; m_copySourceIfUnmodifiedSince = std::move(value); }

    /**
     * Copies the object if it hasn't been modified since the specified time.
     */
    inline CopyObjectRequest& WithCopySourceIfUnmodifiedSince(const Aws::Utils::DateTime& value) { SetCopySourceIfUnmodifiedSince(value); return *this;}

    /**
     * Copies the object if it hasn't been modified since the specified time.
     */
    inline CopyObjectRequest& WithCopySourceIfUnmodifiedSince(Aws::Utils::DateTime&& value) { SetCopySourceIfUnmodifiedSince(std::move(value)); return *this;}


    /**
     * The date and time at which the object is no longer cacheable.
     */
    inline const Aws::Utils::DateTime& GetExpires() const{ return m_expires; }

    /**
     * The date and time at which the object is no longer cacheable.
     */
    inline void SetExpires(const Aws::Utils::DateTime& value) { m_expiresHasBeenSet = true; m_expires = value; }

    /**
     * The date and time at which the object is no longer cacheable.
     */
    inline void SetExpires(Aws::Utils::DateTime&& value) { m_expiresHasBeenSet = true; m_expires = std::move(value); }

    /**
     * The date and time at which the object is no longer cacheable.
     */
    inline CopyObjectRequest& WithExpires(const Aws::Utils::DateTime& value) { SetExpires(value); return *this;}

    /**
     * The date and time at which the object is no longer cacheable.
     */
    inline CopyObjectRequest& WithExpires(Aws::Utils::DateTime&& value) { SetExpires(std::move(value)); return *this;}


    /**
     * Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.
     */
    inline const Aws::String& GetGrantFullControl() const{ return m_grantFullControl; }

    /**
     * Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.
     */
    inline void SetGrantFullControl(const Aws::String& value) { m_grantFullControlHasBeenSet = true; m_grantFullControl = value; }

    /**
     * Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.
     */
    inline void SetGrantFullControl(Aws::String&& value) { m_grantFullControlHasBeenSet = true; m_grantFullControl = std::move(value); }

    /**
     * Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.
     */
    inline void SetGrantFullControl(const char* value) { m_grantFullControlHasBeenSet = true; m_grantFullControl.assign(value); }

    /**
     * Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.
     */
    inline CopyObjectRequest& WithGrantFullControl(const Aws::String& value) { SetGrantFullControl(value); return *this;}

    /**
     * Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.
     */
    inline CopyObjectRequest& WithGrantFullControl(Aws::String&& value) { SetGrantFullControl(std::move(value)); return *this;}

    /**
     * Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.
     */
    inline CopyObjectRequest& WithGrantFullControl(const char* value) { SetGrantFullControl(value); return *this;}


    /**
     * Allows grantee to read the object data and its metadata.
     */
    inline const Aws::String& GetGrantRead() const{ return m_grantRead; }

    /**
     * Allows grantee to read the object data and its metadata.
     */
    inline void SetGrantRead(const Aws::String& value) { m_grantReadHasBeenSet = true; m_grantRead = value; }

    /**
     * Allows grantee to read the object data and its metadata.
     */
    inline void SetGrantRead(Aws::String&& value) { m_grantReadHasBeenSet = true; m_grantRead = std::move(value); }

    /**
     * Allows grantee to read the object data and its metadata.
     */
    inline void SetGrantRead(const char* value) { m_grantReadHasBeenSet = true; m_grantRead.assign(value); }

    /**
     * Allows grantee to read the object data and its metadata.
     */
    inline CopyObjectRequest& WithGrantRead(const Aws::String& value) { SetGrantRead(value); return *this;}

    /**
     * Allows grantee to read the object data and its metadata.
     */
    inline CopyObjectRequest& WithGrantRead(Aws::String&& value) { SetGrantRead(std::move(value)); return *this;}

    /**
     * Allows grantee to read the object data and its metadata.
     */
    inline CopyObjectRequest& WithGrantRead(const char* value) { SetGrantRead(value); return *this;}


    /**
     * Allows grantee to read the object ACL.
     */
    inline const Aws::String& GetGrantReadACP() const{ return m_grantReadACP; }

    /**
     * Allows grantee to read the object ACL.
     */
    inline void SetGrantReadACP(const Aws::String& value) { m_grantReadACPHasBeenSet = true; m_grantReadACP = value; }

    /**
     * Allows grantee to read the object ACL.
     */
    inline void SetGrantReadACP(Aws::String&& value) { m_grantReadACPHasBeenSet = true; m_grantReadACP = std::move(value); }

    /**
     * Allows grantee to read the object ACL.
     */
    inline void SetGrantReadACP(const char* value) { m_grantReadACPHasBeenSet = true; m_grantReadACP.assign(value); }

    /**
     * Allows grantee to read the object ACL.
     */
    inline CopyObjectRequest& WithGrantReadACP(const Aws::String& value) { SetGrantReadACP(value); return *this;}

    /**
     * Allows grantee to read the object ACL.
     */
    inline CopyObjectRequest& WithGrantReadACP(Aws::String&& value) { SetGrantReadACP(std::move(value)); return *this;}

    /**
     * Allows grantee to read the object ACL.
     */
    inline CopyObjectRequest& WithGrantReadACP(const char* value) { SetGrantReadACP(value); return *this;}


    /**
     * Allows grantee to write the ACL for the applicable object.
     */
    inline const Aws::String& GetGrantWriteACP() const{ return m_grantWriteACP; }

    /**
     * Allows grantee to write the ACL for the applicable object.
     */
    inline void SetGrantWriteACP(const Aws::String& value) { m_grantWriteACPHasBeenSet = true; m_grantWriteACP = value; }

    /**
     * Allows grantee to write the ACL for the applicable object.
     */
    inline void SetGrantWriteACP(Aws::String&& value) { m_grantWriteACPHasBeenSet = true; m_grantWriteACP = std::move(value); }

    /**
     * Allows grantee to write the ACL for the applicable object.
     */
    inline void SetGrantWriteACP(const char* value) { m_grantWriteACPHasBeenSet = true; m_grantWriteACP.assign(value); }

    /**
     * Allows grantee to write the ACL for the applicable object.
     */
    inline CopyObjectRequest& WithGrantWriteACP(const Aws::String& value) { SetGrantWriteACP(value); return *this;}

    /**
     * Allows grantee to write the ACL for the applicable object.
     */
    inline CopyObjectRequest& WithGrantWriteACP(Aws::String&& value) { SetGrantWriteACP(std::move(value)); return *this;}

    /**
     * Allows grantee to write the ACL for the applicable object.
     */
    inline CopyObjectRequest& WithGrantWriteACP(const char* value) { SetGrantWriteACP(value); return *this;}


    
    inline const Aws::String& GetKey() const{ return m_key; }

    
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    
    inline CopyObjectRequest& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    
    inline CopyObjectRequest& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    
    inline CopyObjectRequest& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * A map of metadata to store with the object in S3.
     */
    inline const Aws::Map<Aws::String, Aws::String>& GetMetadata() const{ return m_metadata; }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline void SetMetadata(const Aws::Map<Aws::String, Aws::String>& value) { m_metadataHasBeenSet = true; m_metadata = value; }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline void SetMetadata(Aws::Map<Aws::String, Aws::String>&& value) { m_metadataHasBeenSet = true; m_metadata = std::move(value); }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline CopyObjectRequest& WithMetadata(const Aws::Map<Aws::String, Aws::String>& value) { SetMetadata(value); return *this;}

    /**
     * A map of metadata to store with the object in S3.
     */
    inline CopyObjectRequest& WithMetadata(Aws::Map<Aws::String, Aws::String>&& value) { SetMetadata(std::move(value)); return *this;}

    /**
     * A map of metadata to store with the object in S3.
     */
    inline CopyObjectRequest& AddMetadata(const Aws::String& key, const Aws::String& value) { m_metadataHasBeenSet = true; m_metadata.emplace(key, value); return *this; }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline CopyObjectRequest& AddMetadata(Aws::String&& key, const Aws::String& value) { m_metadataHasBeenSet = true; m_metadata.emplace(std::move(key), value); return *this; }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline CopyObjectRequest& AddMetadata(const Aws::String& key, Aws::String&& value) { m_metadataHasBeenSet = true; m_metadata.emplace(key, std::move(value)); return *this; }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline CopyObjectRequest& AddMetadata(Aws::String&& key, Aws::String&& value) { m_metadataHasBeenSet = true; m_metadata.emplace(std::move(key), std::move(value)); return *this; }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline CopyObjectRequest& AddMetadata(const char* key, Aws::String&& value) { m_metadataHasBeenSet = true; m_metadata.emplace(key, std::move(value)); return *this; }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline CopyObjectRequest& AddMetadata(Aws::String&& key, const char* value) { m_metadataHasBeenSet = true; m_metadata.emplace(std::move(key), value); return *this; }

    /**
     * A map of metadata to store with the object in S3.
     */
    inline CopyObjectRequest& AddMetadata(const char* key, const char* value) { m_metadataHasBeenSet = true; m_metadata.emplace(key, value); return *this; }


    /**
     * Specifies whether the metadata is copied from the source object or replaced with
     * metadata provided in the request.
     */
    inline const MetadataDirective& GetMetadataDirective() const{ return m_metadataDirective; }

    /**
     * Specifies whether the metadata is copied from the source object or replaced with
     * metadata provided in the request.
     */
    inline void SetMetadataDirective(const MetadataDirective& value) { m_metadataDirectiveHasBeenSet = true; m_metadataDirective = value; }

    /**
     * Specifies whether the metadata is copied from the source object or replaced with
     * metadata provided in the request.
     */
    inline void SetMetadataDirective(MetadataDirective&& value) { m_metadataDirectiveHasBeenSet = true; m_metadataDirective = std::move(value); }

    /**
     * Specifies whether the metadata is copied from the source object or replaced with
     * metadata provided in the request.
     */
    inline CopyObjectRequest& WithMetadataDirective(const MetadataDirective& value) { SetMetadataDirective(value); return *this;}

    /**
     * Specifies whether the metadata is copied from the source object or replaced with
     * metadata provided in the request.
     */
    inline CopyObjectRequest& WithMetadataDirective(MetadataDirective&& value) { SetMetadataDirective(std::move(value)); return *this;}


    /**
     * Specifies whether the object tag-set are copied from the source object or
     * replaced with tag-set provided in the request.
     */
    inline const TaggingDirective& GetTaggingDirective() const{ return m_taggingDirective; }

    /**
     * Specifies whether the object tag-set are copied from the source object or
     * replaced with tag-set provided in the request.
     */
    inline void SetTaggingDirective(const TaggingDirective& value) { m_taggingDirectiveHasBeenSet = true; m_taggingDirective = value; }

    /**
     * Specifies whether the object tag-set are copied from the source object or
     * replaced with tag-set provided in the request.
     */
    inline void SetTaggingDirective(TaggingDirective&& value) { m_taggingDirectiveHasBeenSet = true; m_taggingDirective = std::move(value); }

    /**
     * Specifies whether the object tag-set are copied from the source object or
     * replaced with tag-set provided in the request.
     */
    inline CopyObjectRequest& WithTaggingDirective(const TaggingDirective& value) { SetTaggingDirective(value); return *this;}

    /**
     * Specifies whether the object tag-set are copied from the source object or
     * replaced with tag-set provided in the request.
     */
    inline CopyObjectRequest& WithTaggingDirective(TaggingDirective&& value) { SetTaggingDirective(std::move(value)); return *this;}


    /**
     * The Server-side encryption algorithm used when storing this object in S3 (e.g.,
     * AES256, aws:kms).
     */
    inline const ServerSideEncryption& GetServerSideEncryption() const{ return m_serverSideEncryption; }

    /**
     * The Server-side encryption algorithm used when storing this object in S3 (e.g.,
     * AES256, aws:kms).
     */
    inline void SetServerSideEncryption(const ServerSideEncryption& value) { m_serverSideEncryptionHasBeenSet = true; m_serverSideEncryption = value; }

    /**
     * The Server-side encryption algorithm used when storing this object in S3 (e.g.,
     * AES256, aws:kms).
     */
    inline void SetServerSideEncryption(ServerSideEncryption&& value) { m_serverSideEncryptionHasBeenSet = true; m_serverSideEncryption = std::move(value); }

    /**
     * The Server-side encryption algorithm used when storing this object in S3 (e.g.,
     * AES256, aws:kms).
     */
    inline CopyObjectRequest& WithServerSideEncryption(const ServerSideEncryption& value) { SetServerSideEncryption(value); return *this;}

    /**
     * The Server-side encryption algorithm used when storing this object in S3 (e.g.,
     * AES256, aws:kms).
     */
    inline CopyObjectRequest& WithServerSideEncryption(ServerSideEncryption&& value) { SetServerSideEncryption(std::move(value)); return *this;}


    /**
     * The type of storage to use for the object. Defaults to 'STANDARD'.
     */
    inline const StorageClass& GetStorageClass() const{ return m_storageClass; }

    /**
     * The type of storage to use for the object. Defaults to 'STANDARD'.
     */
    inline void SetStorageClass(const StorageClass& value) { m_storageClassHasBeenSet = true; m_storageClass = value; }

    /**
     * The type of storage to use for the object. Defaults to 'STANDARD'.
     */
    inline void SetStorageClass(StorageClass&& value) { m_storageClassHasBeenSet = true; m_storageClass = std::move(value); }

    /**
     * The type of storage to use for the object. Defaults to 'STANDARD'.
     */
    inline CopyObjectRequest& WithStorageClass(const StorageClass& value) { SetStorageClass(value); return *this;}

    /**
     * The type of storage to use for the object. Defaults to 'STANDARD'.
     */
    inline CopyObjectRequest& WithStorageClass(StorageClass&& value) { SetStorageClass(std::move(value)); return *this;}


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
    inline void SetWebsiteRedirectLocation(const Aws::String& value) { m_websiteRedirectLocationHasBeenSet = true; m_websiteRedirectLocation = value; }

    /**
     * If the bucket is configured as a website, redirects requests for this object to
     * another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata.
     */
    inline void SetWebsiteRedirectLocation(Aws::String&& value) { m_websiteRedirectLocationHasBeenSet = true; m_websiteRedirectLocation = std::move(value); }

    /**
     * If the bucket is configured as a website, redirects requests for this object to
     * another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata.
     */
    inline void SetWebsiteRedirectLocation(const char* value) { m_websiteRedirectLocationHasBeenSet = true; m_websiteRedirectLocation.assign(value); }

    /**
     * If the bucket is configured as a website, redirects requests for this object to
     * another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata.
     */
    inline CopyObjectRequest& WithWebsiteRedirectLocation(const Aws::String& value) { SetWebsiteRedirectLocation(value); return *this;}

    /**
     * If the bucket is configured as a website, redirects requests for this object to
     * another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata.
     */
    inline CopyObjectRequest& WithWebsiteRedirectLocation(Aws::String&& value) { SetWebsiteRedirectLocation(std::move(value)); return *this;}

    /**
     * If the bucket is configured as a website, redirects requests for this object to
     * another object in the same bucket or to an external URL. Amazon S3 stores the
     * value of this header in the object metadata.
     */
    inline CopyObjectRequest& WithWebsiteRedirectLocation(const char* value) { SetWebsiteRedirectLocation(value); return *this;}


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
    inline CopyObjectRequest& WithSSECustomerAlgorithm(const Aws::String& value) { SetSSECustomerAlgorithm(value); return *this;}

    /**
     * Specifies the algorithm to use to when encrypting the object (e.g., AES256).
     */
    inline CopyObjectRequest& WithSSECustomerAlgorithm(Aws::String&& value) { SetSSECustomerAlgorithm(std::move(value)); return *this;}

    /**
     * Specifies the algorithm to use to when encrypting the object (e.g., AES256).
     */
    inline CopyObjectRequest& WithSSECustomerAlgorithm(const char* value) { SetSSECustomerAlgorithm(value); return *this;}


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
    inline CopyObjectRequest& WithSSECustomerKey(const Aws::String& value) { SetSSECustomerKey(value); return *this;}

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon does not store the encryption key. The key must be appropriate
     * for use with the algorithm specified in the
     * x-amz-server-side​-encryption​-customer-algorithm header.
     */
    inline CopyObjectRequest& WithSSECustomerKey(Aws::String&& value) { SetSSECustomerKey(std::move(value)); return *this;}

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use in
     * encrypting data. This value is used to store the object and then it is
     * discarded; Amazon does not store the encryption key. The key must be appropriate
     * for use with the algorithm specified in the
     * x-amz-server-side​-encryption​-customer-algorithm header.
     */
    inline CopyObjectRequest& WithSSECustomerKey(const char* value) { SetSSECustomerKey(value); return *this;}


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
    inline CopyObjectRequest& WithSSECustomerKeyMD5(const Aws::String& value) { SetSSECustomerKeyMD5(value); return *this;}

    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline CopyObjectRequest& WithSSECustomerKeyMD5(Aws::String&& value) { SetSSECustomerKeyMD5(std::move(value)); return *this;}

    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline CopyObjectRequest& WithSSECustomerKeyMD5(const char* value) { SetSSECustomerKeyMD5(value); return *this;}


    /**
     * Specifies the AWS KMS key ID to use for object encryption. All GET and PUT
     * requests for an object protected by AWS KMS will fail if not made via SSL or
     * using SigV4. Documentation on configuring any of the officially supported AWS
     * SDKs and CLI can be found at
     * http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingAWSSDK.html#specify-signature-version
     */
    inline const Aws::String& GetSSEKMSKeyId() const{ return m_sSEKMSKeyId; }

    /**
     * Specifies the AWS KMS key ID to use for object encryption. All GET and PUT
     * requests for an object protected by AWS KMS will fail if not made via SSL or
     * using SigV4. Documentation on configuring any of the officially supported AWS
     * SDKs and CLI can be found at
     * http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingAWSSDK.html#specify-signature-version
     */
    inline void SetSSEKMSKeyId(const Aws::String& value) { m_sSEKMSKeyIdHasBeenSet = true; m_sSEKMSKeyId = value; }

    /**
     * Specifies the AWS KMS key ID to use for object encryption. All GET and PUT
     * requests for an object protected by AWS KMS will fail if not made via SSL or
     * using SigV4. Documentation on configuring any of the officially supported AWS
     * SDKs and CLI can be found at
     * http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingAWSSDK.html#specify-signature-version
     */
    inline void SetSSEKMSKeyId(Aws::String&& value) { m_sSEKMSKeyIdHasBeenSet = true; m_sSEKMSKeyId = std::move(value); }

    /**
     * Specifies the AWS KMS key ID to use for object encryption. All GET and PUT
     * requests for an object protected by AWS KMS will fail if not made via SSL or
     * using SigV4. Documentation on configuring any of the officially supported AWS
     * SDKs and CLI can be found at
     * http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingAWSSDK.html#specify-signature-version
     */
    inline void SetSSEKMSKeyId(const char* value) { m_sSEKMSKeyIdHasBeenSet = true; m_sSEKMSKeyId.assign(value); }

    /**
     * Specifies the AWS KMS key ID to use for object encryption. All GET and PUT
     * requests for an object protected by AWS KMS will fail if not made via SSL or
     * using SigV4. Documentation on configuring any of the officially supported AWS
     * SDKs and CLI can be found at
     * http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingAWSSDK.html#specify-signature-version
     */
    inline CopyObjectRequest& WithSSEKMSKeyId(const Aws::String& value) { SetSSEKMSKeyId(value); return *this;}

    /**
     * Specifies the AWS KMS key ID to use for object encryption. All GET and PUT
     * requests for an object protected by AWS KMS will fail if not made via SSL or
     * using SigV4. Documentation on configuring any of the officially supported AWS
     * SDKs and CLI can be found at
     * http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingAWSSDK.html#specify-signature-version
     */
    inline CopyObjectRequest& WithSSEKMSKeyId(Aws::String&& value) { SetSSEKMSKeyId(std::move(value)); return *this;}

    /**
     * Specifies the AWS KMS key ID to use for object encryption. All GET and PUT
     * requests for an object protected by AWS KMS will fail if not made via SSL or
     * using SigV4. Documentation on configuring any of the officially supported AWS
     * SDKs and CLI can be found at
     * http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingAWSSDK.html#specify-signature-version
     */
    inline CopyObjectRequest& WithSSEKMSKeyId(const char* value) { SetSSEKMSKeyId(value); return *this;}


    /**
     * Specifies the algorithm to use when decrypting the source object (e.g., AES256).
     */
    inline const Aws::String& GetCopySourceSSECustomerAlgorithm() const{ return m_copySourceSSECustomerAlgorithm; }

    /**
     * Specifies the algorithm to use when decrypting the source object (e.g., AES256).
     */
    inline void SetCopySourceSSECustomerAlgorithm(const Aws::String& value) { m_copySourceSSECustomerAlgorithmHasBeenSet = true; m_copySourceSSECustomerAlgorithm = value; }

    /**
     * Specifies the algorithm to use when decrypting the source object (e.g., AES256).
     */
    inline void SetCopySourceSSECustomerAlgorithm(Aws::String&& value) { m_copySourceSSECustomerAlgorithmHasBeenSet = true; m_copySourceSSECustomerAlgorithm = std::move(value); }

    /**
     * Specifies the algorithm to use when decrypting the source object (e.g., AES256).
     */
    inline void SetCopySourceSSECustomerAlgorithm(const char* value) { m_copySourceSSECustomerAlgorithmHasBeenSet = true; m_copySourceSSECustomerAlgorithm.assign(value); }

    /**
     * Specifies the algorithm to use when decrypting the source object (e.g., AES256).
     */
    inline CopyObjectRequest& WithCopySourceSSECustomerAlgorithm(const Aws::String& value) { SetCopySourceSSECustomerAlgorithm(value); return *this;}

    /**
     * Specifies the algorithm to use when decrypting the source object (e.g., AES256).
     */
    inline CopyObjectRequest& WithCopySourceSSECustomerAlgorithm(Aws::String&& value) { SetCopySourceSSECustomerAlgorithm(std::move(value)); return *this;}

    /**
     * Specifies the algorithm to use when decrypting the source object (e.g., AES256).
     */
    inline CopyObjectRequest& WithCopySourceSSECustomerAlgorithm(const char* value) { SetCopySourceSSECustomerAlgorithm(value); return *this;}


    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use to decrypt
     * the source object. The encryption key provided in this header must be one that
     * was used when the source object was created.
     */
    inline const Aws::String& GetCopySourceSSECustomerKey() const{ return m_copySourceSSECustomerKey; }

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use to decrypt
     * the source object. The encryption key provided in this header must be one that
     * was used when the source object was created.
     */
    inline void SetCopySourceSSECustomerKey(const Aws::String& value) { m_copySourceSSECustomerKeyHasBeenSet = true; m_copySourceSSECustomerKey = value; }

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use to decrypt
     * the source object. The encryption key provided in this header must be one that
     * was used when the source object was created.
     */
    inline void SetCopySourceSSECustomerKey(Aws::String&& value) { m_copySourceSSECustomerKeyHasBeenSet = true; m_copySourceSSECustomerKey = std::move(value); }

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use to decrypt
     * the source object. The encryption key provided in this header must be one that
     * was used when the source object was created.
     */
    inline void SetCopySourceSSECustomerKey(const char* value) { m_copySourceSSECustomerKeyHasBeenSet = true; m_copySourceSSECustomerKey.assign(value); }

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use to decrypt
     * the source object. The encryption key provided in this header must be one that
     * was used when the source object was created.
     */
    inline CopyObjectRequest& WithCopySourceSSECustomerKey(const Aws::String& value) { SetCopySourceSSECustomerKey(value); return *this;}

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use to decrypt
     * the source object. The encryption key provided in this header must be one that
     * was used when the source object was created.
     */
    inline CopyObjectRequest& WithCopySourceSSECustomerKey(Aws::String&& value) { SetCopySourceSSECustomerKey(std::move(value)); return *this;}

    /**
     * Specifies the customer-provided encryption key for Amazon S3 to use to decrypt
     * the source object. The encryption key provided in this header must be one that
     * was used when the source object was created.
     */
    inline CopyObjectRequest& WithCopySourceSSECustomerKey(const char* value) { SetCopySourceSSECustomerKey(value); return *this;}


    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline const Aws::String& GetCopySourceSSECustomerKeyMD5() const{ return m_copySourceSSECustomerKeyMD5; }

    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline void SetCopySourceSSECustomerKeyMD5(const Aws::String& value) { m_copySourceSSECustomerKeyMD5HasBeenSet = true; m_copySourceSSECustomerKeyMD5 = value; }

    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline void SetCopySourceSSECustomerKeyMD5(Aws::String&& value) { m_copySourceSSECustomerKeyMD5HasBeenSet = true; m_copySourceSSECustomerKeyMD5 = std::move(value); }

    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline void SetCopySourceSSECustomerKeyMD5(const char* value) { m_copySourceSSECustomerKeyMD5HasBeenSet = true; m_copySourceSSECustomerKeyMD5.assign(value); }

    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline CopyObjectRequest& WithCopySourceSSECustomerKeyMD5(const Aws::String& value) { SetCopySourceSSECustomerKeyMD5(value); return *this;}

    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline CopyObjectRequest& WithCopySourceSSECustomerKeyMD5(Aws::String&& value) { SetCopySourceSSECustomerKeyMD5(std::move(value)); return *this;}

    /**
     * Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
     * Amazon S3 uses this header for a message integrity check to ensure the
     * encryption key was transmitted without error.
     */
    inline CopyObjectRequest& WithCopySourceSSECustomerKeyMD5(const char* value) { SetCopySourceSSECustomerKeyMD5(value); return *this;}


    
    inline const RequestPayer& GetRequestPayer() const{ return m_requestPayer; }

    
    inline void SetRequestPayer(const RequestPayer& value) { m_requestPayerHasBeenSet = true; m_requestPayer = value; }

    
    inline void SetRequestPayer(RequestPayer&& value) { m_requestPayerHasBeenSet = true; m_requestPayer = std::move(value); }

    
    inline CopyObjectRequest& WithRequestPayer(const RequestPayer& value) { SetRequestPayer(value); return *this;}

    
    inline CopyObjectRequest& WithRequestPayer(RequestPayer&& value) { SetRequestPayer(std::move(value)); return *this;}


    /**
     * The tag-set for the object destination object this value must be used in
     * conjunction with the TaggingDirective. The tag-set must be encoded as URL Query
     * parameters
     */
    inline const Aws::String& GetTagging() const{ return m_tagging; }

    /**
     * The tag-set for the object destination object this value must be used in
     * conjunction with the TaggingDirective. The tag-set must be encoded as URL Query
     * parameters
     */
    inline void SetTagging(const Aws::String& value) { m_taggingHasBeenSet = true; m_tagging = value; }

    /**
     * The tag-set for the object destination object this value must be used in
     * conjunction with the TaggingDirective. The tag-set must be encoded as URL Query
     * parameters
     */
    inline void SetTagging(Aws::String&& value) { m_taggingHasBeenSet = true; m_tagging = std::move(value); }

    /**
     * The tag-set for the object destination object this value must be used in
     * conjunction with the TaggingDirective. The tag-set must be encoded as URL Query
     * parameters
     */
    inline void SetTagging(const char* value) { m_taggingHasBeenSet = true; m_tagging.assign(value); }

    /**
     * The tag-set for the object destination object this value must be used in
     * conjunction with the TaggingDirective. The tag-set must be encoded as URL Query
     * parameters
     */
    inline CopyObjectRequest& WithTagging(const Aws::String& value) { SetTagging(value); return *this;}

    /**
     * The tag-set for the object destination object this value must be used in
     * conjunction with the TaggingDirective. The tag-set must be encoded as URL Query
     * parameters
     */
    inline CopyObjectRequest& WithTagging(Aws::String&& value) { SetTagging(std::move(value)); return *this;}

    /**
     * The tag-set for the object destination object this value must be used in
     * conjunction with the TaggingDirective. The tag-set must be encoded as URL Query
     * parameters
     */
    inline CopyObjectRequest& WithTagging(const char* value) { SetTagging(value); return *this;}

  private:

    ObjectCannedACL m_aCL;
    bool m_aCLHasBeenSet;

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_cacheControl;
    bool m_cacheControlHasBeenSet;

    Aws::String m_contentDisposition;
    bool m_contentDispositionHasBeenSet;

    Aws::String m_contentEncoding;
    bool m_contentEncodingHasBeenSet;

    Aws::String m_contentLanguage;
    bool m_contentLanguageHasBeenSet;

    Aws::String m_contentType;
    bool m_contentTypeHasBeenSet;

    Aws::String m_copySource;
    bool m_copySourceHasBeenSet;

    Aws::String m_copySourceIfMatch;
    bool m_copySourceIfMatchHasBeenSet;

    Aws::Utils::DateTime m_copySourceIfModifiedSince;
    bool m_copySourceIfModifiedSinceHasBeenSet;

    Aws::String m_copySourceIfNoneMatch;
    bool m_copySourceIfNoneMatchHasBeenSet;

    Aws::Utils::DateTime m_copySourceIfUnmodifiedSince;
    bool m_copySourceIfUnmodifiedSinceHasBeenSet;

    Aws::Utils::DateTime m_expires;
    bool m_expiresHasBeenSet;

    Aws::String m_grantFullControl;
    bool m_grantFullControlHasBeenSet;

    Aws::String m_grantRead;
    bool m_grantReadHasBeenSet;

    Aws::String m_grantReadACP;
    bool m_grantReadACPHasBeenSet;

    Aws::String m_grantWriteACP;
    bool m_grantWriteACPHasBeenSet;

    Aws::String m_key;
    bool m_keyHasBeenSet;

    Aws::Map<Aws::String, Aws::String> m_metadata;
    bool m_metadataHasBeenSet;

    MetadataDirective m_metadataDirective;
    bool m_metadataDirectiveHasBeenSet;

    TaggingDirective m_taggingDirective;
    bool m_taggingDirectiveHasBeenSet;

    ServerSideEncryption m_serverSideEncryption;
    bool m_serverSideEncryptionHasBeenSet;

    StorageClass m_storageClass;
    bool m_storageClassHasBeenSet;

    Aws::String m_websiteRedirectLocation;
    bool m_websiteRedirectLocationHasBeenSet;

    Aws::String m_sSECustomerAlgorithm;
    bool m_sSECustomerAlgorithmHasBeenSet;

    Aws::String m_sSECustomerKey;
    bool m_sSECustomerKeyHasBeenSet;

    Aws::String m_sSECustomerKeyMD5;
    bool m_sSECustomerKeyMD5HasBeenSet;

    Aws::String m_sSEKMSKeyId;
    bool m_sSEKMSKeyIdHasBeenSet;

    Aws::String m_copySourceSSECustomerAlgorithm;
    bool m_copySourceSSECustomerAlgorithmHasBeenSet;

    Aws::String m_copySourceSSECustomerKey;
    bool m_copySourceSSECustomerKeyHasBeenSet;

    Aws::String m_copySourceSSECustomerKeyMD5;
    bool m_copySourceSSECustomerKeyMD5HasBeenSet;

    RequestPayer m_requestPayer;
    bool m_requestPayerHasBeenSet;

    Aws::String m_tagging;
    bool m_taggingHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
