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
#include <aws/s3/model/ServerSideEncryption.h>
#include <aws/s3/model/RequestCharged.h>
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
  class AWS_S3_API CompleteMultipartUploadResult
  {
  public:
    CompleteMultipartUploadResult();
    CompleteMultipartUploadResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    CompleteMultipartUploadResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    
    inline const Aws::String& GetLocation() const{ return m_location; }

    
    inline void SetLocation(const Aws::String& value) { m_location = value; }

    
    inline void SetLocation(Aws::String&& value) { m_location = std::move(value); }

    
    inline void SetLocation(const char* value) { m_location.assign(value); }

    
    inline CompleteMultipartUploadResult& WithLocation(const Aws::String& value) { SetLocation(value); return *this;}

    
    inline CompleteMultipartUploadResult& WithLocation(Aws::String&& value) { SetLocation(std::move(value)); return *this;}

    
    inline CompleteMultipartUploadResult& WithLocation(const char* value) { SetLocation(value); return *this;}


    
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    
    inline void SetBucket(const Aws::String& value) { m_bucket = value; }

    
    inline void SetBucket(Aws::String&& value) { m_bucket = std::move(value); }

    
    inline void SetBucket(const char* value) { m_bucket.assign(value); }

    
    inline CompleteMultipartUploadResult& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    
    inline CompleteMultipartUploadResult& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    
    inline CompleteMultipartUploadResult& WithBucket(const char* value) { SetBucket(value); return *this;}


    
    inline const Aws::String& GetKey() const{ return m_key; }

    
    inline void SetKey(const Aws::String& value) { m_key = value; }

    
    inline void SetKey(Aws::String&& value) { m_key = std::move(value); }

    
    inline void SetKey(const char* value) { m_key.assign(value); }

    
    inline CompleteMultipartUploadResult& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    
    inline CompleteMultipartUploadResult& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    
    inline CompleteMultipartUploadResult& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * If the object expiration is configured, this will contain the expiration date
     * (expiry-date) and rule ID (rule-id). The value of rule-id is URL encoded.
     */
    inline const Aws::String& GetExpiration() const{ return m_expiration; }

    /**
     * If the object expiration is configured, this will contain the expiration date
     * (expiry-date) and rule ID (rule-id). The value of rule-id is URL encoded.
     */
    inline void SetExpiration(const Aws::String& value) { m_expiration = value; }

    /**
     * If the object expiration is configured, this will contain the expiration date
     * (expiry-date) and rule ID (rule-id). The value of rule-id is URL encoded.
     */
    inline void SetExpiration(Aws::String&& value) { m_expiration = std::move(value); }

    /**
     * If the object expiration is configured, this will contain the expiration date
     * (expiry-date) and rule ID (rule-id). The value of rule-id is URL encoded.
     */
    inline void SetExpiration(const char* value) { m_expiration.assign(value); }

    /**
     * If the object expiration is configured, this will contain the expiration date
     * (expiry-date) and rule ID (rule-id). The value of rule-id is URL encoded.
     */
    inline CompleteMultipartUploadResult& WithExpiration(const Aws::String& value) { SetExpiration(value); return *this;}

    /**
     * If the object expiration is configured, this will contain the expiration date
     * (expiry-date) and rule ID (rule-id). The value of rule-id is URL encoded.
     */
    inline CompleteMultipartUploadResult& WithExpiration(Aws::String&& value) { SetExpiration(std::move(value)); return *this;}

    /**
     * If the object expiration is configured, this will contain the expiration date
     * (expiry-date) and rule ID (rule-id). The value of rule-id is URL encoded.
     */
    inline CompleteMultipartUploadResult& WithExpiration(const char* value) { SetExpiration(value); return *this;}


    /**
     * Entity tag of the object.
     */
    inline const Aws::String& GetETag() const{ return m_eTag; }

    /**
     * Entity tag of the object.
     */
    inline void SetETag(const Aws::String& value) { m_eTag = value; }

    /**
     * Entity tag of the object.
     */
    inline void SetETag(Aws::String&& value) { m_eTag = std::move(value); }

    /**
     * Entity tag of the object.
     */
    inline void SetETag(const char* value) { m_eTag.assign(value); }

    /**
     * Entity tag of the object.
     */
    inline CompleteMultipartUploadResult& WithETag(const Aws::String& value) { SetETag(value); return *this;}

    /**
     * Entity tag of the object.
     */
    inline CompleteMultipartUploadResult& WithETag(Aws::String&& value) { SetETag(std::move(value)); return *this;}

    /**
     * Entity tag of the object.
     */
    inline CompleteMultipartUploadResult& WithETag(const char* value) { SetETag(value); return *this;}


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
    inline CompleteMultipartUploadResult& WithServerSideEncryption(const ServerSideEncryption& value) { SetServerSideEncryption(value); return *this;}

    /**
     * The Server-side encryption algorithm used when storing this object in S3 (e.g.,
     * AES256, aws:kms).
     */
    inline CompleteMultipartUploadResult& WithServerSideEncryption(ServerSideEncryption&& value) { SetServerSideEncryption(std::move(value)); return *this;}


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
    inline CompleteMultipartUploadResult& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    /**
     * Version of the object.
     */
    inline CompleteMultipartUploadResult& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    /**
     * Version of the object.
     */
    inline CompleteMultipartUploadResult& WithVersionId(const char* value) { SetVersionId(value); return *this;}


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
    inline CompleteMultipartUploadResult& WithSSEKMSKeyId(const Aws::String& value) { SetSSEKMSKeyId(value); return *this;}

    /**
     * If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.
     */
    inline CompleteMultipartUploadResult& WithSSEKMSKeyId(Aws::String&& value) { SetSSEKMSKeyId(std::move(value)); return *this;}

    /**
     * If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.
     */
    inline CompleteMultipartUploadResult& WithSSEKMSKeyId(const char* value) { SetSSEKMSKeyId(value); return *this;}


    
    inline const RequestCharged& GetRequestCharged() const{ return m_requestCharged; }

    
    inline void SetRequestCharged(const RequestCharged& value) { m_requestCharged = value; }

    
    inline void SetRequestCharged(RequestCharged&& value) { m_requestCharged = std::move(value); }

    
    inline CompleteMultipartUploadResult& WithRequestCharged(const RequestCharged& value) { SetRequestCharged(value); return *this;}

    
    inline CompleteMultipartUploadResult& WithRequestCharged(RequestCharged&& value) { SetRequestCharged(std::move(value)); return *this;}

  private:

    Aws::String m_location;

    Aws::String m_bucket;

    Aws::String m_key;

    Aws::String m_expiration;

    Aws::String m_eTag;

    ServerSideEncryption m_serverSideEncryption;

    Aws::String m_versionId;

    Aws::String m_sSEKMSKeyId;

    RequestCharged m_requestCharged;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
