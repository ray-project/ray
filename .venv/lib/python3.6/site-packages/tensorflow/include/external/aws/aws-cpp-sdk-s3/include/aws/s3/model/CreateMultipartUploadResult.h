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
#include <aws/core/utils/DateTime.h>
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
  class AWS_S3_API CreateMultipartUploadResult
  {
  public:
    CreateMultipartUploadResult();
    CreateMultipartUploadResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    CreateMultipartUploadResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * Date when multipart upload will become eligible for abort operation by
     * lifecycle.
     */
    inline const Aws::Utils::DateTime& GetAbortDate() const{ return m_abortDate; }

    /**
     * Date when multipart upload will become eligible for abort operation by
     * lifecycle.
     */
    inline void SetAbortDate(const Aws::Utils::DateTime& value) { m_abortDate = value; }

    /**
     * Date when multipart upload will become eligible for abort operation by
     * lifecycle.
     */
    inline void SetAbortDate(Aws::Utils::DateTime&& value) { m_abortDate = std::move(value); }

    /**
     * Date when multipart upload will become eligible for abort operation by
     * lifecycle.
     */
    inline CreateMultipartUploadResult& WithAbortDate(const Aws::Utils::DateTime& value) { SetAbortDate(value); return *this;}

    /**
     * Date when multipart upload will become eligible for abort operation by
     * lifecycle.
     */
    inline CreateMultipartUploadResult& WithAbortDate(Aws::Utils::DateTime&& value) { SetAbortDate(std::move(value)); return *this;}


    /**
     * Id of the lifecycle rule that makes a multipart upload eligible for abort
     * operation.
     */
    inline const Aws::String& GetAbortRuleId() const{ return m_abortRuleId; }

    /**
     * Id of the lifecycle rule that makes a multipart upload eligible for abort
     * operation.
     */
    inline void SetAbortRuleId(const Aws::String& value) { m_abortRuleId = value; }

    /**
     * Id of the lifecycle rule that makes a multipart upload eligible for abort
     * operation.
     */
    inline void SetAbortRuleId(Aws::String&& value) { m_abortRuleId = std::move(value); }

    /**
     * Id of the lifecycle rule that makes a multipart upload eligible for abort
     * operation.
     */
    inline void SetAbortRuleId(const char* value) { m_abortRuleId.assign(value); }

    /**
     * Id of the lifecycle rule that makes a multipart upload eligible for abort
     * operation.
     */
    inline CreateMultipartUploadResult& WithAbortRuleId(const Aws::String& value) { SetAbortRuleId(value); return *this;}

    /**
     * Id of the lifecycle rule that makes a multipart upload eligible for abort
     * operation.
     */
    inline CreateMultipartUploadResult& WithAbortRuleId(Aws::String&& value) { SetAbortRuleId(std::move(value)); return *this;}

    /**
     * Id of the lifecycle rule that makes a multipart upload eligible for abort
     * operation.
     */
    inline CreateMultipartUploadResult& WithAbortRuleId(const char* value) { SetAbortRuleId(value); return *this;}


    /**
     * Name of the bucket to which the multipart upload was initiated.
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * Name of the bucket to which the multipart upload was initiated.
     */
    inline void SetBucket(const Aws::String& value) { m_bucket = value; }

    /**
     * Name of the bucket to which the multipart upload was initiated.
     */
    inline void SetBucket(Aws::String&& value) { m_bucket = std::move(value); }

    /**
     * Name of the bucket to which the multipart upload was initiated.
     */
    inline void SetBucket(const char* value) { m_bucket.assign(value); }

    /**
     * Name of the bucket to which the multipart upload was initiated.
     */
    inline CreateMultipartUploadResult& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * Name of the bucket to which the multipart upload was initiated.
     */
    inline CreateMultipartUploadResult& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * Name of the bucket to which the multipart upload was initiated.
     */
    inline CreateMultipartUploadResult& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * Object key for which the multipart upload was initiated.
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * Object key for which the multipart upload was initiated.
     */
    inline void SetKey(const Aws::String& value) { m_key = value; }

    /**
     * Object key for which the multipart upload was initiated.
     */
    inline void SetKey(Aws::String&& value) { m_key = std::move(value); }

    /**
     * Object key for which the multipart upload was initiated.
     */
    inline void SetKey(const char* value) { m_key.assign(value); }

    /**
     * Object key for which the multipart upload was initiated.
     */
    inline CreateMultipartUploadResult& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * Object key for which the multipart upload was initiated.
     */
    inline CreateMultipartUploadResult& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * Object key for which the multipart upload was initiated.
     */
    inline CreateMultipartUploadResult& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * ID for the initiated multipart upload.
     */
    inline const Aws::String& GetUploadId() const{ return m_uploadId; }

    /**
     * ID for the initiated multipart upload.
     */
    inline void SetUploadId(const Aws::String& value) { m_uploadId = value; }

    /**
     * ID for the initiated multipart upload.
     */
    inline void SetUploadId(Aws::String&& value) { m_uploadId = std::move(value); }

    /**
     * ID for the initiated multipart upload.
     */
    inline void SetUploadId(const char* value) { m_uploadId.assign(value); }

    /**
     * ID for the initiated multipart upload.
     */
    inline CreateMultipartUploadResult& WithUploadId(const Aws::String& value) { SetUploadId(value); return *this;}

    /**
     * ID for the initiated multipart upload.
     */
    inline CreateMultipartUploadResult& WithUploadId(Aws::String&& value) { SetUploadId(std::move(value)); return *this;}

    /**
     * ID for the initiated multipart upload.
     */
    inline CreateMultipartUploadResult& WithUploadId(const char* value) { SetUploadId(value); return *this;}


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
    inline CreateMultipartUploadResult& WithServerSideEncryption(const ServerSideEncryption& value) { SetServerSideEncryption(value); return *this;}

    /**
     * The Server-side encryption algorithm used when storing this object in S3 (e.g.,
     * AES256, aws:kms).
     */
    inline CreateMultipartUploadResult& WithServerSideEncryption(ServerSideEncryption&& value) { SetServerSideEncryption(std::move(value)); return *this;}


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
    inline CreateMultipartUploadResult& WithSSECustomerAlgorithm(const Aws::String& value) { SetSSECustomerAlgorithm(value); return *this;}

    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header confirming the encryption algorithm used.
     */
    inline CreateMultipartUploadResult& WithSSECustomerAlgorithm(Aws::String&& value) { SetSSECustomerAlgorithm(std::move(value)); return *this;}

    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header confirming the encryption algorithm used.
     */
    inline CreateMultipartUploadResult& WithSSECustomerAlgorithm(const char* value) { SetSSECustomerAlgorithm(value); return *this;}


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
    inline CreateMultipartUploadResult& WithSSECustomerKeyMD5(const Aws::String& value) { SetSSECustomerKeyMD5(value); return *this;}

    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header to provide round trip message integrity
     * verification of the customer-provided encryption key.
     */
    inline CreateMultipartUploadResult& WithSSECustomerKeyMD5(Aws::String&& value) { SetSSECustomerKeyMD5(std::move(value)); return *this;}

    /**
     * If server-side encryption with a customer-provided encryption key was requested,
     * the response will include this header to provide round trip message integrity
     * verification of the customer-provided encryption key.
     */
    inline CreateMultipartUploadResult& WithSSECustomerKeyMD5(const char* value) { SetSSECustomerKeyMD5(value); return *this;}


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
    inline CreateMultipartUploadResult& WithSSEKMSKeyId(const Aws::String& value) { SetSSEKMSKeyId(value); return *this;}

    /**
     * If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.
     */
    inline CreateMultipartUploadResult& WithSSEKMSKeyId(Aws::String&& value) { SetSSEKMSKeyId(std::move(value)); return *this;}

    /**
     * If present, specifies the ID of the AWS Key Management Service (KMS) master
     * encryption key that was used for the object.
     */
    inline CreateMultipartUploadResult& WithSSEKMSKeyId(const char* value) { SetSSEKMSKeyId(value); return *this;}


    
    inline const RequestCharged& GetRequestCharged() const{ return m_requestCharged; }

    
    inline void SetRequestCharged(const RequestCharged& value) { m_requestCharged = value; }

    
    inline void SetRequestCharged(RequestCharged&& value) { m_requestCharged = std::move(value); }

    
    inline CreateMultipartUploadResult& WithRequestCharged(const RequestCharged& value) { SetRequestCharged(value); return *this;}

    
    inline CreateMultipartUploadResult& WithRequestCharged(RequestCharged&& value) { SetRequestCharged(std::move(value)); return *this;}

  private:

    Aws::Utils::DateTime m_abortDate;

    Aws::String m_abortRuleId;

    Aws::String m_bucket;

    Aws::String m_key;

    Aws::String m_uploadId;

    ServerSideEncryption m_serverSideEncryption;

    Aws::String m_sSECustomerAlgorithm;

    Aws::String m_sSECustomerKeyMD5;

    Aws::String m_sSEKMSKeyId;

    RequestCharged m_requestCharged;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
