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
#include <aws/s3/model/BucketCannedACL.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/CreateBucketConfiguration.h>
#include <utility>

namespace Aws
{
namespace S3
{
namespace Model
{

  /**
   */
  class AWS_S3_API CreateBucketRequest : public S3Request
  {
  public:
    CreateBucketRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "CreateBucket"; }

    Aws::String SerializePayload() const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    /**
     * The canned ACL to apply to the bucket.
     */
    inline const BucketCannedACL& GetACL() const{ return m_aCL; }

    /**
     * The canned ACL to apply to the bucket.
     */
    inline void SetACL(const BucketCannedACL& value) { m_aCLHasBeenSet = true; m_aCL = value; }

    /**
     * The canned ACL to apply to the bucket.
     */
    inline void SetACL(BucketCannedACL&& value) { m_aCLHasBeenSet = true; m_aCL = std::move(value); }

    /**
     * The canned ACL to apply to the bucket.
     */
    inline CreateBucketRequest& WithACL(const BucketCannedACL& value) { SetACL(value); return *this;}

    /**
     * The canned ACL to apply to the bucket.
     */
    inline CreateBucketRequest& WithACL(BucketCannedACL&& value) { SetACL(std::move(value)); return *this;}


    
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    
    inline CreateBucketRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    
    inline CreateBucketRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    
    inline CreateBucketRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    
    inline const CreateBucketConfiguration& GetCreateBucketConfiguration() const{ return m_createBucketConfiguration; }

    
    inline void SetCreateBucketConfiguration(const CreateBucketConfiguration& value) { m_createBucketConfigurationHasBeenSet = true; m_createBucketConfiguration = value; }

    
    inline void SetCreateBucketConfiguration(CreateBucketConfiguration&& value) { m_createBucketConfigurationHasBeenSet = true; m_createBucketConfiguration = std::move(value); }

    
    inline CreateBucketRequest& WithCreateBucketConfiguration(const CreateBucketConfiguration& value) { SetCreateBucketConfiguration(value); return *this;}

    
    inline CreateBucketRequest& WithCreateBucketConfiguration(CreateBucketConfiguration&& value) { SetCreateBucketConfiguration(std::move(value)); return *this;}


    /**
     * Allows grantee the read, write, read ACP, and write ACP permissions on the
     * bucket.
     */
    inline const Aws::String& GetGrantFullControl() const{ return m_grantFullControl; }

    /**
     * Allows grantee the read, write, read ACP, and write ACP permissions on the
     * bucket.
     */
    inline void SetGrantFullControl(const Aws::String& value) { m_grantFullControlHasBeenSet = true; m_grantFullControl = value; }

    /**
     * Allows grantee the read, write, read ACP, and write ACP permissions on the
     * bucket.
     */
    inline void SetGrantFullControl(Aws::String&& value) { m_grantFullControlHasBeenSet = true; m_grantFullControl = std::move(value); }

    /**
     * Allows grantee the read, write, read ACP, and write ACP permissions on the
     * bucket.
     */
    inline void SetGrantFullControl(const char* value) { m_grantFullControlHasBeenSet = true; m_grantFullControl.assign(value); }

    /**
     * Allows grantee the read, write, read ACP, and write ACP permissions on the
     * bucket.
     */
    inline CreateBucketRequest& WithGrantFullControl(const Aws::String& value) { SetGrantFullControl(value); return *this;}

    /**
     * Allows grantee the read, write, read ACP, and write ACP permissions on the
     * bucket.
     */
    inline CreateBucketRequest& WithGrantFullControl(Aws::String&& value) { SetGrantFullControl(std::move(value)); return *this;}

    /**
     * Allows grantee the read, write, read ACP, and write ACP permissions on the
     * bucket.
     */
    inline CreateBucketRequest& WithGrantFullControl(const char* value) { SetGrantFullControl(value); return *this;}


    /**
     * Allows grantee to list the objects in the bucket.
     */
    inline const Aws::String& GetGrantRead() const{ return m_grantRead; }

    /**
     * Allows grantee to list the objects in the bucket.
     */
    inline void SetGrantRead(const Aws::String& value) { m_grantReadHasBeenSet = true; m_grantRead = value; }

    /**
     * Allows grantee to list the objects in the bucket.
     */
    inline void SetGrantRead(Aws::String&& value) { m_grantReadHasBeenSet = true; m_grantRead = std::move(value); }

    /**
     * Allows grantee to list the objects in the bucket.
     */
    inline void SetGrantRead(const char* value) { m_grantReadHasBeenSet = true; m_grantRead.assign(value); }

    /**
     * Allows grantee to list the objects in the bucket.
     */
    inline CreateBucketRequest& WithGrantRead(const Aws::String& value) { SetGrantRead(value); return *this;}

    /**
     * Allows grantee to list the objects in the bucket.
     */
    inline CreateBucketRequest& WithGrantRead(Aws::String&& value) { SetGrantRead(std::move(value)); return *this;}

    /**
     * Allows grantee to list the objects in the bucket.
     */
    inline CreateBucketRequest& WithGrantRead(const char* value) { SetGrantRead(value); return *this;}


    /**
     * Allows grantee to read the bucket ACL.
     */
    inline const Aws::String& GetGrantReadACP() const{ return m_grantReadACP; }

    /**
     * Allows grantee to read the bucket ACL.
     */
    inline void SetGrantReadACP(const Aws::String& value) { m_grantReadACPHasBeenSet = true; m_grantReadACP = value; }

    /**
     * Allows grantee to read the bucket ACL.
     */
    inline void SetGrantReadACP(Aws::String&& value) { m_grantReadACPHasBeenSet = true; m_grantReadACP = std::move(value); }

    /**
     * Allows grantee to read the bucket ACL.
     */
    inline void SetGrantReadACP(const char* value) { m_grantReadACPHasBeenSet = true; m_grantReadACP.assign(value); }

    /**
     * Allows grantee to read the bucket ACL.
     */
    inline CreateBucketRequest& WithGrantReadACP(const Aws::String& value) { SetGrantReadACP(value); return *this;}

    /**
     * Allows grantee to read the bucket ACL.
     */
    inline CreateBucketRequest& WithGrantReadACP(Aws::String&& value) { SetGrantReadACP(std::move(value)); return *this;}

    /**
     * Allows grantee to read the bucket ACL.
     */
    inline CreateBucketRequest& WithGrantReadACP(const char* value) { SetGrantReadACP(value); return *this;}


    /**
     * Allows grantee to create, overwrite, and delete any object in the bucket.
     */
    inline const Aws::String& GetGrantWrite() const{ return m_grantWrite; }

    /**
     * Allows grantee to create, overwrite, and delete any object in the bucket.
     */
    inline void SetGrantWrite(const Aws::String& value) { m_grantWriteHasBeenSet = true; m_grantWrite = value; }

    /**
     * Allows grantee to create, overwrite, and delete any object in the bucket.
     */
    inline void SetGrantWrite(Aws::String&& value) { m_grantWriteHasBeenSet = true; m_grantWrite = std::move(value); }

    /**
     * Allows grantee to create, overwrite, and delete any object in the bucket.
     */
    inline void SetGrantWrite(const char* value) { m_grantWriteHasBeenSet = true; m_grantWrite.assign(value); }

    /**
     * Allows grantee to create, overwrite, and delete any object in the bucket.
     */
    inline CreateBucketRequest& WithGrantWrite(const Aws::String& value) { SetGrantWrite(value); return *this;}

    /**
     * Allows grantee to create, overwrite, and delete any object in the bucket.
     */
    inline CreateBucketRequest& WithGrantWrite(Aws::String&& value) { SetGrantWrite(std::move(value)); return *this;}

    /**
     * Allows grantee to create, overwrite, and delete any object in the bucket.
     */
    inline CreateBucketRequest& WithGrantWrite(const char* value) { SetGrantWrite(value); return *this;}


    /**
     * Allows grantee to write the ACL for the applicable bucket.
     */
    inline const Aws::String& GetGrantWriteACP() const{ return m_grantWriteACP; }

    /**
     * Allows grantee to write the ACL for the applicable bucket.
     */
    inline void SetGrantWriteACP(const Aws::String& value) { m_grantWriteACPHasBeenSet = true; m_grantWriteACP = value; }

    /**
     * Allows grantee to write the ACL for the applicable bucket.
     */
    inline void SetGrantWriteACP(Aws::String&& value) { m_grantWriteACPHasBeenSet = true; m_grantWriteACP = std::move(value); }

    /**
     * Allows grantee to write the ACL for the applicable bucket.
     */
    inline void SetGrantWriteACP(const char* value) { m_grantWriteACPHasBeenSet = true; m_grantWriteACP.assign(value); }

    /**
     * Allows grantee to write the ACL for the applicable bucket.
     */
    inline CreateBucketRequest& WithGrantWriteACP(const Aws::String& value) { SetGrantWriteACP(value); return *this;}

    /**
     * Allows grantee to write the ACL for the applicable bucket.
     */
    inline CreateBucketRequest& WithGrantWriteACP(Aws::String&& value) { SetGrantWriteACP(std::move(value)); return *this;}

    /**
     * Allows grantee to write the ACL for the applicable bucket.
     */
    inline CreateBucketRequest& WithGrantWriteACP(const char* value) { SetGrantWriteACP(value); return *this;}

  private:

    BucketCannedACL m_aCL;
    bool m_aCLHasBeenSet;

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    CreateBucketConfiguration m_createBucketConfiguration;
    bool m_createBucketConfigurationHasBeenSet;

    Aws::String m_grantFullControl;
    bool m_grantFullControlHasBeenSet;

    Aws::String m_grantRead;
    bool m_grantReadHasBeenSet;

    Aws::String m_grantReadACP;
    bool m_grantReadACPHasBeenSet;

    Aws::String m_grantWrite;
    bool m_grantWriteHasBeenSet;

    Aws::String m_grantWriteACP;
    bool m_grantWriteACPHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
