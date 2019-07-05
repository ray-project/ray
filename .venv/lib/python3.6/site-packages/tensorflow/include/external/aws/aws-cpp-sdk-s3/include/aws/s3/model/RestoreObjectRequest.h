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
#include <aws/s3/model/RestoreRequest.h>
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
  class AWS_S3_API RestoreObjectRequest : public S3Request
  {
  public:
    RestoreObjectRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "RestoreObject"; }

    Aws::String SerializePayload() const override;

    void AddQueryStringParameters(Aws::Http::URI& uri) const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    
    inline RestoreObjectRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    
    inline RestoreObjectRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    
    inline RestoreObjectRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    
    inline const Aws::String& GetKey() const{ return m_key; }

    
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    
    inline RestoreObjectRequest& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    
    inline RestoreObjectRequest& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    
    inline RestoreObjectRequest& WithKey(const char* value) { SetKey(value); return *this;}


    
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    
    inline void SetVersionId(const Aws::String& value) { m_versionIdHasBeenSet = true; m_versionId = value; }

    
    inline void SetVersionId(Aws::String&& value) { m_versionIdHasBeenSet = true; m_versionId = std::move(value); }

    
    inline void SetVersionId(const char* value) { m_versionIdHasBeenSet = true; m_versionId.assign(value); }

    
    inline RestoreObjectRequest& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    
    inline RestoreObjectRequest& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    
    inline RestoreObjectRequest& WithVersionId(const char* value) { SetVersionId(value); return *this;}


    
    inline const RestoreRequest& GetRestoreRequest() const{ return m_restoreRequest; }

    
    inline void SetRestoreRequest(const RestoreRequest& value) { m_restoreRequestHasBeenSet = true; m_restoreRequest = value; }

    
    inline void SetRestoreRequest(RestoreRequest&& value) { m_restoreRequestHasBeenSet = true; m_restoreRequest = std::move(value); }

    
    inline RestoreObjectRequest& WithRestoreRequest(const RestoreRequest& value) { SetRestoreRequest(value); return *this;}

    
    inline RestoreObjectRequest& WithRestoreRequest(RestoreRequest&& value) { SetRestoreRequest(std::move(value)); return *this;}


    
    inline const RequestPayer& GetRequestPayer() const{ return m_requestPayer; }

    
    inline void SetRequestPayer(const RequestPayer& value) { m_requestPayerHasBeenSet = true; m_requestPayer = value; }

    
    inline void SetRequestPayer(RequestPayer&& value) { m_requestPayerHasBeenSet = true; m_requestPayer = std::move(value); }

    
    inline RestoreObjectRequest& WithRequestPayer(const RequestPayer& value) { SetRequestPayer(value); return *this;}

    
    inline RestoreObjectRequest& WithRequestPayer(RequestPayer&& value) { SetRequestPayer(std::move(value)); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_key;
    bool m_keyHasBeenSet;

    Aws::String m_versionId;
    bool m_versionIdHasBeenSet;

    RestoreRequest m_restoreRequest;
    bool m_restoreRequestHasBeenSet;

    RequestPayer m_requestPayer;
    bool m_requestPayerHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
