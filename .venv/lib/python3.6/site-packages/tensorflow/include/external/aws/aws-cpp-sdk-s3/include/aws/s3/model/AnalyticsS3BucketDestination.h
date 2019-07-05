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
#include <aws/s3/model/AnalyticsS3ExportFileFormat.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <utility>

namespace Aws
{
namespace Utils
{
namespace Xml
{
  class XmlNode;
} // namespace Xml
} // namespace Utils
namespace S3
{
namespace Model
{

  class AWS_S3_API AnalyticsS3BucketDestination
  {
  public:
    AnalyticsS3BucketDestination();
    AnalyticsS3BucketDestination(const Aws::Utils::Xml::XmlNode& xmlNode);
    AnalyticsS3BucketDestination& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * The file format used when exporting data to Amazon S3.
     */
    inline const AnalyticsS3ExportFileFormat& GetFormat() const{ return m_format; }

    /**
     * The file format used when exporting data to Amazon S3.
     */
    inline void SetFormat(const AnalyticsS3ExportFileFormat& value) { m_formatHasBeenSet = true; m_format = value; }

    /**
     * The file format used when exporting data to Amazon S3.
     */
    inline void SetFormat(AnalyticsS3ExportFileFormat&& value) { m_formatHasBeenSet = true; m_format = std::move(value); }

    /**
     * The file format used when exporting data to Amazon S3.
     */
    inline AnalyticsS3BucketDestination& WithFormat(const AnalyticsS3ExportFileFormat& value) { SetFormat(value); return *this;}

    /**
     * The file format used when exporting data to Amazon S3.
     */
    inline AnalyticsS3BucketDestination& WithFormat(AnalyticsS3ExportFileFormat&& value) { SetFormat(std::move(value)); return *this;}


    /**
     * The account ID that owns the destination bucket. If no account ID is provided,
     * the owner will not be validated prior to exporting data.
     */
    inline const Aws::String& GetBucketAccountId() const{ return m_bucketAccountId; }

    /**
     * The account ID that owns the destination bucket. If no account ID is provided,
     * the owner will not be validated prior to exporting data.
     */
    inline void SetBucketAccountId(const Aws::String& value) { m_bucketAccountIdHasBeenSet = true; m_bucketAccountId = value; }

    /**
     * The account ID that owns the destination bucket. If no account ID is provided,
     * the owner will not be validated prior to exporting data.
     */
    inline void SetBucketAccountId(Aws::String&& value) { m_bucketAccountIdHasBeenSet = true; m_bucketAccountId = std::move(value); }

    /**
     * The account ID that owns the destination bucket. If no account ID is provided,
     * the owner will not be validated prior to exporting data.
     */
    inline void SetBucketAccountId(const char* value) { m_bucketAccountIdHasBeenSet = true; m_bucketAccountId.assign(value); }

    /**
     * The account ID that owns the destination bucket. If no account ID is provided,
     * the owner will not be validated prior to exporting data.
     */
    inline AnalyticsS3BucketDestination& WithBucketAccountId(const Aws::String& value) { SetBucketAccountId(value); return *this;}

    /**
     * The account ID that owns the destination bucket. If no account ID is provided,
     * the owner will not be validated prior to exporting data.
     */
    inline AnalyticsS3BucketDestination& WithBucketAccountId(Aws::String&& value) { SetBucketAccountId(std::move(value)); return *this;}

    /**
     * The account ID that owns the destination bucket. If no account ID is provided,
     * the owner will not be validated prior to exporting data.
     */
    inline AnalyticsS3BucketDestination& WithBucketAccountId(const char* value) { SetBucketAccountId(value); return *this;}


    /**
     * The Amazon resource name (ARN) of the bucket to which data is exported.
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * The Amazon resource name (ARN) of the bucket to which data is exported.
     */
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /**
     * The Amazon resource name (ARN) of the bucket to which data is exported.
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /**
     * The Amazon resource name (ARN) of the bucket to which data is exported.
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /**
     * The Amazon resource name (ARN) of the bucket to which data is exported.
     */
    inline AnalyticsS3BucketDestination& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * The Amazon resource name (ARN) of the bucket to which data is exported.
     */
    inline AnalyticsS3BucketDestination& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * The Amazon resource name (ARN) of the bucket to which data is exported.
     */
    inline AnalyticsS3BucketDestination& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * The prefix to use when exporting data. The exported data begins with this
     * prefix.
     */
    inline const Aws::String& GetPrefix() const{ return m_prefix; }

    /**
     * The prefix to use when exporting data. The exported data begins with this
     * prefix.
     */
    inline void SetPrefix(const Aws::String& value) { m_prefixHasBeenSet = true; m_prefix = value; }

    /**
     * The prefix to use when exporting data. The exported data begins with this
     * prefix.
     */
    inline void SetPrefix(Aws::String&& value) { m_prefixHasBeenSet = true; m_prefix = std::move(value); }

    /**
     * The prefix to use when exporting data. The exported data begins with this
     * prefix.
     */
    inline void SetPrefix(const char* value) { m_prefixHasBeenSet = true; m_prefix.assign(value); }

    /**
     * The prefix to use when exporting data. The exported data begins with this
     * prefix.
     */
    inline AnalyticsS3BucketDestination& WithPrefix(const Aws::String& value) { SetPrefix(value); return *this;}

    /**
     * The prefix to use when exporting data. The exported data begins with this
     * prefix.
     */
    inline AnalyticsS3BucketDestination& WithPrefix(Aws::String&& value) { SetPrefix(std::move(value)); return *this;}

    /**
     * The prefix to use when exporting data. The exported data begins with this
     * prefix.
     */
    inline AnalyticsS3BucketDestination& WithPrefix(const char* value) { SetPrefix(value); return *this;}

  private:

    AnalyticsS3ExportFileFormat m_format;
    bool m_formatHasBeenSet;

    Aws::String m_bucketAccountId;
    bool m_bucketAccountIdHasBeenSet;

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_prefix;
    bool m_prefixHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
