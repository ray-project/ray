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
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/TargetGrant.h>
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

  class AWS_S3_API LoggingEnabled
  {
  public:
    LoggingEnabled();
    LoggingEnabled(const Aws::Utils::Xml::XmlNode& xmlNode);
    LoggingEnabled& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Specifies the bucket where you want Amazon S3 to store server access logs. You
     * can have your logs delivered to any bucket that you own, including the same
     * bucket that is being logged. You can also configure multiple buckets to deliver
     * their logs to the same target bucket. In this case you should choose a different
     * TargetPrefix for each source bucket so that the delivered log files can be
     * distinguished by key.
     */
    inline const Aws::String& GetTargetBucket() const{ return m_targetBucket; }

    /**
     * Specifies the bucket where you want Amazon S3 to store server access logs. You
     * can have your logs delivered to any bucket that you own, including the same
     * bucket that is being logged. You can also configure multiple buckets to deliver
     * their logs to the same target bucket. In this case you should choose a different
     * TargetPrefix for each source bucket so that the delivered log files can be
     * distinguished by key.
     */
    inline void SetTargetBucket(const Aws::String& value) { m_targetBucketHasBeenSet = true; m_targetBucket = value; }

    /**
     * Specifies the bucket where you want Amazon S3 to store server access logs. You
     * can have your logs delivered to any bucket that you own, including the same
     * bucket that is being logged. You can also configure multiple buckets to deliver
     * their logs to the same target bucket. In this case you should choose a different
     * TargetPrefix for each source bucket so that the delivered log files can be
     * distinguished by key.
     */
    inline void SetTargetBucket(Aws::String&& value) { m_targetBucketHasBeenSet = true; m_targetBucket = std::move(value); }

    /**
     * Specifies the bucket where you want Amazon S3 to store server access logs. You
     * can have your logs delivered to any bucket that you own, including the same
     * bucket that is being logged. You can also configure multiple buckets to deliver
     * their logs to the same target bucket. In this case you should choose a different
     * TargetPrefix for each source bucket so that the delivered log files can be
     * distinguished by key.
     */
    inline void SetTargetBucket(const char* value) { m_targetBucketHasBeenSet = true; m_targetBucket.assign(value); }

    /**
     * Specifies the bucket where you want Amazon S3 to store server access logs. You
     * can have your logs delivered to any bucket that you own, including the same
     * bucket that is being logged. You can also configure multiple buckets to deliver
     * their logs to the same target bucket. In this case you should choose a different
     * TargetPrefix for each source bucket so that the delivered log files can be
     * distinguished by key.
     */
    inline LoggingEnabled& WithTargetBucket(const Aws::String& value) { SetTargetBucket(value); return *this;}

    /**
     * Specifies the bucket where you want Amazon S3 to store server access logs. You
     * can have your logs delivered to any bucket that you own, including the same
     * bucket that is being logged. You can also configure multiple buckets to deliver
     * their logs to the same target bucket. In this case you should choose a different
     * TargetPrefix for each source bucket so that the delivered log files can be
     * distinguished by key.
     */
    inline LoggingEnabled& WithTargetBucket(Aws::String&& value) { SetTargetBucket(std::move(value)); return *this;}

    /**
     * Specifies the bucket where you want Amazon S3 to store server access logs. You
     * can have your logs delivered to any bucket that you own, including the same
     * bucket that is being logged. You can also configure multiple buckets to deliver
     * their logs to the same target bucket. In this case you should choose a different
     * TargetPrefix for each source bucket so that the delivered log files can be
     * distinguished by key.
     */
    inline LoggingEnabled& WithTargetBucket(const char* value) { SetTargetBucket(value); return *this;}


    
    inline const Aws::Vector<TargetGrant>& GetTargetGrants() const{ return m_targetGrants; }

    
    inline void SetTargetGrants(const Aws::Vector<TargetGrant>& value) { m_targetGrantsHasBeenSet = true; m_targetGrants = value; }

    
    inline void SetTargetGrants(Aws::Vector<TargetGrant>&& value) { m_targetGrantsHasBeenSet = true; m_targetGrants = std::move(value); }

    
    inline LoggingEnabled& WithTargetGrants(const Aws::Vector<TargetGrant>& value) { SetTargetGrants(value); return *this;}

    
    inline LoggingEnabled& WithTargetGrants(Aws::Vector<TargetGrant>&& value) { SetTargetGrants(std::move(value)); return *this;}

    
    inline LoggingEnabled& AddTargetGrants(const TargetGrant& value) { m_targetGrantsHasBeenSet = true; m_targetGrants.push_back(value); return *this; }

    
    inline LoggingEnabled& AddTargetGrants(TargetGrant&& value) { m_targetGrantsHasBeenSet = true; m_targetGrants.push_back(std::move(value)); return *this; }


    /**
     * This element lets you specify a prefix for the keys that the log files will be
     * stored under.
     */
    inline const Aws::String& GetTargetPrefix() const{ return m_targetPrefix; }

    /**
     * This element lets you specify a prefix for the keys that the log files will be
     * stored under.
     */
    inline void SetTargetPrefix(const Aws::String& value) { m_targetPrefixHasBeenSet = true; m_targetPrefix = value; }

    /**
     * This element lets you specify a prefix for the keys that the log files will be
     * stored under.
     */
    inline void SetTargetPrefix(Aws::String&& value) { m_targetPrefixHasBeenSet = true; m_targetPrefix = std::move(value); }

    /**
     * This element lets you specify a prefix for the keys that the log files will be
     * stored under.
     */
    inline void SetTargetPrefix(const char* value) { m_targetPrefixHasBeenSet = true; m_targetPrefix.assign(value); }

    /**
     * This element lets you specify a prefix for the keys that the log files will be
     * stored under.
     */
    inline LoggingEnabled& WithTargetPrefix(const Aws::String& value) { SetTargetPrefix(value); return *this;}

    /**
     * This element lets you specify a prefix for the keys that the log files will be
     * stored under.
     */
    inline LoggingEnabled& WithTargetPrefix(Aws::String&& value) { SetTargetPrefix(std::move(value)); return *this;}

    /**
     * This element lets you specify a prefix for the keys that the log files will be
     * stored under.
     */
    inline LoggingEnabled& WithTargetPrefix(const char* value) { SetTargetPrefix(value); return *this;}

  private:

    Aws::String m_targetBucket;
    bool m_targetBucketHasBeenSet;

    Aws::Vector<TargetGrant> m_targetGrants;
    bool m_targetGrantsHasBeenSet;

    Aws::String m_targetPrefix;
    bool m_targetPrefixHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
