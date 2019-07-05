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
#include <aws/s3/model/LifecycleExpiration.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/LifecycleRuleFilter.h>
#include <aws/s3/model/ExpirationStatus.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/NoncurrentVersionExpiration.h>
#include <aws/s3/model/AbortIncompleteMultipartUpload.h>
#include <aws/s3/model/Transition.h>
#include <aws/s3/model/NoncurrentVersionTransition.h>
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

  class AWS_S3_API LifecycleRule
  {
  public:
    LifecycleRule();
    LifecycleRule(const Aws::Utils::Xml::XmlNode& xmlNode);
    LifecycleRule& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    
    inline const LifecycleExpiration& GetExpiration() const{ return m_expiration; }

    
    inline void SetExpiration(const LifecycleExpiration& value) { m_expirationHasBeenSet = true; m_expiration = value; }

    
    inline void SetExpiration(LifecycleExpiration&& value) { m_expirationHasBeenSet = true; m_expiration = std::move(value); }

    
    inline LifecycleRule& WithExpiration(const LifecycleExpiration& value) { SetExpiration(value); return *this;}

    
    inline LifecycleRule& WithExpiration(LifecycleExpiration&& value) { SetExpiration(std::move(value)); return *this;}


    /**
     * Unique identifier for the rule. The value cannot be longer than 255 characters.
     */
    inline const Aws::String& GetID() const{ return m_iD; }

    /**
     * Unique identifier for the rule. The value cannot be longer than 255 characters.
     */
    inline void SetID(const Aws::String& value) { m_iDHasBeenSet = true; m_iD = value; }

    /**
     * Unique identifier for the rule. The value cannot be longer than 255 characters.
     */
    inline void SetID(Aws::String&& value) { m_iDHasBeenSet = true; m_iD = std::move(value); }

    /**
     * Unique identifier for the rule. The value cannot be longer than 255 characters.
     */
    inline void SetID(const char* value) { m_iDHasBeenSet = true; m_iD.assign(value); }

    /**
     * Unique identifier for the rule. The value cannot be longer than 255 characters.
     */
    inline LifecycleRule& WithID(const Aws::String& value) { SetID(value); return *this;}

    /**
     * Unique identifier for the rule. The value cannot be longer than 255 characters.
     */
    inline LifecycleRule& WithID(Aws::String&& value) { SetID(std::move(value)); return *this;}

    /**
     * Unique identifier for the rule. The value cannot be longer than 255 characters.
     */
    inline LifecycleRule& WithID(const char* value) { SetID(value); return *this;}


    
    inline const LifecycleRuleFilter& GetFilter() const{ return m_filter; }

    
    inline void SetFilter(const LifecycleRuleFilter& value) { m_filterHasBeenSet = true; m_filter = value; }

    
    inline void SetFilter(LifecycleRuleFilter&& value) { m_filterHasBeenSet = true; m_filter = std::move(value); }

    
    inline LifecycleRule& WithFilter(const LifecycleRuleFilter& value) { SetFilter(value); return *this;}

    
    inline LifecycleRule& WithFilter(LifecycleRuleFilter&& value) { SetFilter(std::move(value)); return *this;}


    /**
     * If 'Enabled', the rule is currently being applied. If 'Disabled', the rule is
     * not currently being applied.
     */
    inline const ExpirationStatus& GetStatus() const{ return m_status; }

    /**
     * If 'Enabled', the rule is currently being applied. If 'Disabled', the rule is
     * not currently being applied.
     */
    inline void SetStatus(const ExpirationStatus& value) { m_statusHasBeenSet = true; m_status = value; }

    /**
     * If 'Enabled', the rule is currently being applied. If 'Disabled', the rule is
     * not currently being applied.
     */
    inline void SetStatus(ExpirationStatus&& value) { m_statusHasBeenSet = true; m_status = std::move(value); }

    /**
     * If 'Enabled', the rule is currently being applied. If 'Disabled', the rule is
     * not currently being applied.
     */
    inline LifecycleRule& WithStatus(const ExpirationStatus& value) { SetStatus(value); return *this;}

    /**
     * If 'Enabled', the rule is currently being applied. If 'Disabled', the rule is
     * not currently being applied.
     */
    inline LifecycleRule& WithStatus(ExpirationStatus&& value) { SetStatus(std::move(value)); return *this;}


    
    inline const Aws::Vector<Transition>& GetTransitions() const{ return m_transitions; }

    
    inline void SetTransitions(const Aws::Vector<Transition>& value) { m_transitionsHasBeenSet = true; m_transitions = value; }

    
    inline void SetTransitions(Aws::Vector<Transition>&& value) { m_transitionsHasBeenSet = true; m_transitions = std::move(value); }

    
    inline LifecycleRule& WithTransitions(const Aws::Vector<Transition>& value) { SetTransitions(value); return *this;}

    
    inline LifecycleRule& WithTransitions(Aws::Vector<Transition>&& value) { SetTransitions(std::move(value)); return *this;}

    
    inline LifecycleRule& AddTransitions(const Transition& value) { m_transitionsHasBeenSet = true; m_transitions.push_back(value); return *this; }

    
    inline LifecycleRule& AddTransitions(Transition&& value) { m_transitionsHasBeenSet = true; m_transitions.push_back(std::move(value)); return *this; }


    
    inline const Aws::Vector<NoncurrentVersionTransition>& GetNoncurrentVersionTransitions() const{ return m_noncurrentVersionTransitions; }

    
    inline void SetNoncurrentVersionTransitions(const Aws::Vector<NoncurrentVersionTransition>& value) { m_noncurrentVersionTransitionsHasBeenSet = true; m_noncurrentVersionTransitions = value; }

    
    inline void SetNoncurrentVersionTransitions(Aws::Vector<NoncurrentVersionTransition>&& value) { m_noncurrentVersionTransitionsHasBeenSet = true; m_noncurrentVersionTransitions = std::move(value); }

    
    inline LifecycleRule& WithNoncurrentVersionTransitions(const Aws::Vector<NoncurrentVersionTransition>& value) { SetNoncurrentVersionTransitions(value); return *this;}

    
    inline LifecycleRule& WithNoncurrentVersionTransitions(Aws::Vector<NoncurrentVersionTransition>&& value) { SetNoncurrentVersionTransitions(std::move(value)); return *this;}

    
    inline LifecycleRule& AddNoncurrentVersionTransitions(const NoncurrentVersionTransition& value) { m_noncurrentVersionTransitionsHasBeenSet = true; m_noncurrentVersionTransitions.push_back(value); return *this; }

    
    inline LifecycleRule& AddNoncurrentVersionTransitions(NoncurrentVersionTransition&& value) { m_noncurrentVersionTransitionsHasBeenSet = true; m_noncurrentVersionTransitions.push_back(std::move(value)); return *this; }


    
    inline const NoncurrentVersionExpiration& GetNoncurrentVersionExpiration() const{ return m_noncurrentVersionExpiration; }

    
    inline void SetNoncurrentVersionExpiration(const NoncurrentVersionExpiration& value) { m_noncurrentVersionExpirationHasBeenSet = true; m_noncurrentVersionExpiration = value; }

    
    inline void SetNoncurrentVersionExpiration(NoncurrentVersionExpiration&& value) { m_noncurrentVersionExpirationHasBeenSet = true; m_noncurrentVersionExpiration = std::move(value); }

    
    inline LifecycleRule& WithNoncurrentVersionExpiration(const NoncurrentVersionExpiration& value) { SetNoncurrentVersionExpiration(value); return *this;}

    
    inline LifecycleRule& WithNoncurrentVersionExpiration(NoncurrentVersionExpiration&& value) { SetNoncurrentVersionExpiration(std::move(value)); return *this;}


    
    inline const AbortIncompleteMultipartUpload& GetAbortIncompleteMultipartUpload() const{ return m_abortIncompleteMultipartUpload; }

    
    inline void SetAbortIncompleteMultipartUpload(const AbortIncompleteMultipartUpload& value) { m_abortIncompleteMultipartUploadHasBeenSet = true; m_abortIncompleteMultipartUpload = value; }

    
    inline void SetAbortIncompleteMultipartUpload(AbortIncompleteMultipartUpload&& value) { m_abortIncompleteMultipartUploadHasBeenSet = true; m_abortIncompleteMultipartUpload = std::move(value); }

    
    inline LifecycleRule& WithAbortIncompleteMultipartUpload(const AbortIncompleteMultipartUpload& value) { SetAbortIncompleteMultipartUpload(value); return *this;}

    
    inline LifecycleRule& WithAbortIncompleteMultipartUpload(AbortIncompleteMultipartUpload&& value) { SetAbortIncompleteMultipartUpload(std::move(value)); return *this;}

  private:

    LifecycleExpiration m_expiration;
    bool m_expirationHasBeenSet;

    Aws::String m_iD;
    bool m_iDHasBeenSet;

    LifecycleRuleFilter m_filter;
    bool m_filterHasBeenSet;

    ExpirationStatus m_status;
    bool m_statusHasBeenSet;

    Aws::Vector<Transition> m_transitions;
    bool m_transitionsHasBeenSet;

    Aws::Vector<NoncurrentVersionTransition> m_noncurrentVersionTransitions;
    bool m_noncurrentVersionTransitionsHasBeenSet;

    NoncurrentVersionExpiration m_noncurrentVersionExpiration;
    bool m_noncurrentVersionExpirationHasBeenSet;

    AbortIncompleteMultipartUpload m_abortIncompleteMultipartUpload;
    bool m_abortIncompleteMultipartUploadHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
