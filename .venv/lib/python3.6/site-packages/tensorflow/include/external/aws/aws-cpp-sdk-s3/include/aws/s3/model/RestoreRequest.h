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
#include <aws/s3/model/GlacierJobParameters.h>
#include <aws/s3/model/RestoreRequestType.h>
#include <aws/s3/model/Tier.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/SelectParameters.h>
#include <aws/s3/model/OutputLocation.h>
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

  /**
   * Container for restore job parameters.<p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/RestoreRequest">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API RestoreRequest
  {
  public:
    RestoreRequest();
    RestoreRequest(const Aws::Utils::Xml::XmlNode& xmlNode);
    RestoreRequest& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Lifetime of the active copy in days. Do not use with restores that specify
     * OutputLocation.
     */
    inline int GetDays() const{ return m_days; }

    /**
     * Lifetime of the active copy in days. Do not use with restores that specify
     * OutputLocation.
     */
    inline void SetDays(int value) { m_daysHasBeenSet = true; m_days = value; }

    /**
     * Lifetime of the active copy in days. Do not use with restores that specify
     * OutputLocation.
     */
    inline RestoreRequest& WithDays(int value) { SetDays(value); return *this;}


    /**
     * Glacier related parameters pertaining to this job. Do not use with restores that
     * specify OutputLocation.
     */
    inline const GlacierJobParameters& GetGlacierJobParameters() const{ return m_glacierJobParameters; }

    /**
     * Glacier related parameters pertaining to this job. Do not use with restores that
     * specify OutputLocation.
     */
    inline void SetGlacierJobParameters(const GlacierJobParameters& value) { m_glacierJobParametersHasBeenSet = true; m_glacierJobParameters = value; }

    /**
     * Glacier related parameters pertaining to this job. Do not use with restores that
     * specify OutputLocation.
     */
    inline void SetGlacierJobParameters(GlacierJobParameters&& value) { m_glacierJobParametersHasBeenSet = true; m_glacierJobParameters = std::move(value); }

    /**
     * Glacier related parameters pertaining to this job. Do not use with restores that
     * specify OutputLocation.
     */
    inline RestoreRequest& WithGlacierJobParameters(const GlacierJobParameters& value) { SetGlacierJobParameters(value); return *this;}

    /**
     * Glacier related parameters pertaining to this job. Do not use with restores that
     * specify OutputLocation.
     */
    inline RestoreRequest& WithGlacierJobParameters(GlacierJobParameters&& value) { SetGlacierJobParameters(std::move(value)); return *this;}


    /**
     * Type of restore request.
     */
    inline const RestoreRequestType& GetType() const{ return m_type; }

    /**
     * Type of restore request.
     */
    inline void SetType(const RestoreRequestType& value) { m_typeHasBeenSet = true; m_type = value; }

    /**
     * Type of restore request.
     */
    inline void SetType(RestoreRequestType&& value) { m_typeHasBeenSet = true; m_type = std::move(value); }

    /**
     * Type of restore request.
     */
    inline RestoreRequest& WithType(const RestoreRequestType& value) { SetType(value); return *this;}

    /**
     * Type of restore request.
     */
    inline RestoreRequest& WithType(RestoreRequestType&& value) { SetType(std::move(value)); return *this;}


    /**
     * Glacier retrieval tier at which the restore will be processed.
     */
    inline const Tier& GetTier() const{ return m_tier; }

    /**
     * Glacier retrieval tier at which the restore will be processed.
     */
    inline void SetTier(const Tier& value) { m_tierHasBeenSet = true; m_tier = value; }

    /**
     * Glacier retrieval tier at which the restore will be processed.
     */
    inline void SetTier(Tier&& value) { m_tierHasBeenSet = true; m_tier = std::move(value); }

    /**
     * Glacier retrieval tier at which the restore will be processed.
     */
    inline RestoreRequest& WithTier(const Tier& value) { SetTier(value); return *this;}

    /**
     * Glacier retrieval tier at which the restore will be processed.
     */
    inline RestoreRequest& WithTier(Tier&& value) { SetTier(std::move(value)); return *this;}


    /**
     * The optional description for the job.
     */
    inline const Aws::String& GetDescription() const{ return m_description; }

    /**
     * The optional description for the job.
     */
    inline void SetDescription(const Aws::String& value) { m_descriptionHasBeenSet = true; m_description = value; }

    /**
     * The optional description for the job.
     */
    inline void SetDescription(Aws::String&& value) { m_descriptionHasBeenSet = true; m_description = std::move(value); }

    /**
     * The optional description for the job.
     */
    inline void SetDescription(const char* value) { m_descriptionHasBeenSet = true; m_description.assign(value); }

    /**
     * The optional description for the job.
     */
    inline RestoreRequest& WithDescription(const Aws::String& value) { SetDescription(value); return *this;}

    /**
     * The optional description for the job.
     */
    inline RestoreRequest& WithDescription(Aws::String&& value) { SetDescription(std::move(value)); return *this;}

    /**
     * The optional description for the job.
     */
    inline RestoreRequest& WithDescription(const char* value) { SetDescription(value); return *this;}


    /**
     * Describes the parameters for Select job types.
     */
    inline const SelectParameters& GetSelectParameters() const{ return m_selectParameters; }

    /**
     * Describes the parameters for Select job types.
     */
    inline void SetSelectParameters(const SelectParameters& value) { m_selectParametersHasBeenSet = true; m_selectParameters = value; }

    /**
     * Describes the parameters for Select job types.
     */
    inline void SetSelectParameters(SelectParameters&& value) { m_selectParametersHasBeenSet = true; m_selectParameters = std::move(value); }

    /**
     * Describes the parameters for Select job types.
     */
    inline RestoreRequest& WithSelectParameters(const SelectParameters& value) { SetSelectParameters(value); return *this;}

    /**
     * Describes the parameters for Select job types.
     */
    inline RestoreRequest& WithSelectParameters(SelectParameters&& value) { SetSelectParameters(std::move(value)); return *this;}


    /**
     * Describes the location where the restore job's output is stored.
     */
    inline const OutputLocation& GetOutputLocation() const{ return m_outputLocation; }

    /**
     * Describes the location where the restore job's output is stored.
     */
    inline void SetOutputLocation(const OutputLocation& value) { m_outputLocationHasBeenSet = true; m_outputLocation = value; }

    /**
     * Describes the location where the restore job's output is stored.
     */
    inline void SetOutputLocation(OutputLocation&& value) { m_outputLocationHasBeenSet = true; m_outputLocation = std::move(value); }

    /**
     * Describes the location where the restore job's output is stored.
     */
    inline RestoreRequest& WithOutputLocation(const OutputLocation& value) { SetOutputLocation(value); return *this;}

    /**
     * Describes the location where the restore job's output is stored.
     */
    inline RestoreRequest& WithOutputLocation(OutputLocation&& value) { SetOutputLocation(std::move(value)); return *this;}

  private:

    int m_days;
    bool m_daysHasBeenSet;

    GlacierJobParameters m_glacierJobParameters;
    bool m_glacierJobParametersHasBeenSet;

    RestoreRequestType m_type;
    bool m_typeHasBeenSet;

    Tier m_tier;
    bool m_tierHasBeenSet;

    Aws::String m_description;
    bool m_descriptionHasBeenSet;

    SelectParameters m_selectParameters;
    bool m_selectParametersHasBeenSet;

    OutputLocation m_outputLocation;
    bool m_outputLocationHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
