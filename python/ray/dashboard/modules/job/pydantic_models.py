from ray._common.pydantic_compat import PYDANTIC_INSTALLED

# Pydantic is not part of the minimal Ray installation.
# TODO(chiayi): This file is kept for backward compatibility because many files still import from here.
# The actual definitions have been moved to ray.dashboard.schemas.
# Once all importers are updated to use ray.dashboard.schemas, this file can be removed.
if PYDANTIC_INSTALLED:
    from ray.dashboard.pydantic_models import DriverInfo, JobDetails, JobType

else:
    DriverInfo = None
    JobType = None
    JobDetails = None
