# Copyright (2023 and onwards) Anyscale, Inc.
import os

# The availabiliy zone label for Ray nodes that will be set by Anyscale
ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL = "anyscale.com/availability_zone"

# Socket over which Anyscaled dataplane HTTP server is listening
ANYSCALE_DATAPLANE_SERVICE_SOCKET = os.environ.get(
    "ANYSCALE_DATAPLANE_SERVICE_SOCKET",
    "/tmp/anyscale/anyscaled/sockets/dataplane_service.sock",
)
