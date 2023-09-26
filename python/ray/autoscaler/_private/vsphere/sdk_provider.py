import logging
from enum import Enum

from ray.autoscaler._private.vsphere.pyvmomi_sdk_provider import PyvmomiSdkProvider
from ray.autoscaler._private.vsphere.utils import Constants
from ray.autoscaler._private.vsphere.vsphere_sdk_provider import VsphereSdkProvider

logger = logging.getLogger(__name__)

# Global Vars, one instance
pyvmomi_sdk_provider = None
vsphere_sdk_provider = None


class VmwSdkProviderFactory:
    class ClientType(Enum):
        # Enum for SDK clients
        PYVMOMI_SDK = "pyvmomi"
        AUTOMATION_SDK = "automation_sdk"

    def __init__(self, server, user, password, client_type: ClientType):
        self.server = server
        self.user = user
        self.password = password

        if client_type == self.ClientType.PYVMOMI_SDK:
            self.sdk_provider = self.get_pyvmomi_sdk_provider()
        elif client_type == self.ClientType.AUTOMATION_SDK:
            self.sdk_provider = self.get_vsphere_sdk_provider()
        else:
            raise ValueError(
                f"Unknown client {client_type}, supported client types"
                f"are: {list(self.ClientType)}"
            )

    def get_pyvmomi_sdk_provider(self):
        global pyvmomi_sdk_provider
        if pyvmomi_sdk_provider is None:
            pyvmomi_sdk_provider = PyvmomiSdkProvider(
                self.server, self.user, self.password, Constants.SessionType.UNVERIFIED
            )
        return pyvmomi_sdk_provider

    def get_vsphere_sdk_provider(self):
        global vsphere_sdk_provider
        if vsphere_sdk_provider is None:
            vsphere_sdk_provider = VsphereSdkProvider(
                self.server, self.user, self.password, Constants.SessionType.UNVERIFIED
            )
        return vsphere_sdk_provider
