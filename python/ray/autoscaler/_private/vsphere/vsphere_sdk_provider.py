import requests
from vmware.vapi.vsphere.client import create_vsphere_client

from ray.autoscaler._private.vsphere.utils import Constants


class VsphereSdkProvider:
    def __init__(self, server, user, password, session_type: Constants.SessionType):
        self.server = server
        self.user = user
        self.password = password
        self.session_type = session_type
        session = None
        if self.session_type == Constants.SessionType.UNVERIFIED:
            session = self.get_unverified_session()
        vsphere_sdk_client = create_vsphere_client(
            server=server,
            username=user,
            password=password,
            session=session,
        )
        self.vsphere_sdk_client = vsphere_sdk_client

    def get_unverified_session(self):
        """
        vCenter provisioned internally have SSH certificates
        expired so we use unverified session. Find out what
        could be done for production.

        Get a requests session with cert verification disabled.
        Also disable the insecure warnings message.
        Note this is not recommended in production code.
        @return: a requests session with verification disabled.
        """
        session = requests.session()
        session.verify = False
        requests.packages.urllib3.disable_warnings()
        return session
