import requests
from com.vmware.vapi.std.errors_client import Unauthenticated
from vmware.vapi.vsphere.client import create_vsphere_client

from ray.autoscaler._private.vsphere.utils import Constants, singleton_client


def get_unverified_session():
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


@singleton_client
class VsphereSdkProvider:
    def __init__(self, server, user, password, session_type: Constants.SessionType):
        self.server = server
        self.user = user
        self.password = password
        self.session_type = session_type
        self.vsphere_sdk_client = self.get_client()

    def get_client(self):
        session = None
        if self.session_type == Constants.SessionType.UNVERIFIED:
            session = get_unverified_session()
        else:
            # TODO: support verified context
            pass
        return create_vsphere_client(
            server=self.server,
            username=self.user,
            password=self.password,
            session=session,
        )

    def ensure_connect(self):
        try:
            # List the clusters to check the connectivity
            _ = self.vsphere_sdk_client.vcenter.Cluster.list()
        except Unauthenticated:
            self.vsphere_sdk_client = self.get_client()
        except Exception as e:
            raise RuntimeError(f"failed to ensure the connect, exception: {e}")
