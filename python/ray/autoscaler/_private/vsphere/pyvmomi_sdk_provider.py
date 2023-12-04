import atexit
import ssl

from pyVim.connect import Disconnect, SmartStubAdapter, VimSessionOrientedStub
from pyVmomi import vim

from ray.autoscaler._private.vsphere.utils import Constants, singleton_client


@singleton_client
class PyvmomiSdkProvider:
    def __init__(
        self,
        server,
        user,
        password,
        session_type: Constants.SessionType,
        port: int = 443,
    ):
        # Instance variables
        self.server = server
        self.user = user
        self.password = password
        self.session_type = session_type
        self.port = port

        # Instance parameters
        self.timeout = 0

        # Connect using a session oriented connection
        # Ref. https://github.com/vmware/pyvmomi/issues/347
        self.pyvmomi_sdk_client = self.get_client()
        if not self.pyvmomi_sdk_client:
            raise ValueError("Could not connect to the specified host")
        atexit.register(Disconnect, self.pyvmomi_sdk_client)

    def get_client(self):
        if self.session_type == Constants.SessionType.UNVERIFIED:
            context_obj = ssl._create_unverified_context()
        else:
            # TODO: support verified context
            pass
        credentials = VimSessionOrientedStub.makeUserLoginMethod(
            self.user, self.password
        )
        smart_stub = SmartStubAdapter(
            host=self.server,
            port=self.port,
            sslContext=context_obj,
            connectionPoolTimeout=self.timeout,
        )
        session_stub = VimSessionOrientedStub(smart_stub, credentials)
        return vim.ServiceInstance("ServiceInstance", session_stub)

    def get_pyvmomi_obj(self, vimtype, name=None, obj_id=None):
        """
        This function will return the vSphere object.
        The argument for `vimtype` can be "vim.VM", "vim.Host", "vim.Datastore", etc.
        Then either the name or the object id need to be provided.
        To check all such object information, you can go to the managed object board
        page of your vCenter Server, such as: https://<your_vc_ip/mob
        """
        if not name and not obj_id:
            # Raise runtime error because this is not user fault
            raise RuntimeError("Either name or obj id must be provided")
        if self.pyvmomi_sdk_client is None:
            raise RuntimeError("Must init pyvmomi_sdk_client first")

        container = self.pyvmomi_sdk_client.content.viewManager.CreateContainerView(
            self.pyvmomi_sdk_client.content.rootFolder, vimtype, True
        )
        # If both name and moid are provided we will prioritize name.
        if name:
            for c in container.view:
                if c.name == name:
                    return c
        elif obj_id:
            for c in container.view:
                if obj_id in str(c):
                    return c
        raise ValueError(
            f"Cannot find the object with type {vimtype} on vSphere with"
            f"name={name} and obj_id={obj_id}"
        )

    def ensure_connect(self):
        try:
            _ = self.pyvmomi_sdk_client.RetrieveContent()
        except vim.fault.NotAuthenticated:
            self.pyvmomi_sdk_client = self.get_client()
        except Exception as e:
            raise RuntimeError(f"failed to ensure the connect, exception: {e}")
