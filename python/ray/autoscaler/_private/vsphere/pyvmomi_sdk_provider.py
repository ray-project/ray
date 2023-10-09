import atexit
import ssl

from pyVim.connect import Disconnect, SmartConnect

from ray.autoscaler._private.vsphere.utils import Constants


class PyvmomiSdkProvider:
    def __init__(self, server, user, password, session_type: Constants.SessionType):
        self.server = server
        self.user = user
        self.password = password
        self.session_type = session_type

        if self.session_type == Constants.SessionType.UNVERIFIED:
            context_obj = ssl._create_unverified_context()

        smart_connect_obj = SmartConnect(
            host=server,
            user=user,
            pwd=password,
            sslContext=context_obj,
        )
        atexit.register(Disconnect, smart_connect_obj)
        self.pyvmomi_sdk_client = smart_connect_obj.content

    def get_pyvmomi_obj(self, vimtype, name):
        """
        This function finds the vSphere object by the object name and the object type.
        The object type can be "VM", "Host", "Datastore", etc.
        The object name is a unique name under the vCenter server.
        To check all such object information, you can go to the managed object board
        page of your vCenter Server, such as: https://<your_vc_ip/mob
        """
        obj = None
        if self.pyvmomi_sdk_client is None:
            raise ValueError("Must init pyvmomi_sdk_client first.")

        container = self.pyvmomi_sdk_client.viewManager.CreateContainerView(
            self.pyvmomi_sdk_client.rootFolder, vimtype, True
        )

        for c in container.view:
            if name:
                if c.name == name:
                    obj = c
                    break
            else:
                obj = c
                break
        if not obj:
            raise RuntimeError(
                f"Unexpected: cannot find vSphere object {vimtype} with name: {name}"
            )
        return obj
