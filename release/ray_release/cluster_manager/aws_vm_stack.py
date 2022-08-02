from ray_release.cluster_manager.cluster_manager import ClusterManager
from typing import Dict, Any, Optional
import subprocess
import sys
import signal

# TODO Rename this, it works for anything that `ray up` supports (GCP)
class AwsVmClusterManager(ClusterManager):

    def __init__(self, a, b):
        super(ClusterManager, self).__init__()
        #super(ClusterManager, self).__init__(test_name='TODO', project_id='TODO', sdk='TODO')

        self.port_forward_proc = None
        self.cluster_name = 'TODO-cluster-name'

    def create_cluster_env(self, _repeat: bool = True):
        raise NotImplementedError

    def build_cluster_env(self, timeout: float):
        raise NotImplementedError

    def fetch_build_info(self):
        raise NotImplementedError

    def create_cluster_compute(self, _repeat: bool = True):
        raise NotImplementedError

    def build_configs(self, timeout: float):
        pass

    def delete_configs(self):
        raise NotImplementedError

    def start_cluster(self, timeout: float):

        # TODO need to propagate bad error codes
        import os
        if os.environ.get('CADE_SKIP_RAY_UP_COMMAND', None):
            pass
        else:
            process = subprocess.Popen(['ray', 'up', 'cluster_launcher_config_aws.yaml', '-y'])
            # TODO handle timeouts
            return_code = process.wait()

        # TODO error handling
        # TODO fix: The terminal is corrupted by the ray dashboard command.

        self.port_forward_proc = subprocess.Popen(['ray', 'dashboard', 'cluster_launcher_config_aws.yaml'])

        # TODO(cade) remove this sleep, find less flaky way to wait until port forwarding is done
        import time
        print('Sleeping 10s to wait for port forward to be online.')
        time.sleep(10)
        # debug: wait for user input
        #print('enter any key to continue')
        #subprocess.Popen(['read', '-n', '1']).wait()

    def terminate_cluster(self, wait: bool):

        self.port_forward_proc.send_signal(signal.SIGTERM)
        self.port_forward_proc.wait()

        # TODO This is missing wait=False functionality
        import os
        if os.environ.get('CADE_SKIP_RAY_DOWN_COMMAND', None):
            pass
        else:
            process = subprocess.Popen(['ray', 'down', 'cluster_launcher_config_aws.yaml', '-y'])
            # TODO handle timeouts
            return_code = process.wait()

    def get_cluster_address(self) -> str:
        return None
    
    def get_cluster_url(self) -> Optional[str]:
        return None

#def stream_subproc_output(process):
#    # I can delete this function and just use proc.wait()
#    # If we need to check the output as it runs, we can use this
#    # TODO delete this
#    #
#    # process = subprocess.Popen(['ray', 'up', 'cluster_launcher_config.yaml', '-y'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#    # stream_subproc_output(process)
#    import select
#
#    associated_streams = {process.stderr: sys.stderr, process.stdout: sys.stdout}
#    timeout_seconds = 0.1
#
#    while True:
#        ready_reads, _, _ = select.select(associated_streams.keys(), [], [], timeout_seconds)
#        
#        if process.poll() != None:
#            break
#    
#        for stream in ready_reads:
#            read_one = stream.read1().decode('utf-8')
#            associated_streams[stream].write(read_one)
#
