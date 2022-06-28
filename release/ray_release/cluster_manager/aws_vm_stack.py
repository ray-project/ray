from ray_release.cluster_manager.cluster_manager import ClusterManager
from typing import Dict, Any, Optional
import subprocess
import sys

def stream_subproc_output(process):
    # TODO dup output so we can assert on it
    # Otherwise, this can be simplified to proc.wait()
    import select

    associated_streams = {process.stderr: sys.stderr, process.stdout: sys.stdout}
    timeout_seconds = 0.1

    while True:
        ready_reads, _, _ = select.select(associated_streams.keys(), [], [], timeout_seconds)
        
        if process.poll() != None:
            break
    
        for stream in ready_reads:
            read_one = stream.read1().decode('utf-8')
            associated_streams[stream].write(read_one)


class AwsVmClusterManager(ClusterManager):

    def __init__(self, a, b):
        super(ClusterManager, self).__init__()
        #super(ClusterManager, self).__init__(test_name='TODO', project_id='TODO', sdk='TODO')

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
        # This is commented out as unless we need the stdout/stderr we can defer to process.wait
        #process = subprocess.Popen(['ray', 'up', 'cluster_launcher_config.yaml', '-y'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #stream_subproc_output(process)

        # TODO need to propagate bad error codes
        import os
        if os.environ.get('CADE_SKIP_RAY_UP_COMMAND', None):
            pass
        else:
            process = subprocess.Popen(['ray', 'up', 'cluster_launcher_config_aws.yaml', '-y'])
            # TODO handle timeouts
            return_code = process.wait()

        '''
        Next steps:
        * Terminate cluster (why is it not called?)
        * Run a command on the cluster
        * Do we need files on the cluster?
        * Do we need `wait_for_nodes`? in runner
        * Get last logs, fetch results
        * AWS testcase (simpler on on BK). Also should be using legacy-work with its delicious lambdas.
        '''

    def terminate_cluster(self, wait: bool):
        # TODO wait=False functionality
        import os
        if os.environ.get('CADE_SKIP_RAY_DOWN_COMMAND', None):
            pass
        else:
            process = subprocess.Popen(['ray', 'down', 'cluster_launcher_config_aws.yaml', '-y'])
            # TODO handle timeouts
            return_code = process.wait()

    def get_cluster_address(self) -> str:
        raise NotImplementedError
    
    def get_cluster_url(self) -> Optional[str]:
        return None
