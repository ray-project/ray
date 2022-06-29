from ray_release.cluster_manager.cluster_manager import ClusterManager
from typing import Dict, Any, Optional
import subprocess
import sys
import signal

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

        # TODO error handling?
        # TODO should dump this output to stdout (actually I think it already goes to stdout)
        # TODO fix terminal borking.
        #self.port_forward_proc = subprocess.Popen(' '.join(['ray', 'dashboard', 'cluster_launcher_config_aws.yaml']), shell=True)
        self.port_forward_proc = subprocess.Popen(['ray', 'dashboard', 'cluster_launcher_config_aws.yaml'])
        #self.port_forward_proc = subprocess.Popen(['sleep', '120'])
        import time
        print('Sleeping 10s to wait for port forward to be online. TODO(cade) remove this')
        time.sleep(10)
        # debug: wait for user input
        #print('enter any key to continue')
        #subprocess.Popen(['read', '-n', '1']).wait()

    def terminate_cluster(self, wait: bool):

        self.port_forward_proc.send_signal(signal.SIGTERM)
        self.port_forward_proc.wait()

        # TODO wait=False functionality
        import os
        if os.environ.get('CADE_SKIP_RAY_DOWN_COMMAND', None):
            pass
        else:
            process = subprocess.Popen(['ray', 'down', 'cluster_launcher_config_aws.yaml', '-y'])
            # TODO handle timeouts
            return_code = process.wait()


        ''' Annoying terminal borking'''

    def get_cluster_address(self) -> str:
        command = ['ray', 'get-head-ip', 'cluster_launcher_config_aws.yaml']
        process = subprocess.Popen(command ,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        
        # TODO verbose logs currently go to stdout. They should go to stderr so I can separate stdout/stderr.
        lines_in_stdout = stdout.strip().split(b'\n')

        if not lines_in_stdout:
            raise ValueError(f'Unexpected output from command {command}: {stdout}, {stderr}')

        return None
        #return 'http://localhost'
        #return 'https://' + lines_in_stdout[-1].decode('utf-8')
    
    def get_cluster_url(self) -> Optional[str]:
        return None
