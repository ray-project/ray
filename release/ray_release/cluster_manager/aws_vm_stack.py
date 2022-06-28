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
        self.port_forward_proc = subprocess.Popen(['ray', 'dashboard', 'cluster_launcher_config_aws.yaml'])
        subprocess.Popen(['read', '-n', '1']).wait()

        '''
        Next steps:
        * Run a command on the cluster
        * Do we need files on the cluster?
        * Do we need `wait_for_nodes`? in runner
        * Get last logs, fetch results
        * AWS testcase (simpler on on BK). Also should be using legacy-work with its delicious lambdas.

        Notes on command running:
            I can either port-forward the Dashboard URL by running a `ray dashboard config.yaml` proc,
            then run `ray job submit script`, or I can use `ray submit config.yaml script`.

            The former runs into issues because we don't have files on the cluster -- need to rsync them
            but if we do that then we might as well use ray submit directly.

            But if we use the job_manager, it can't connect to the Ray address without port forwarding.
            In other words, we don't have a `ray submit` manager AFAIK.

            What tar.gz are we uploading to s3? and why doesn't it have what we need?


            Looks like the tar.gz has the files we want.
        '''

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
