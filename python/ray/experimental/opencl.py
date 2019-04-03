import ray
import pyopencl as cl

"""
IN DEVELOPMENT
TODO: 
    Need to determine if the GPU platform is using CUDA or OpenCL
    if available (CPU otherwise). Perhaps in ray.init() arg "platform=PYOPENCL"/"CUDA"

    Need to write an opencl kernel for task processing on local machines.

    Need to write task_submit process
"""

class OpenCL(): 

    def __init__(self):
        self.gpu_ids = get_gpu_devices()
        self.platforms = get_platforms()
        
    def get_platform():
        """ 
            Get the available OpenCL platforms to compute across
        """
        return cl.get_platforms()

    def get_gpu_devices():
        """
        Get the device ID's of the OpenCL environment using PyOpenCL
        Returns:
            If OpenCL devices are available, this returns a list of integers with
                the ID's of the GPU's. If it is not set, this returns None.
        """
        dev_ids = []
        
        for p in self.platforms:
            dev = p.get_devices(device_type=cl.device_type.GPU)
            for d in dev:
                dev_ids.append(d.int_ptr)
        return None if dev_ids is None else dev_ids
    
    def get_gpu_ids():
        return self.gpu_ids

    def submit_task(task):
        pass

    def kernel():
        pass