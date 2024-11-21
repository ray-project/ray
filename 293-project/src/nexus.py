from bisect import bisect, insort
from collections import namedtuple
from operator import attrgetter
import math, copy

"""
    Definition of a session Si is as follows: 
        Si = < Mk, Li, Ri >
        Mk: str,   model name
        Li: float, SLO for model
        Ri: float, request rate for the model
"""
class session:
    batch_size: int  = 0 # batch when scheduled

    def __init__(self, model_name: str, latency_SLO: float, request_rate: float, batch_size: int = None):
        self.model_name   = model_name
        self.latency_SLO  = latency_SLO
        self.request_rate = request_rate
        
        if batch_size:
            self.batch_size = batch_size
        
    def print_session_pretty(self):
        print(f"Model name: {self.model_name}, SLO: {self.latency_SLO}ms, request rate: {self.request_rate}, batch size: {self.batch_size}")

"""
    Definition of a node:
        Node represents a GPU in hardware and a bin in the squishy bin packing problem.
        For now all nodes are assumed to be homogeeneous is nature, i.e, same gpu memory and gpu type. 
"""
class node:
    node_sessions: list[(session, float)] = [] # (session, occupancy)
    duty_cycle: float = float('inf')

    def __init__(self, node_sessions: list[(session, float)] = None, duty_cycle: float = None, gpu_type: str = 'A6000', gpu_mem: float = 48):
        self.gpu_type = gpu_type
        self.gpu_mem  = gpu_mem
        
        if duty_cycle:
            self.duty_cycle = duty_cycle
        
        if node_sessions:
            self.node_sessions = node_sessions

    # def __copy__(self):
    #     return node(self.node_sessions, self.duty_cycle, self.gpu_type, self.gpu_mem)
        

    def get_occupancy(self):
        node_occupancy = 0
        for _, occupancy in self.node_sessions:
            node_occupancy += occupancy
        
        return node_occupancy

    def print_node_pretty(self):
        print(f"---------------------------------------------------------------------------")
        print(f"Node gpu type: {self.gpu_type}, Node gpu memory: {self.gpu_mem}GB")
        print(f"Node duty cycle: {self.duty_cycle}ms")
        print(f"Node sessions: {len(self.node_sessions)}")
        for i, (s, occ) in enumerate(self.node_sessions):
            print(f"session number {i+1} has occupancy: {round(occ * 100, 2)}%")
            s.print_session_pretty()
        print(f"---------------------------------------------------------------------------")


class NexusScheduler:
    """
        This class implements the squishy bin packing algorithm described
        in section 6.1 of nexus paper

        batching profile dictionary needs to be structured as follows:
        key = batch size: { key = column name in csv(latency, memory usage etc): value}
    """
    def __init__(self, batching_profile: dict[str, dict[int, dict [str, float]]]):
        self.batching_profile = batching_profile

    """
        Squishy bin packing implementation

        Inputs:
            - List of sessions

        Returns:
            - List of nodes: Representing the final schedule
    """
    def squishyBinPacking(self, sessions: list[session]):
        nodes, residual_sessions = self.scheduleSaturate(sessions)
        nodes.extend(self.scheduleResidue(residual_sessions))
        
        return nodes

    """
        Schedule full nodes at maximum batch size possible and return residual work

        Inputs:
            - List of sessions

        Returns:
            - List of nodes: Full node schedules
            - List of sessions: Residual work remaining
    """
    def scheduleSaturate(self, sessions: list[session]):
        nodes: list[node] = []
        residual_sessions: list[session] = []

        for s in sessions:
            latency_entry  = namedtuple('latency_entry', ('batch_size', 'avg_latency_ms'))
            latency_list   = [latency_entry(key, self.batching_profile[s.model_name][key]['avg_latency_ms']) for key in self.batching_profile[s.model_name].keys()]
            
            by_latency = attrgetter('avg_latency_ms')
            max_batch_ind  = bisect(latency_list, s.latency_SLO/2, key=by_latency)
            max_batch_size, max_latency = latency_list[max_batch_ind]
            max_throughput = (max_batch_size/max_latency) * 1000

            # decompose request rate into the form
            # R = n * Throughput + r
            # where n is whole number and r is less than R
            # print(f"DEGUB:SATURATE: max batch size {max_batch_size} max latency {max_latency} max throughput {max_throughput}, request_rate {s.request_rate}")
            n, r = divmod(s.request_rate, max_throughput)
            
            # allocate n GPUs at max batch size to model
            # print(f"DEBUG:SATURATE: model name: {s.model_name} n: {n} r:{r}")
            if n > 0:
                nodes.extend([node([(session(s.model_name, s.latency_SLO, max_throughput, max_batch_size), 1.0)], duty_cycle=max_latency)] * int(n))

            # create sessions for the residual work
            residual_sessions.append(session(s.model_name, s.latency_SLO, r))

        return nodes, residual_sessions
    
    """ 
        Function to merge two nodes with multiple sessions in them.

        Input:
            - Node1
            - Node2
        
        Results:
            - node: If merge possible else returns none
    """
    def mergeNodes(self, node1: node, node2: node):
        # setting node2 to have the lower duty cycle
        if(node1.duty_cycle < node2.duty_cycle):
            node1, node2 = node2, node1

        new_node   = copy.deepcopy(node2)
        duty_cycle = node2.duty_cycle

        # add all session in node1 to node2
        for s, occ in node1.node_sessions:
            new_batch   = int(math.ceil((duty_cycle * s.request_rate) / 1000))
            new_latency = self.batching_profile[s.model_name][new_batch]['avg_latency_ms']
            
            new_node.node_sessions.append((session(s.model_name, s.latency_SLO, s.request_rate, batch_size=new_batch), new_latency/duty_cycle))

        # check if all sessions fit in the duty cycle
        if new_node.get_occupancy() > 1:
            return None
        
        # check if memory is sufficient
        total_memory = 0
        for s, occ in new_node.node_sessions:
            total_memory += float(self.batching_profile[s.model_name][s.batch_size]['peak_memory_mb'])/1024
        if total_memory > new_node.gpu_mem:
            return None

        return new_node

    """
        Function called after scheduleSaturate. Create schedule using squish bin packing for the
        residual work

        Inputs:
            - List of sessions: Representing residual work

        Returns:
            - List of nodes: Representing a schedule
    """
    def scheduleResidue(self, sessions: list[session]):
        nodes: list[node] = []

        # schedule each session in separate node
        single_nodes: list[node] = []
        for s in sessions:
            latency_entry   = namedtuple('latency_entry', ('batch_size', 'avg_latency_ms'))
            request_latency = [latency_entry(key, self.batching_profile[s.model_name][key]['avg_latency_ms'] + key/s.request_rate) for key in self.batching_profile[s.model_name].keys()]

            by_latency = attrgetter('avg_latency_ms')
            max_batch_ind  = bisect(request_latency, s.latency_SLO, key=by_latency)
            max_batch_size, max_latency = request_latency[max_batch_ind]

            duty_cycle = (max_batch_size/s.request_rate) * 1000
            occupancy  = max_latency/duty_cycle

            s.batch_size = max_batch_size
            s.occupancy  = occupancy
            single_nodes.append(node([(s, occupancy)], duty_cycle=duty_cycle))

        # sort nodes basd on occpancy in decreasing order 
        sorted_nodes = sorted(single_nodes, key=lambda node: node.get_occupancy(), reverse=True)
        # for n in sorted_nodes:
        #     n.print_node_pretty()
        # print("+++"*50)

        for residual_node in sorted_nodes:
            max_occupancy = 0
            max_node_ind  = None
            max_node      = None

            # Try to merge residual node with existing nodes
            # find the node that results in the maximum occupancy
            for i, n in enumerate(nodes):
                new_node = self.mergeNodes(n, residual_node)
                if new_node and new_node.get_occupancy() > max_occupancy:
                    print(f"Merge possible between nodes")
                    max_occupancy = new_node.get_occupancy()
                    max_node_ind  = i
                    max_node      = new_node

            if max_node:
                nodes[max_node_ind] = max_node
            else:
                nodes.append(residual_node)
                
                
        return nodes

import csv
column_list = [
    'avg_latency_ms',
    'peak_memory_mb'
]
def load_csv_to_dict(file_path):
    result = {}
    with open(file_path, 'r') as csvfile:
        reader  = csv.DictReader(csvfile)
        headers = reader.fieldnames
        key_column = headers[0]
        for row in reader:
            key = int(row[key_column])
            result[key] = {header: float(row[header]) for header in headers[1:] and column_list}
    return result

def main():
    resnet_profile = "../profiling/resnet50_20241117_154052_summary.csv"

    batching_profile = {}
    batching_profile['resnet'] = load_csv_to_dict(resnet_profile)
    batching_profile['vit']    = load_csv_to_dict(resnet_profile)
    for batch in batching_profile['vit'].keys():
        for c in column_list:
            batching_profile['vit'][batch][c] /= 2

    scheduler = NexusScheduler(batching_profile)

    # create sessions
    sessions: list [session] = []
    resnet_session = session('resnet', 200, 1000)
    vit_session    = session('vit', 100, 500)

    sessions.append(resnet_session)
    sessions.append(vit_session)

    nodes = scheduler.squishyBinPacking(sessions)
    for n in nodes:
        n.print_node_pretty()

if __name__ == "__main__":
    main()
    
    