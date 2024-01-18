PROMETHEUS_URL="http://localhost:9090"
import requests
import ray


from pprint import pprint


class Monitor:
    
    def __init__(self):
       
        self.usr = PROMETHEUS_URL
        self.port_url=[]
        self.up_list = []
        self.Node_ID=[]
        self.nodes_info={}
        self.down_list = []
    def target(self):
        """
     
        :return:
        """
        url = self.usr + '/api/v1/targets'
        response = requests.request('GET', url)
        if response.status_code == 200:
            targets = response.json()['data']['activeTargets']
            for target in targets:
                if target['health'] == 'up':
                    self.up_list.append(target['discoveredLabels']['__address__'])
                else:
                    self.down_list.append(target['discoveredLabels']['__address__'])
            return self.up_list
        else:
            print('Get targets status failed!')
            return None
    
    def get_metric_port(self):
        # pprint(ray.nodes())
        
        ray.init()
        nodes_info=ray.nodes()
        for i in range(len(ray.nodes())):
            self.Node_ID.append(nodes_info[i]["NodeID"])
            self.nodes_info[nodes_info[i]["NodeID"]]=nodes_info[i]
        ray.shutdown()
            # result=nodes_info[i]["NodeManagerAddress"]+":"+nodes_info[i]["MetricsExportPort"])
            # print(result)
            # self.port_url.append(result)
        # return self.port_url



    def getQueryValue(self, query):
        """
        Run Query Value for single output
        :param query: query text
        :return: return value
        """
        base_url = self.usr + '/api/v1/query?query='
        inquire = base_url + query
        # print(inquire)
        response = requests.request('GET', inquire)
        if response.status_code == 200:
            # print(response.json())
            if len(response.json()['data']['result'])==0:
                # print("No Value")
                return None
            result = response.json()['data']['result'][0]
            # print("Query Result:", result)
            return result
        else:
            return None




    def getQueryRange(self, query, time_range):
        """
        run query within time range
        :param time_range: query time range
        :param query: query text
        :return: return value
        """
        base_url = self.usr + '/api/v1/query_range?query='
        inquire = base_url + query + time_range
        # print(inquire)
        response = requests.request('GET', inquire)
        if response.status_code == 200:
            result = response.json()['data']['result']
            # print("Query Result:", result)
            return result
        else:
            return None
    
    def ray_node_cpu_utilization(self, *params):
        """
        Get CPU using rate
        Query Single value——（address）
        # Query time range——(start_time,end_time)
        :return:
        """
        # if len(params) == 1:
        
        ID=params[0]
        info=self.nodes_info[ID]
        address=info["NodeManagerAddress"]+":"+str(info["MetricsExportPort"])
        # query = 'avg(rate(node_cpu_seconds_total{job="linux",instance="'+address+'",mode="user"}[2m])) by (instance) *100'
        # query = 'avg(rate(ray_node_cpu_utilization{job="ray"}[2m])) by (instance) *100'
        query = 'ray_node_cpu_utilization{job="ray",instance="'+address+'"}[2m]'

        result = self.getQueryValue(query)
        value = round(sum([float(i[1])for i in result['values']])/len(result['values']))
        print(" ray_node_cpu_utilization:"+ str(value) + '%')

        return str(value) + '%'
        # elif len(params) == 2:
        #     query = 'avg(rate(node_cpu_seconds_total{job="linux",mode="user"}[2m])) by (instance) *100'
        #     time_range = self.timeQuery(params[0], params[1])
        #     result = self.getQueryRange(query, time_range)
        #     return result
        # else:
        #     print('Error')
    def ray_tasks(self, *params):
        """
        Get CPU using rate
        Query Single value——（address）
        # Query time range——(start_time,end_time)
        :return:
        """
        # if len(params) == 1:
        
        ID=params[0]
        info=self.nodes_info[ID]
        address=info["NodeManagerAddress"]+":"+str(info["MetricsExportPort"])
        
        query = 'ray_scheduler_tasks{job="ray",instance="'+address+'"}'

        result = self.getQueryValue(query)
        print(result)

    def ray_task_start_time(self, *params):
        """
        Get CPU using rate
        Query Single value——（address）
        # Query time range——(start_time,end_time)
        :return:
        """
        # if len(params) == 1:
        
        ID=params[0]
        info=self.nodes_info[ID]
        address=info["NodeManagerAddress"]+":"+str(info["MetricsExportPort"])
        
        # query = 'ray_task_start_time{job="ray",instance="'+address+'"}'
        query = 'ray_task_start_time{job="ray",instance="'+address+'"}'
        
        result = self.getQueryValue(query)
        # print(result)
        if result !=None:
            print(" Running Task:"+result['metric']['actor_name']+"  Task Start Time:"+result['value'][1])
        else:
            print(" No task start! ")
        return result
        
       




if __name__ == '__main__':
    m=Monitor()
    # up_list=m.target()
    # print(up_list)
    # for i in up_list:
    #     try:
    #         m.get_cpu_use_rate(i)
    #     except:
    #         print(i+"  next one")
    m.get_metric_port()
    # i=0
    for j in m.Node_ID:
        # if i==0:
        # info=m.nodes_info[j]
        # address=info["NodeManagerAddress"]+":"+info["MetricsExportPort"])
        print("Node Name:"+j)
        res=m.ray_task_start_time(j)
        m.ray_node_cpu_utilization(j)

       
         



   