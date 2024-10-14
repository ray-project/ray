# AutoML Service based on cloud native

## 0.Install Docker and K8s

### [Docker Desktop](https://www.docker.com/products/docker-desktop/)

Docker Desktop has integrated with K8s to better the experience.

### [kubectl](https://kubernetes.io/docs/tasks/tools/)

kubectl used to communicate with k8s cluster.

### [kustomize](https://kustomize.io/)

kustomize used to manage manifest to k8s.

## 1.Install and Start AutoML Operator

After start Docker Desktop and enable Kubernetes(K8s), to install ray-automl operator

```
cd golang/ray-automl/Makefile
make deploy
```

## 2.Check AutoML Operator Running state

When pod STATUS is Running, shows controller is running as expectation. Also, can check pod log to see if controller running failed. 

```
$ kubectl get po -nray-automl-system -owide|sort
NAME                                             READY   STATUS    RESTARTS   AGE   IP           NODE             NOMINATED NODE   READINESS GATES
ray-automl-controller-manager-567986f548-c8q4p   2/2     Running   0          28s   10.1.1.112   docker-desktop   <none>           <none>

$ kubectl logs ray-automl-controller-manager-567986f548-c8q4p -nray-automl-system            
2023-05-12T03:23:18.524Z	info	controller-runtime.metrics	Metrics server is starting to listen	{"addr": "127.0.0.1:8080"}
2023-05-12T03:23:18.525Z	info	setup	starting http server
[GIN-debug] [WARNING] Creating an Engine instance with the Logger and Recovery middleware already attached.

[GIN-debug] [WARNING] Running in "debug" mode. Switch to "release" mode in production.
 - using env:	export GIN_MODE=release
 - using code:	gin.SetMode(gin.ReleaseMode)

[GIN-debug] POST   /api/v1/trainer/create    --> github.com/ray-automl/http.(*RestServer).trainerCreateV1-fm (4 handlers)
[GIN-debug] POST   /api/v1/trainer/delete    --> github.com/ray-automl/http.(*RestServer).trainerDeleteV1-fm (4 handlers)
[GIN-debug] POST   /api/v1/worker/create     --> github.com/ray-automl/http.(*RestServer).workerCreateV1-fm (4 handlers)
[GIN-debug] POST   /api/v1/worker/delete     --> github.com/ray-automl/http.(*RestServer).workerDeleteV1-fm (4 handlers)
[GIN-debug] GET    /api/health/              --> github.com/ray-automl/http.HealthCheck (4 handlers)
[GIN-debug] GET    /swagger/*any             --> github.com/swaggo/gin-swagger.CustomWrapHandler.func1 (4 handlers)
2023-05-12T03:23:18.526Z	info	setup	starting manager
2023-05-12T03:23:18.527Z	info	Starting server	{"path": "/metrics", "kind": "metrics", "addr": "127.0.0.1:8080"}
2023-05-12T03:23:18.527Z	info	Starting server	{"kind": "health probe", "addr": "[::]:8081"}
I0512 03:23:18.527422       1 leaderelection.go:248] attempting to acquire leader lease ray-automl-system/82d4c1d3.my.domain...
I0512 03:23:34.880288       1 leaderelection.go:258] successfully acquired lease ray-automl-system/82d4c1d3.my.domain
2023-05-12T03:23:34.880Z	debug	events	ray-automl-controller-manager-567986f548-c8q4p_7472afdd-b08f-407e-954a-1d8600ba59e2 became leader	{"type": "Normal", "object": {"kind":"Lease","namespace":"ray-automl-system","name":"82d4c1d3.my.domain","uid":"33874d65-fd6b-456a-b42d-ffb9166e9391","apiVersion":"coordination.k8s.io/v1","resourceVersion":"429503"}, "reason": "LeaderElection"}
2023-05-12T03:23:34.882Z	info	Starting EventSource	{"controller": "trainer", "controllerGroup": "automl.my.domain", "controllerKind": "Trainer", "source": "kind source: *v1.Trainer"}
2023-05-12T03:23:34.882Z	info	Starting EventSource	{"controller": "proxy", "controllerGroup": "automl.my.domain", "controllerKind": "Proxy", "source": "kind source: *v1.Proxy"}
2023-05-12T03:23:34.883Z	info	Starting Controller	{"controller": "proxy", "controllerGroup": "automl.my.domain", "controllerKind": "Proxy"}
2023-05-12T03:23:34.884Z	info	Starting Controller	{"controller": "trainer", "controllerGroup": "automl.my.domain", "controllerKind": "Trainer"}
2023-05-12T03:23:35.021Z	info	Starting workers	{"controller": "proxy", "controllerGroup": "automl.my.domain", "controllerKind": "Proxy", "worker count": 1}
2023-05-12T03:23:35.021Z	info	Starting workers	{"controller": "trainer", "controllerGroup": "automl.my.domain", "controllerKind": "Trainer", "worker count": 1}

```

## 3.Deploy automl proxy to serve
```
$ kubectl create -f golang/ray-automl/config/samples/automl_v1_proxy.yaml
proxy.automl.my.domain/proxy-sample created

$ kubectl get po -nray-automl-system -owide|sort
NAME                                             READY   STATUS    RESTARTS   AGE     IP           NODE             NOMINATED NODE   READINESS GATES
proxy-sample-cb44dd9d7-ghvtc                     1/1     Running   0          6s      10.1.1.114   docker-desktop   <none>           <none>

$ kubectl logs proxy-sample-cb44dd9d7-ghvtc -nray-automl-system -f
2023-05-12 03:29:43,252 Using selector: EpollSelector
2023-05-12 03:29:43,252 Using AsyncIOEngine.POLLER as I/O engine
2023-05-12 03:29:43,259 Proxy grpc address: 0.0.0.0:1234
```


## 4.Deploy automl trainer to train
When run trainer local, need to port-forward k8s pod to expose service of controller and proxy
```
$ kubectl port-forward  ray-automl-controller-manager-567986f548-c8q4p 7070:7070 -nray-automl-system
Forwarding from 127.0.0.1:7070 -> 7070
Forwarding from [::1]:7070 -> 7070

$ kubectl port-forward proxy-sample-cb44dd9d7-ghvtc   1234:1234 -nray-automl-system
Forwarding from 127.0.0.1:1234 -> 1234
Forwarding from [::1]:1234 -> 1234
```

Submit Trainer to train, trainer will start worker to calculate.
```
cd cloud_native/python
python application.py

$ kubectl get po -nray-automl-system -owide|sort
NAME                                             READY   STATUS    RESTARTS   AGE     IP           NODE             NOMINATED NODE   READINESS GATES
trainer-0-6bc998d8fb-5r8gr                       1/1     Running   0          65s     10.1.1.115   docker-desktop   <none>           <none>
worker-5f15-0                                    1/1     Running   0          18s     10.1.1.116   docker-desktop   <none>           <none>
worker-5f15-1                                    1/1     Running   0          18s     10.1.1.117   docker-desktop   <none>           <none>
worker-5f15-2                                    1/1     Running   0          18s     10.1.1.118   docker-desktop   <none>           <none>
worker-5f15-3                                    1/1     Running   0          18s     10.1.1.119   docker-desktop   <none>           <none>


$ python application.py 
2023-05-12 11:35:47,765 The task is not finished, wait...
2023-05-12 11:35:52,769 The task is not finished, wait...
2023-05-12 11:35:57,777 The task is not finished, wait...
2023-05-12 11:36:02,786 The task is not finished, wait...
2023-05-12 11:36:07,795 The task is not finished, wait...
2023-05-12 11:36:12,809 The task is not finished, wait...
2023-05-12 11:36:17,933 The task is not finished, wait...
2023-05-12 11:36:22,947 The task is not finished, wait...
2023-05-12 11:36:27,957 The task is not finished, wait...
2023-05-12 11:36:32,964 The task is not finished, wait...
2023-05-12 11:36:37,971 The task is not finished, wait...
2023-05-12 11:36:42,981 The task is not finished, wait...
2023-05-12 11:36:47,988 The task is not finished, wait...
2023-05-12 11:36:52,994 The task is not finished, wait...
2023-05-12 11:36:57,998 The task is not finished, wait...
2023-05-12 11:37:03,007 The task is not finished, wait...
2023-05-12 11:37:08,016 The task is not finished, wait...
2023-05-12 11:37:13,021 The task is not finished, wait...
2023-05-12 11:37:18,030 The task is not finished, wait...
2023-05-12 11:37:23,037 The task is not finished, wait...
2023-05-12 11:37:28,044 The task is not finished, wait...
2023-05-12 11:37:33,049 The task is not finished, wait...
2023-05-12 11:37:38,059 The task is not finished, wait...
2023-05-12 11:37:43,064 The task is not finished, wait...
2023-05-12 11:37:48,072 The task is not finished, wait...
2023-05-12 11:37:53,079 The task is not finished, wait...
2023-05-12 11:37:58,087 The task is not finished, wait...
2023-05-12 11:38:03,094 The task is not finished, wait...
2023-05-12 11:38:08,101 The task is not finished, wait...
2023-05-12 11:38:13,110 The task is not finished, wait...
2023-05-12 11:38:18,118 The task is not finished, wait...
2023-05-12 11:38:23,132 The task has already finished, the result {'d': None, 'D': None, 'max_p': 5, 'max_q': 5, 'max_P': 2, 'max_Q': 2, 'max_order': 5, 'max_d': 2, 'max_D': 1, 'start_p': 2, 'start_q': 2, 'start_P': 1, 'start_Q': 1, 'stationary': False, 'seasonal': True, 'ic': 'aicc', 'stepwise': True, 'nmodels': 94, 'trace': False, 'approximation': False, 'method': None, 'truncate': None, 'test': 'kpss', 'test_kwargs': None, 'seasonal_test': 'seas', 'seasonal_test_kwargs': None, 'allowdrift': False, 'allowmean': False, 'blambda': None, 'biasadj': False, 'parallel': False, 'num_cores': 2, 'season_length': 1, 'alias': 'AutoARIMA'}.
2023-05-12 11:38:23,132 Finished!
```


## 5.Destroy automl trainer , worker, proxy
```
$ kubectl get trainers -nray-automl-system      
NAME        AGE
trainer-0   4m25s

$ kubectl delete trainers trainer-0 -nray-automl-system 
trainer.automl.my.domain "trainer-0" deleted


$ kubectl get sts -nray-automl-system    
NAME          READY   AGE
worker-5f15   4/4     4m35s


$ kubectl delete sts worker-5f15 -nray-automl-system 
statefulset.apps "worker-5f15" deleted


$ kubectl get proxies -nray-automl-system               
NAME           AGE
proxy-sample   12m

$ kubectl delete proxies proxy-sample -nray-automl-system
proxy.automl.my.domain "proxy-sample" deleted
```

## 6.Destroy automl operator

```
cd golang/ray-automl/Makefile
make undeploy
```
