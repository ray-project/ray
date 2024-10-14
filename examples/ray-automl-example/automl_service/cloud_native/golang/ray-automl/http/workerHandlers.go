package http

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
)

// workerCreateV1
// @Tags workerCreateV1
// @ID workerCreateV1
// @Summary workerCreateV1 from ray-automl-operator
// @Param data body WorkerStartReq{} true "request for worker create V1"
// @Success 200 {object} Response{success=bool,code=int,data=string}
// @Failure 500 {object} Response{success=bool,code=int,data=string}
// @Router /api/v1/worker/create [post]
func (r *RestServer) workerCreateV1(c *gin.Context) {
	data, err := c.GetRawData()
	if err != nil {
		message := fmt.Sprintf("the request for worker create is failed: %v", err)
		serverLog.Error(err, message)
		JSONFailedResponse(c, err, message)
		return
	}

	workerStartReq := &WorkerStartReq{}
	err = json.Unmarshal(data, workerStartReq)
	if err != nil {
		JSONFailedResponse(c, err, fmt.Sprintf("invalid json :%v", string(data)))
		return
	}

	serverLog.Info("workerCreateV1 received", "workerStartReq", workerStartReq)

	workerStartResponse := &WorkerStartResponse{}

	deployments := NewDeploymentInstanceWorker(workerStartReq, serverLog)
	for _, deployment := range deployments {
		//utils.SetTrainerOwnerReference(deployment, instance)
		serverLog.Info("reconcileTrainerWorkerDeploy", "deployment", deployment)
		if err := r.Create(context.TODO(), deployment); err != nil {
			JSONFailedResponse(c, err, fmt.Sprintf("invalid create worker :%v", deployment))
			return
		}
		workerStartResponse.GroupId = deployment.Name
		for i := 0; i < int(*deployment.Spec.Replicas); i++ {
			workerStartResponse.WorkerIds = append(workerStartResponse.WorkerIds, fmt.Sprintf("%s-%v", deployment.Name, i))
		}

	}
	if err != nil {
		serverLog.Error(err, "workerCreateV1 failed")
		JSONFailedResponse(c, err, fmt.Sprintf("invalid create worker :%v", string(data)))
		return
	}

	deployment, _ := json.Marshal(deployments[0])
	serverLog.Info("success to submit worker start request", "worker", workerStartReq, "deployment", string(deployment))

	workerStartResponseJson, _ := json.Marshal(workerStartResponse)
	JSONSuccessResponse(c, string(workerStartResponseJson), "success to submit worker start request")
}

// workerDeleteV1
// @Tags workerDeleteV1
// @ID workerDeleteV1
// @Summary workerDeleteV1 from ray-automl-operator
// @Param data body WorkerStartReq{} true "request for worker delete V1"
// @Success 200 {object} Response{success=bool,code=int,data=string}
// @Failure 500 {object} Response{success=bool,code=int,data=string}
// @Router /api/v1/worker/delete [post]
func (r *RestServer) workerDeleteV1(c *gin.Context) {
	data, err := c.GetRawData()
	if err != nil {
		message := fmt.Sprintf("the request for worker delete is failed: %v", err)
		serverLog.Error(err, message)
		JSONFailedResponse(c, err, message)
		return
	}

	workerStartReq := &WorkerStartReq{}
	err = json.Unmarshal(data, workerStartReq)
	if err != nil {
		JSONFailedResponse(c, err, fmt.Sprintf("invalid json :%v", string(data)))
		return
	}

	serverLog.Info("workerDeleteV1 received", "workerStartReq", workerStartReq)

	deployments := NewDeploymentInstanceWorker(workerStartReq, serverLog)
	for _, deployment := range deployments {
		//utils.SetTrainerOwnerReference(deployment, instance)
		serverLog.Info("reconcileTrainerWorkerDeploy", "deployment", deployment)
		if err := r.Delete(context.TODO(), deployment); err != nil {
			JSONFailedResponse(c, err, fmt.Sprintf("invalid delete worker :%v", deployment))
			return
		}
	}
	if err != nil {
		serverLog.Error(err, "workerDeleteV1 failed")
		JSONFailedResponse(c, err, fmt.Sprintf("invalid delete worker :%v", string(data)))
		return
	}

	serverLog.Info("success to submit worker delete request", "worker", workerStartReq)
	deployment, _ := json.Marshal(deployments[0])
	JSONSuccessResponse(c, string(deployment), "success to submit worker delete request")
}

type WorkerStartReq struct {
	Namespace      string                       `json:"namespace,omitempty"`
	Name           string                       `json:"name,omitempty"`
	Image          string                       `json:"image,omitempty"`
	TrainerAddress string                       `json:"trainerAddress,omitempty"`
	Workers        map[string]map[string]string `json:"workers,omitempty"`
}

type WorkerStartResponse struct {
	GroupId   string   `json:"group_id,omitempty"`
	WorkerIds []string `json:"worker_ids,omitempty"`
}
