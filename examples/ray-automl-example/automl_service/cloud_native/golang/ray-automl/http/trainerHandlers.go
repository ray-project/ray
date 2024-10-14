package http

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	automlv1 "github.com/ray-automl/apis/automl/v1"
	"github.com/ray-automl/common"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
)

// trainerCreateV1
// @Tags trainerCreateV1
// @ID trainerCreateV1
// @Summary trainerCreateV1 from ray-automl-operator
// @Param data body TrainerStartReq{} true "request for trainer create V1"
// @Success 200 {object} Response{success=bool,code=int,data=string}
// @Failure 500 {object} Response{success=bool,code=int,data=string}
// @Router /api/v1/trainer/create [post]
func (r *RestServer) trainerCreateV1(c *gin.Context) {
	data, err := c.GetRawData()
	if err != nil {
		message := fmt.Sprintf("the request for trainer create is failed: %v", err)
		serverLog.Error(err, message)
		JSONFailedResponse(c, err, message)
		return
	}

	trainerStartReq := &TrainerStartReq{}
	err = json.Unmarshal(data, trainerStartReq)
	if err != nil {
		JSONFailedResponse(c, err, fmt.Sprintf("invalid json :%v", string(data)))
		return
	}

	serverLog.Info("trainerCreateV1 received", "trainerStartReq", trainerStartReq)

	trainerInstance := trainerGenerator(trainerStartReq)
	serverLog.Info("trainerCreateV1 trainer", "instance", trainerInstance)
	err = r.Create(context.TODO(), trainerInstance)
	if err != nil {
		serverLog.Error(err, "trainerCreateV1 failed")
		JSONFailedResponse(c, err, fmt.Sprintf("invalid create trainer :%v", string(data)))
		return
	}

	serverLog.Info("success to submit trainer start request", "trainer", trainerInstance)

	trainerInstanceJson, _ := json.Marshal(trainerInstance)
	JSONSuccessResponse(c, string(trainerInstanceJson), "success to submit trainer start request")
}

// trainerDeleteV1
// @Tags trainerDeleteV1
// @ID trainerDeleteV1
// @Summary trainerDeleteV1 from ray-automl-operator
// @Param data body TrainerStartReq{} true "request for trainer delete V1"
// @Success 200 {object} Response{success=bool,code=int,data=string}
// @Failure 500 {object} Response{success=bool,code=int,data=string}
// @Router /api/v1/trainer/delete [post]
func (r *RestServer) trainerDeleteV1(c *gin.Context) {
	data, err := c.GetRawData()
	if err != nil {
		message := fmt.Sprintf("the request for trainer delete is failed: %v", err)
		serverLog.Error(err, message)
		JSONFailedResponse(c, err, message)
		return
	}

	trainerStartReq := &TrainerStartReq{}
	err = json.Unmarshal(data, trainerStartReq)
	if err != nil {
		JSONFailedResponse(c, err, fmt.Sprintf("invalid json :%v", string(data)))
		return
	}

	serverLog.Info("trainerDeleteV1 received", "trainerReq", trainerStartReq)

	trainerInstance := trainerGenerator(trainerStartReq)
	serverLog.Info("trainerDeleteV1 trainer", "instance", trainerInstance)
	err = r.Delete(context.TODO(), trainerInstance)
	if err != nil {
		serverLog.Error(err, "trainerCreateV1 failed")
		JSONFailedResponse(c, err, fmt.Sprintf("invalid create trainer :%v", string(data)))
		return
	}

	serverLog.Info("success to submit trainer delete request", "trainer", trainerInstance)
	trainerInstanceJson, _ := json.Marshal(trainerInstance)
	JSONSuccessResponse(c, string(trainerInstanceJson), "success to submit trainer delete request")
}

type TrainerStartReq struct {
	ProxyName   string            `json:"proxyName,omitempty"`
	Name        string            `json:"name,omitempty"`
	Namespace   string            `json:"namespace,omitempty"`
	StartParams map[string]string `json:"startParams,omitempty"`
	Image       string            `json:"image,omitempty"`
}

func trainerGenerator(trainerStartReq *TrainerStartReq) *automlv1.Trainer {
	trainer := &automlv1.Trainer{}
	trainer.APIVersion = automlv1.GroupVersion.String()
	trainer.Kind = "Trainer"
	trainer.Name = trainerStartReq.Name
	if trainer.Labels == nil {
		trainer.Labels = map[string]string{}
	}
	trainer.Labels[common.ProxyLabelSelector] = trainerStartReq.ProxyName
	trainer.Labels[common.TrainerLabelSelector] = trainerStartReq.Name
	trainer.Namespace = os.Getenv(common.Namespace)
	if trainerStartReq.Namespace != "" {
		trainer.Namespace = trainerStartReq.Namespace
	}
	trainer.Spec.StartParams = trainerStartReq.StartParams
	trainer.Spec.Image = os.Getenv(common.Image)
	if trainerStartReq.Image != "" {
		trainer.Spec.Image = trainerStartReq.Image
	}
	if trainer.Spec.DeploySpec.Selector == nil {
		trainer.Spec.DeploySpec.Selector = &metav1.LabelSelector{
			MatchLabels: trainer.Labels,
		}
	}
	trainer.Spec.DeploySpec.Template.Spec.Containers = append(trainer.Spec.DeploySpec.Template.Spec.Containers, corev1.Container{Name: automlv1.TrainerContainerName})
	return trainer
}
