package v1

import (
	"fmt"
	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestMarshalling(t *testing.T) {
	trainer := &Trainer{
		TypeMeta:   metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{},
		Spec:       TrainerSpec{},
		Status:     TrainerStatus{},
	}

	trainer.Namespace = "test"
	trainer.Name = "test"

	group := map[string]string{}
	group["cpu"] = "1"
	group["memory"] = "1"
	group["disk"] = "1"
	trainer.Spec.Workers = map[string]map[string]string{}
	trainer.Spec.Workers["group1"] = map[string]string{}
	trainer.Spec.Workers["group1"] = group

	bytes, _ := yaml.Marshal(trainer)

	t.Logf(fmt.Sprintf("%v", string(bytes)))
}
