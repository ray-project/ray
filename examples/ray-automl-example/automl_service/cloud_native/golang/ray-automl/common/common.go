package common

import (
	"bytes"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sort"
)

func ConvertParamMap(startParams map[string]string) (s string) {
	keys := make([]string, 0)
	for k := range startParams {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	flags := new(bytes.Buffer)
	for _, k := range keys {
		_, _ = fmt.Fprintf(flags, " --%s=%s ", k, startParams[k])
	}
	return flags.String()
}

func GetOrDefault(params map[string]string, key string, defaultValue string) string {
	if params[key] != "" {
		return params[key]
	}
	return defaultValue
}

func BuildResourceList(cpu string, memory string, ephemeralStorage string) corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:              resource.MustParse(cpu),
		corev1.ResourceMemory:           resource.MustParse(memory),
		corev1.ResourceEphemeralStorage: resource.MustParse(ephemeralStorage),
	}
}
