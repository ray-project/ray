package common

import (
	"bufio"
	"fmt"
	automlv1 "github.com/ray-automl/apis/automl/v1"
	"io"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"strings"
	"sync"
)

const (
	resolverFileName    = "/etc/resolv.conf"
	clusterDomainEnvKey = "CLUSTER_DOMAIN"
	defaultDomainName   = "cluster.local"
)

var (
	domainName = defaultDomainName
	once       sync.Once
)

// GenerateServiceName generates a ray head service name from cluster name
func GenerateServiceName(instance string) string {
	return fmt.Sprintf("%s-%s", instance, "svc")
}

// GetClusterDomainName returns cluster's domain name or an error
func GetClusterDomainName() string {
	once.Do(func() {
		f, err := os.Open(resolverFileName)
		if err != nil {
			return
		}
		defer f.Close()
		domainName = getClusterDomainName(f)
	})

	return domainName
}

func getClusterDomainName(r io.Reader) string {
	// First look in the conf file.
	for scanner := bufio.NewScanner(r); scanner.Scan(); {
		elements := strings.Split(scanner.Text(), " ")
		if elements[0] != "search" {
			continue
		}
		for _, e := range elements[1:] {
			if strings.HasPrefix(e, "svc.") {
				return strings.TrimSuffix(e[4:], ".")
			}
		}
	}

	// Then look in the ENV.
	if domain := os.Getenv(clusterDomainEnvKey); len(domain) > 0 {
		return domain
	}

	// For all abnormal cases return default domain name.
	return defaultDomainName
}

func SetProxyOwnerReference(object metav1.Object, proxy *automlv1.Proxy) {
	ref := metav1.NewControllerRef(proxy, proxy.GetObjectKind().GroupVersionKind())
	object.SetOwnerReferences([]metav1.OwnerReference{*ref})
}

func SetTrainerOwnerReference(object metav1.Object, trainer *automlv1.Trainer) {
	ref := metav1.NewControllerRef(trainer, trainer.GetObjectKind().GroupVersionKind())
	object.SetOwnerReferences([]metav1.OwnerReference{*ref})
}
