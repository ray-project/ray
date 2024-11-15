import os
import unittest
from unittest.mock import patch
from ray.autoscaler._private.kuberay.node_provider import url_from_resource


class TestKubeRayNodeProvider(unittest.TestCase):
    def test_url_from_resource_default(self):
        k8s_host = "https://kubernetes.default:443"
        testData = [
            {
                "namespace": "test",
                "path": "pods",
                "crdVersion": "v1",
                "expectedUrl": f"{k8s_host}/api/v1/namespaces/test/pods",
            },
            {
                "namespace": "test",
                "path": "rayclusters",
                "crdVersion": "v1",
                "expectedUrl": f"{k8s_host}/apis/ray.io/v1/namespaces/test/rayclusters",
            },
        ]
        # iterate through the data
        for item in testData:
            urlResult = url_from_resource(
                item["namespace"], item["path"], item["crdVersion"]
            )
            self.assertEqual(urlResult, item["expectedUrl"])

    @patch.dict(
        os.environ,
        {
            "KUBERNETES_SERVICE_HOST": "https://kubernetes.custom",
            "KUBERNETES_SERVICE_PORT_HTTPS": "6443",
        },
    )
    def test_url_from_resource_custom(self):
        k8s_host = "https://kubernetes.custom:6443"
        testData = [
            {
                "namespace": "test",
                "path": "pods",
                "crdVersion": "v1",
                "expectedUrl": f"{k8s_host}/api/v1/namespaces/test/pods",
            },
            {
                "namespace": "test",
                "path": "rayclusters",
                "crdVersion": "v1",
                "expectedUrl": f"{k8s_host}/apis/ray.io/v1/namespaces/test/rayclusters",
            },
        ]
        # iterate through the data
        for item in testData:
            urlResult = url_from_resource(
                item["namespace"], item["path"], item["crdVersion"]
            )
            self.assertEqual(urlResult, item["expectedUrl"])


if __name__ == "__main__":
    unittest.main()
