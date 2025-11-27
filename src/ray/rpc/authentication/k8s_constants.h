// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

namespace ray {
namespace rpc {
namespace k8s {

constexpr char kK8sServiceHostEnvVar[] = "KUBERNETES_SERVICE_HOST";
constexpr char kK8sServicePortEnvVar[] = "KUBERNETES_SERVICE_PORT";

constexpr char kK8sCaCertPath[] = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
constexpr char kK8sSaTokenPath[] = "/var/run/secrets/kubernetes.io/serviceaccount/token";

constexpr char kRayClusterNameEnvVar[] = "RAY_CLUSTER_NAME";
constexpr char kRayClusterNamespaceEnvVar[] = "RAY_CLUSTER_NAMESPACE";

constexpr char kAuthenticationV1TokenReviewPath[] =
    "/apis/authentication.k8s.io/v1/tokenreviews";
constexpr char kAuthorizationV1SubjectAccessReviewPath[] =
    "/apis/authorization.k8s.io/v1/subjectaccessreviews";

constexpr char kTokenReviewKind[] = "TokenReview";
constexpr char kSubjectAccessReviewKind[] = "SubjectAccessReview";

constexpr char kAuthenticationAPIVersion[] = "authentication.k8s.io/v1";
constexpr char kAuthorizationAPIVersion[] = "authorization.k8s.io/v1";

constexpr char kRayResourceGroup[] = "ray.io";
constexpr char kRayClusterResourceName[] = "rayclusters";
constexpr char kRayClusterRayUserVerb[] = "ray-user";

}  // namespace k8s
}  // namespace rpc
}  // namespace ray
