set -ex

install_kubectl() {
    curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl"
    chmod +x ./kubectl
    sudo mv ./kubectl /usr/local/bin/kubectl
    kubectl version --client
}

install_minikube() {
    curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
    sudo install minikube-linux-amd64 /usr/local/bin/minikube
}

start_minikube() {
    sudo minikube start --vm-driver=none
    sudo minikube status
}

check_k8s() {
    kubectl get nodes
    kubectl get pods --all
}

install_kubectl
install_minikube
start_minikube
check_k8s
