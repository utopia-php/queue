#!/bin/sh
# KEDA ScaledJob POC e2e: stand up kind + KEDA + Redis + the ScaledJob, push
# messages onto the Redis queue, and assert KEDA spawns worker Jobs that drain
# it. Needs docker + kind + kubectl + helm.
set -e
cd "$(dirname "$0")/.."

CLUSTER="${KIND_CLUSTER:-utopia-queue-keda}"
KUBECONFIG_FILE="$(pwd)/.kubeconfig.keda"

cleanup() {
    kind delete cluster --name "$CLUSTER" > /dev/null 2>&1 || true
    rm -f "$KUBECONFIG_FILE"
}
trap cleanup EXIT INT TERM

if ! kind get clusters 2> /dev/null | grep -qx "$CLUSTER"; then
    kind create cluster --name "$CLUSTER" --wait 120s
fi
kind get kubeconfig --name "$CLUSTER" > "$KUBECONFIG_FILE"
export KUBECONFIG="$KUBECONFIG_FILE"

helm repo add kedacore https://kedacore.github.io/charts > /dev/null 2>&1 || true
helm repo update kedacore > /dev/null
helm upgrade --install keda kedacore/keda --namespace keda --create-namespace --wait

docker build -t utopia-queue-keda-worker:e2e -f tests/Queue/servers/Keda/Dockerfile .
kind load docker-image utopia-queue-keda-worker:e2e --name "$CLUSTER"

kubectl apply -f tests/Queue/servers/Keda/k8s.yaml
kubectl rollout status deploy/redis -n utopia-queue-keda --timeout=120s

KEDA_E2E=true ../../vendor/bin/phpunit --filter KedaTest
