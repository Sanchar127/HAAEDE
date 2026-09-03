
#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
K8S_DIR="$ROOT_DIR/infra/k8s"

echo "=========================================="
echo " HAAEDE Kubernetes Deployment"
echo "=========================================="

cd "$ROOT_DIR"

wait_for_deployment() {
    local namespace="$1"
    local deployment="$2"
    local timeout="${3:-180s}"

    echo
    echo "Waiting for deployment $namespace/$deployment..."

    kubectl rollout status \
        "deployment/$deployment" \
        -n "$namespace" \
        --timeout="$timeout"
}

echo
echo "=========================================="
echo "1. Applying base namespaces/configuration"
echo "=========================================="

kubectl apply -k "$K8S_DIR/base"

echo
echo "Waiting for namespaces..."

kubectl wait \
    --for=jsonpath='{.status.phase}'=Active \
    namespace/recovery \
    --timeout=60s

kubectl wait \
    --for=jsonpath='{.status.phase}'=Active \
    namespace/kafka \
    --timeout=60s

kubectl wait \
    --for=jsonpath='{.status.phase}'=Active \
    namespace/spark \
    --timeout=60s

kubectl wait \
    --for=jsonpath='{.status.phase}'=Active \
    namespace/lakehouse \
    --timeout=60s


echo
echo "=========================================="
echo "2. Installing Spark Operator"
echo "=========================================="

# Spark CRDs are large, so use server-side apply.
kubectl apply \
    --server-side \
    --force-conflicts \
    -k "$K8S_DIR/spark-operator"

wait_for_deployment \
    spark-operator \
    spark-operator-controller

wait_for_deployment \
    spark-operator \
    spark-operator-webhook


echo
echo "=========================================="
echo "3. Installing Strimzi / Kafka operator"
echo "=========================================="

kubectl apply \
    --server-side \
    --force-conflicts \
    -k "$K8S_DIR/kafka"

echo
echo "Waiting for Strimzi operator..."

if kubectl get deployment strimzi-cluster-operator -n kafka >/dev/null 2>&1; then
    wait_for_deployment \
        kafka \
        strimzi-cluster-operator
else
    echo "WARNING: Strimzi deployment was not found."
    echo "The Kafka Kustomization may need to be adjusted."
fi


echo
echo "=========================================="
echo "4. Applying Kafka cluster resources"
echo "=========================================="

if kubectl get crd kafkas.kafka.strimzi.io >/dev/null 2>&1; then

    if kubectl get kafka recovery-kafka -n kafka >/dev/null 2>&1; then
        echo "Kafka cluster recovery-kafka already exists."
    else
        echo "Creating Kafka cluster..."
        kubectl apply -k "$K8S_DIR/kafka"
    fi

else
    echo "ERROR: Strimzi Kafka CRD is not installed."
    echo "Cannot create Kafka cluster."
    exit 1
fi


echo
echo "=========================================="
echo "5. Applying PostgreSQL"
echo "=========================================="

kubectl apply -k "$K8S_DIR/postgres"


echo
echo "=========================================="
echo "6. Applying MinIO"
echo "=========================================="

kubectl apply -k "$K8S_DIR/lakehouse/minio"


echo
echo "=========================================="
echo "7. Applying Iceberg"
echo "=========================================="

kubectl apply -k "$K8S_DIR/lakehouse/iceberg"


echo
echo "=========================================="
echo "8. Applying Spark application infrastructure"
echo "=========================================="

kubectl apply -k "$K8S_DIR/lakehouse/spark"


echo
echo "=========================================="
echo "9. Applying application services"
echo "=========================================="

kubectl apply -k "$K8S_DIR/apps"


echo
echo "=========================================="
echo "10. Final cluster status"
echo "=========================================="

echo
echo "--- Nodes ---"
kubectl get nodes

echo
echo "--- Namespaces ---"
kubectl get namespaces

echo
echo "--- Spark Operator ---"
kubectl get pods -n spark-operator

echo
echo "--- Kafka ---"
kubectl get pods -n kafka

echo
echo "--- PostgreSQL ---"
kubectl get pods -n recovery -l app=postgres

echo
echo "--- MinIO ---"
kubectl get pods -n lakehouse

echo
echo "--- Spark Applications ---"
kubectl get sparkapplications -A 2>/dev/null || true

echo
echo "=========================================="
echo " HAAEDE Kubernetes deployment finished"
echo "=========================================="
