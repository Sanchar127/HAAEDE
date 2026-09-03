#!/usr/bin/env bash

set -Eeuo pipefail

# ============================================================
# HAAEDE Kubernetes Setup
# ============================================================
#
# Usage:
#
#   ./infra/scripts/setup.sh
#
# If the Spark image is already built and imported:
#
#   ./infra/scripts/setup.sh --skip-build
#
# This script is designed for local Kubernetes development.
#
# Deployment order:
#
#   1. Check prerequisites
#   2. Create namespaces
#   3. Build/import custom Spark image
#   4. Install Spark Operator CRDs
#   5. Install Spark Operator
#   6. Install Strimzi Kafka Operator
#   7. Deploy PostgreSQL
#   8. Deploy Kafka
#   9. Deploy MinIO
#  10. Deploy lakehouse/Spark infrastructure
#  11. Deploy application services
#  12. Deploy Iceberg resources
#  13. Show final status
#
# ============================================================


# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

K8S_DIR="$ROOT_DIR/infra/k8s"

SPARK_IMAGE="kafka-iceberg-spark:1.0"
SPARK_DOCKERFILE="$ROOT_DIR/infra/docker/spark/Dockerfile"
SPARK_IMAGE_TAR="/tmp/kafka-iceberg-spark.tar"

cd "$ROOT_DIR"


# ------------------------------------------------------------
# Options
# ------------------------------------------------------------

SKIP_BUILD=false

if [[ "${1:-}" == "--skip-build" ]]; then
    SKIP_BUILD=true
elif [[ $# -gt 0 ]]; then
    echo "ERROR: Unknown argument: $1"
    echo
    echo "Usage:"
    echo "  ./infra/scripts/setup.sh"
    echo "  ./infra/scripts/setup.sh --skip-build"
    exit 1
fi


# ------------------------------------------------------------
# Colors / logging
# ------------------------------------------------------------

info() {
    echo
    echo "=========================================="
    echo "$1"
    echo "=========================================="
}

success() {
    echo "✓ $1"
}

warning() {
    echo "WARNING: $1"
}

error() {
    echo "ERROR: $1" >&2
}


# ------------------------------------------------------------
# Failure handler
# ------------------------------------------------------------

on_error() {
    echo
    echo "=========================================="
    echo " HAAEDE Kubernetes setup FAILED"
    echo "=========================================="
    echo
    echo "The command that failed:"
    echo "  $BASH_COMMAND"
    echo
    echo "Useful debugging commands:"
    echo
    echo "  kubectl get pods -A"
    echo "  kubectl get events -A --sort-by=.lastTimestamp"
    echo
}

trap on_error ERR


# ------------------------------------------------------------
# Helper: command check
# ------------------------------------------------------------

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        error "'$1' is required but was not found."
        exit 1
    fi
}


# ------------------------------------------------------------
# Helper: deployment readiness
# ------------------------------------------------------------

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

    success "$namespace/$deployment is ready."
}


# ------------------------------------------------------------
# Helper: namespace readiness
# ------------------------------------------------------------

ensure_namespace() {
    local namespace="$1"

    if kubectl get namespace "$namespace" >/dev/null 2>&1; then
        echo "Namespace '$namespace' already exists."
    else
        echo "Creating namespace '$namespace'..."
        kubectl create namespace "$namespace"
    fi
}


# ------------------------------------------------------------
# Helper: GitHub authentication secret
# ------------------------------------------------------------

ensure_github_secret() {
    local namespace="recovery"
    local secret_name="ingestion-github"

    echo
    echo "Checking GitHub authentication..."

    # Reuse existing secret
    if kubectl get secret "$secret_name" -n "$namespace" >/dev/null 2>&1; then
        success "GitHub Secret '$secret_name' already exists."
        return 0
    fi

    echo
    echo "GitHub authentication is not configured."
    echo
    echo "A GitHub Personal Access Token (PAT) is required so the"
    echo "ingestion service can access the GitHub API with authentication."
    echo
    echo "The token will be stored only in Kubernetes Secret:"
    echo "  $namespace/$secret_name"
    echo

    # Allow experienced users / CI to provide the token through
    # an environment variable.
    if [[ -n "${INGESTION_GITHUB_TOKEN:-}" ]]; then

        GITHUB_TOKEN="$INGESTION_GITHUB_TOKEN"

    else

        # Interactive setup
        if [[ ! -t 0 ]]; then
            error "GitHub PAT is required, but setup is running non-interactively."
            echo
            echo "Set the token first:"
            echo
            echo '  export INGESTION_GITHUB_TOKEN="your_github_pat"'
            echo
            echo "Then run:"
            echo
            echo "  ./infra/scripts/setup.sh"
            exit 1
        fi

        read -rsp "Enter your GitHub PAT: " GITHUB_TOKEN
        echo

    fi

    if [[ -z "$GITHUB_TOKEN" ]]; then
        error "GitHub PAT cannot be empty."
        exit 1
    fi

    echo
    echo "Creating Kubernetes Secret..."

    kubectl create secret generic "$secret_name" \
        -n "$namespace" \
        --from-literal=INGESTION_GITHUB_TOKEN="$GITHUB_TOKEN"

    # Remove the shell variable as soon as it is no longer needed.
    unset GITHUB_TOKEN

    success "GitHub authentication Secret created."
}

# ------------------------------------------------------------
# 1. Prerequisites
# ------------------------------------------------------------

info "1. Checking prerequisites"

require_command kubectl
require_command docker
require_command ctr

echo
echo "kubectl:"
kubectl version --client

echo
echo "Docker:"
docker --version

echo
echo "containerd:"
ctr --version


# ------------------------------------------------------------
# 2. Kubernetes connectivity
# ------------------------------------------------------------

info "2. Checking Kubernetes cluster"

if ! kubectl cluster-info >/dev/null 2>&1; then
    error "Cannot connect to Kubernetes."
    echo
    echo "Make sure your local Kubernetes cluster is running."
    echo
    echo "Check with:"
    echo "  kubectl cluster-info"
    echo "  kubectl get nodes"
    exit 1
fi

echo
echo "--- Kubernetes nodes ---"
kubectl get nodes -o wide

if ! kubectl get nodes --no-headers | grep -q " Ready "; then
    error "No Kubernetes node is currently Ready."
    exit 1
fi

success "Kubernetes cluster is reachable."


# ------------------------------------------------------------
# 3. Create namespaces
# ------------------------------------------------------------

info "3. Creating HAAEDE namespaces"

# Do this explicitly instead of assuming base/kustomization
# creates every namespace.

ensure_namespace "recovery"
ensure_namespace "kafka"
ensure_namespace "lakehouse"
ensure_namespace "spark"
ensure_namespace "spark-operator"

success "All required namespaces exist."


# ------------------------------------------------------------
# 4. Build custom Spark image
# ------------------------------------------------------------

info "4. Preparing custom Spark image"

if [[ "$SKIP_BUILD" == "true" ]]; then

    echo "Skipping Docker build because --skip-build was supplied."

    if ! sudo ctr -n k8s.io images list | grep -q "kafka-iceberg-spark"; then
        error "Spark image '$SPARK_IMAGE' was not found in containerd."
        echo
        echo "Run without --skip-build:"
        echo
        echo "  ./infra/scripts/setup.sh"
        exit 1
    fi

    success "Existing Spark image found in containerd."

else

    if [[ ! -f "$SPARK_DOCKERFILE" ]]; then
        error "Spark Dockerfile not found:"
        echo "  $SPARK_DOCKERFILE"
        exit 1
    fi

    echo
    echo "Building:"
    echo "  $SPARK_IMAGE"

    docker build \
        -t "$SPARK_IMAGE" \
        -f "$SPARK_DOCKERFILE" \
        "$ROOT_DIR"

    success "Spark Docker image built."

    echo
    echo "Exporting Spark image..."

    docker save \
        "$SPARK_IMAGE" \
        -o "$SPARK_IMAGE_TAR"

    success "Spark image exported to $SPARK_IMAGE_TAR."

    echo
    echo "Importing Spark image into Kubernetes containerd..."

    sudo ctr -n k8s.io images import "$SPARK_IMAGE_TAR"

    success "Spark image imported into containerd."

fi


# ------------------------------------------------------------
# 5. Base configuration
# ------------------------------------------------------------

info "5. Applying base configuration"

kubectl apply -k "$K8S_DIR/base"

success "Base configuration applied."


# ------------------------------------------------------------
# 6. Spark Operator CRDs
# ------------------------------------------------------------

info "6. Installing Spark Operator CRDs"

# Spark Operator CRDs are large.
#
# Normal kubectl apply can fail with:
#
#   metadata.annotations: Too long
#
# Therefore CRDs are installed using server-side apply.

kubectl apply \
    --server-side \
    --force-conflicts \
    -k "$K8S_DIR/spark-operator/crd"

success "Spark Operator CRDs installed."


# ------------------------------------------------------------
# 7. Spark Operator
# ------------------------------------------------------------

info "7. Installing Spark Operator"

kubectl apply \
    --server-side \
    --force-conflicts \
    -k "$K8S_DIR/spark-operator"

success "Spark Operator resources applied."

wait_for_deployment \
    "spark-operator" \
    "spark-operator-controller"

wait_for_deployment \
    "spark-operator" \
    "spark-operator-webhook"

success "Spark Operator is ready."


# ------------------------------------------------------------
# 8. Strimzi Kafka Operator
# ------------------------------------------------------------

info "8. Installing Strimzi Kafka Operator"

# IMPORTANT:
#
# Do NOT create the Kafka cluster before the Strimzi operator
# is running.
#
# The order is:
#
#   Strimzi CRDs
#        ↓
#   Strimzi Operator
#        ↓
#   Kafka CR
#        ↓
#   Kafka cluster

kubectl apply -k "$K8S_DIR/kafka/strimzi-install.yaml"

success "Strimzi resources applied."


# ------------------------------------------------------------
# Wait for Strimzi
# ------------------------------------------------------------

echo
echo "Waiting for Strimzi Cluster Operator..."

if ! kubectl get deployment \
    strimzi-cluster-operator \
    -n kafka >/dev/null 2>&1; then

    error "Strimzi Cluster Operator deployment was not created."

    echo
    echo "Current Kafka namespace:"
    kubectl get all -n kafka || true

    exit 1
fi

wait_for_deployment \
    "kafka" \
    "strimzi-cluster-operator"

success "Strimzi Kafka Operator is ready."


# ------------------------------------------------------------
# 9. PostgreSQL
# ------------------------------------------------------------

info "9. Deploying PostgreSQL"

kubectl apply -k "$K8S_DIR/postgres"

success "PostgreSQL resources applied."

wait_for_deployment \
    "recovery" \
    "postgres" \
    "180s" 2>/dev/null || true

# PostgreSQL is a StatefulSet, not Deployment.
# Check the StatefulSet directly.

echo
echo "Waiting for PostgreSQL StatefulSet..."

kubectl rollout status \
    statefulset/postgres \
    -n recovery \
    --timeout=180s

success "PostgreSQL is ready."


# ------------------------------------------------------------
# 10. Kafka cluster
# ------------------------------------------------------------

info "10. Deploying Kafka cluster"

if ! kubectl get crd kafkas.kafka.strimzi.io >/dev/null 2>&1; then
    error "Strimzi Kafka CRD is not installed."
    exit 1
fi

# At this point Strimzi is already running, so now it is safe
# to create Kafka CRs.

kubectl apply -k "$K8S_DIR/kafka"

success "Kafka resources applied."


# ------------------------------------------------------------
# Wait for Kafka
# ------------------------------------------------------------

echo
echo "Waiting for Kafka cluster recovery-kafka..."

KAFKA_READY=false

for attempt in $(seq 1 60); do

    READY_STATUS="$(
        kubectl get kafka recovery-kafka \
            -n kafka \
            -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' \
            2>/dev/null || true
    )"

    if [[ "$READY_STATUS" == "True" ]]; then
        KAFKA_READY=true
        break
    fi

    echo "  Kafka is not ready yet... ($attempt/60)"
    sleep 5

done

if [[ "$KAFKA_READY" != "true" ]]; then

    error "Kafka did not become Ready within 5 minutes."

    echo
    echo "--- Kafka resource ---"
    kubectl get kafka recovery-kafka -n kafka -o yaml || true

    echo
    echo "--- Kafka pods ---"
    kubectl get pods -n kafka -o wide || true

    exit 1
fi

success "Kafka recovery-kafka is Ready."


# ------------------------------------------------------------
# 11. MinIO
# ------------------------------------------------------------

info "11. Deploying MinIO"

kubectl apply -k "$K8S_DIR/lakehouse/minio"

success "MinIO resources applied."


# ------------------------------------------------------------
# Wait for MinIO
# ------------------------------------------------------------

echo
echo "Waiting for MinIO..."

kubectl rollout status \
    deployment/minio \
    -n lakehouse \
    --timeout=180s

success "MinIO deployment is ready."


# ------------------------------------------------------------
# Wait for MinIO initialization
# ------------------------------------------------------------

echo
echo "Waiting for MinIO initialization job..."

if kubectl get job minio-init -n lakehouse >/dev/null 2>&1; then

    kubectl wait \
        --for=condition=complete \
        job/minio-init \
        -n lakehouse \
        --timeout=180s

    success "MinIO buckets initialized."

else

    warning "MinIO initialization job was not found."

fi


# ------------------------------------------------------------
# 12. Lakehouse storage
# ------------------------------------------------------------

info "12. Applying lakehouse storage"

kubectl apply -k "$K8S_DIR/lakehouse/storage"

success "Lakehouse storage applied."


# ------------------------------------------------------------
# 13. Spark infrastructure
# ------------------------------------------------------------

info "13. Applying Spark infrastructure"

kubectl apply -k "$K8S_DIR/lakehouse/spark"

success "Spark infrastructure applied."


# ------------------------------------------------------------
# 14. Application services
# ------------------------------------------------------------

info "14. Deploying HAAEDE application services"

kubectl apply -k "$K8S_DIR/apps"

success "Application services applied."


# ------------------------------------------------------------
# 15. Iceberg
# ------------------------------------------------------------

info "15. Applying Iceberg resources"

kubectl apply -k "$K8S_DIR/lakehouse/iceberg"

success "Iceberg resources applied."


# ------------------------------------------------------------
# 16. Give controllers time to reconcile
# ------------------------------------------------------------

info "16. Waiting for Kubernetes controllers"

echo "Allowing controllers to reconcile resources..."
sleep 10


# ------------------------------------------------------------
# 17. Final status
# ------------------------------------------------------------

info "17. Final cluster status"


echo
echo "--- Nodes ---"
kubectl get nodes -o wide


echo
echo "--- Namespaces ---"
kubectl get namespaces


echo
echo "--- Spark Operator ---"
kubectl get pods -n spark-operator -o wide


echo
echo "--- Kafka / Strimzi ---"
kubectl get pods -n kafka -o wide


echo
echo "--- Kafka cluster ---"
kubectl get kafka -n kafka || true


echo
echo "--- PostgreSQL ---"
kubectl get pods -n recovery -l app=postgres -o wide || true


echo
echo "--- Recovery applications ---"
kubectl get pods -n recovery -o wide


echo
echo "--- MinIO ---"
kubectl get pods -n lakehouse -o wide


echo
echo "--- PVCs ---"
kubectl get pvc -A


echo
echo "--- Spark Applications ---"
kubectl get sparkapplications -A 2>/dev/null || true


# ------------------------------------------------------------
# 18. Success
# ------------------------------------------------------------

echo
echo "=========================================="
echo " HAAEDE Kubernetes setup completed"
echo "=========================================="

echo
echo "Useful commands:"
echo
echo "  kubectl get pods -A"
echo "  kubectl get sparkapplications -A"
echo "  kubectl get kafka -n kafka"
echo "  kubectl get pvc -A"
echo
echo "Kafka:"
echo
echo "  kubectl get kafka recovery-kafka -n kafka"
echo
echo "Spark streaming logs:"
echo
echo "  kubectl logs -n recovery kafka-to-iceberg-driver -f"
echo
echo "Ingestion logs:"
echo
echo "  kubectl logs -n recovery deployment/ingestion -f"
echo
echo "Run again without rebuilding the Spark image:"
echo
echo "  ./infra/scripts/setup.sh --skip-build"
echo
echo "=========================================="
