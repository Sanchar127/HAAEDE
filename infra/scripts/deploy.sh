#!/bin/bash

set -e

echo "🚀 Deploying Recovery Platform..."

kubectl apply -f infra/k8s/base/
kubectl apply -f infra/k8s/postgres/
kubectl apply -f infra/k8s/kafka/
kubectl apply -f infra/k8s/apps/
kubectl apply -f infra/k8s/observability/

echo "✅ Deployment complete"