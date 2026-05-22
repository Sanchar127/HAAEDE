#!/bin/bash

set -e

echo "Creating namespace..."

kubectl create namespace observability \
--dry-run=client -o yaml | kubectl apply -f -

echo "Adding Helm repositories..."

helm repo add prometheus-community \
https://prometheus-community.github.io/helm-charts

helm repo add grafana \
https://grafana.github.io/helm-charts

helm repo update

echo "Installing Prometheus stack..."

helm upgrade --install monitoring \
prometheus-community/kube-prometheus-stack \
-n observability \
-f ./prometheus/values.yaml

echo "Installing Loki stack..."

helm upgrade --install loki \
grafana/loki-stack \
-n observability \
-f ./loki/values.yaml

echo "Installing Grafana..."

helm upgrade --install grafana \
grafana/grafana \
-n observability \
-f ./grafana/values.yaml

echo "Completed"