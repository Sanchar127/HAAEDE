#!/bin/bash

echo "Pods"

kubectl get pods -n observability

echo ""
echo "Services"

kubectl get svc -n observability

echo ""
echo "Persistent volumes"

kubectl get pvc -n observability