#!/bin/bash

helm uninstall monitoring -n observability

helm uninstall loki -n observability

helm uninstall grafana -n observability

kubectl delete namespace observability