#!/bin/bash
set -e

cd "$(dirname "$0")"

# Define apps: name | chart-name | version | repo
apps=(
  "prometheus|kube-prometheus-stack|58.0.0|https://prometheus-community.github.io/helm-charts"
  "grafana|grafana|7.3.7|https://grafana.github.io/helm-charts"
  "loki|loki|6.25.0|https://grafana.github.io/helm-charts"
  "tempo|tempo|1.10.1|https://grafana.github.io/helm-charts"
)

for entry in "${apps[@]}"; do
  IFS='|' read -r app chart version repo <<< "$entry"
  
  echo "🔧 Setting up wrapper for: $app"
  
  # ✅ CORRECT: Chart.yaml (not charts.yaml)
  cat > "$app/Chart.yaml" << EOF
apiVersion: v2
name: ${app}-wrapper
version: 1.0.0
appVersion: "1.0.0"
type: application
dependencies:
  - name: $chart
    version: $version
    repository: $repo
EOF

  # Run helm dependency update in the app directory
  (cd "$app" && helm dependency update)
done

echo "✅ All wrappers ready! Commit and push to trigger ArgoCD sync."