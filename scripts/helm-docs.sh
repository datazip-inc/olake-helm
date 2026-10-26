#!/usr/bin/env bash
set -euo pipefail

# Navigate to the repository root directory
cd "$(dirname "$0")/.."

HELM_DOCS_VERSION="v1.14.2"

echo "Running helm-docs via Docker (${HELM_DOCS_VERSION})..."
docker run --rm \
    -v "$(pwd):/helm-docs" \
    -u "$(id -u):$(id -g)" \
    "jnorwood/helm-docs:${HELM_DOCS_VERSION}" \
    --chart-search-root=helm/olake \
    --template-files=OLAKE_README.md.gotmpl \
    --output-file=OLAKE_README.md
