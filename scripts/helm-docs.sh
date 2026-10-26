#!/usr/bin/env bash
set -euo pipefail

# Navigate to the repository root directory
cd "$(dirname "$0")/.."

HELM_DOCS_VERSION="1.14.2"
BIN_DIR="$(pwd)/.bin"
HELM_DOCS_BIN="${BIN_DIR}/helm-docs"

# 1. Use system helm-docs if available on PATH and version matches pinned version
if command -v helm-docs &> /dev/null && helm-docs --version 2>&1 | grep -q "${HELM_DOCS_VERSION}"; then
    HELM_DOCS_CMD="helm-docs"
# 2. Use previously downloaded binary in .bin/ if available and version matches
elif [[ -x "${HELM_DOCS_BIN}" ]] && "${HELM_DOCS_BIN}" --version 2>&1 | grep -q "${HELM_DOCS_VERSION}"; then
    HELM_DOCS_CMD="${HELM_DOCS_BIN}"
# 3. Otherwise, auto-download official standalone binary into .bin/
else
    echo "helm-docs (v${HELM_DOCS_VERSION}) not found locally. Downloading official binary into .bin/..."
    mkdir -p "${BIN_DIR}"

    OS="$(uname -s)"
    ARCH="$(uname -m)"
    case "${ARCH}" in
        x86_64|amd64) ARCH="x86_64" ;;
        arm64|aarch64) ARCH="arm64" ;;
        *) echo "Unsupported architecture: ${ARCH}" >&2; exit 1 ;;
    esac

    DOWNLOAD_URL="https://github.com/norwoodj/helm-docs/releases/download/v${HELM_DOCS_VERSION}/helm-docs_${HELM_DOCS_VERSION}_${OS}_${ARCH}.tar.gz"

    curl -sSL "${DOWNLOAD_URL}" | tar -xz -C "${BIN_DIR}" helm-docs
    chmod +x "${HELM_DOCS_BIN}"
    HELM_DOCS_CMD="${HELM_DOCS_BIN}"
    echo "Successfully downloaded helm-docs to ${HELM_DOCS_BIN}"
fi

# Run helm-docs
"${HELM_DOCS_CMD}" \
    --chart-search-root=helm/olake \
    --template-files=OLAKE_README.md.gotmpl \
    --output-file=OLAKE_README.md
