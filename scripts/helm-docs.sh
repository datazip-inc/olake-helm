#!/usr/bin/env bash
set -euo pipefail

# Navigate to the repository root directory
cd "$(dirname "$0")/.."

HELM_DOCS_VERSION="${HELM_DOCS_VERSION:-1.14.2}"
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
    case "${OS}" in
        Darwin) OS="Darwin" ;;
        Linux)  OS="Linux" ;;
        *) echo "Unsupported operating system: ${OS}" >&2; exit 1 ;;
    esac

    ARCH="$(uname -m)"
    case "${ARCH}" in
        x86_64|amd64) ARCH="x86_64" ;;
        arm64|aarch64) ARCH="arm64" ;;
        *) echo "Unsupported architecture: ${ARCH}" >&2; exit 1 ;;
    esac

    RELEASE_BASE_URL="https://github.com/norwoodj/helm-docs/releases/download/v${HELM_DOCS_VERSION}"
    TAR_NAME="helm-docs_${HELM_DOCS_VERSION}_${OS}_${ARCH}.tar.gz"
    TAR_PATH="${BIN_DIR}/${TAR_NAME}"
    CHECKSUMS_PATH="${BIN_DIR}/checksums.txt"

    echo "Downloading ${TAR_NAME}..."
    curl -sSL "${RELEASE_BASE_URL}/${TAR_NAME}" -o "${TAR_PATH}"

    echo "Downloading official checksums.txt..."
    curl -sSL "${RELEASE_BASE_URL}/checksums.txt" -o "${CHECKSUMS_PATH}"

    echo "Verifying SHA256 checksum..."
    EXPECTED_SHA="$(grep -F "${TAR_NAME}" "${CHECKSUMS_PATH}" | awk '{print $1}')"
    if [[ -z "${EXPECTED_SHA}" ]]; then
        echo "Error: Could not find checksum for ${TAR_NAME} in official checksums.txt!" >&2
        rm -f "${TAR_PATH}" "${CHECKSUMS_PATH}"
        exit 1
    fi

    if command -v sha256sum &> /dev/null; then
        ACTUAL_SHA="$(sha256sum "${TAR_PATH}" | awk '{print $1}')"
    elif command -v shasum &> /dev/null; then
        ACTUAL_SHA="$(shasum -a 256 "${TAR_PATH}" | awk '{print $1}')"
    else
        echo "Error: Neither sha256sum nor shasum is available to verify checksum." >&2
        rm -f "${TAR_PATH}" "${CHECKSUMS_PATH}"
        exit 1
    fi

    if [[ "${ACTUAL_SHA}" != "${EXPECTED_SHA}" ]]; then
        echo "SECURITY ERROR: Checksum verification failed for ${TAR_NAME}!" >&2
        echo "Expected: ${EXPECTED_SHA}" >&2
        echo "Got:      ${ACTUAL_SHA}" >&2
        rm -f "${TAR_PATH}" "${CHECKSUMS_PATH}"
        exit 1
    fi

    echo "Checksum verified successfully (${ACTUAL_SHA})."
    tar -xzf "${TAR_PATH}" -C "${BIN_DIR}" helm-docs
    rm -f "${TAR_PATH}" "${CHECKSUMS_PATH}"
    chmod +x "${HELM_DOCS_BIN}"
    HELM_DOCS_CMD="${HELM_DOCS_BIN}"
    echo "Successfully installed helm-docs to ${HELM_DOCS_BIN}"
fi

# Run helm-docs
"${HELM_DOCS_CMD}" \
    --chart-search-root=helm/olake \
    --template-files=OLAKE_README.md.gotmpl \
    --output-file=OLAKE_README.md
