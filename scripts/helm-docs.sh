#!/usr/bin/env bash
set -euo pipefail

# Navigate to the repository root directory
cd "$(dirname "$0")/.."

# Detect Operating System (including Git Bash/MSYS2/Cygwin/UCRT64 on Windows)
OS="$(uname -s)"
case "${OS}" in
    Darwin)  OS="Darwin" ;;
    Linux)   OS="Linux" ;;
    MINGW*|MSYS*|CYGWIN*|Windows_NT*|*_NT*) OS="Windows" ;;
    *) echo "Unsupported operating system: ${OS}" >&2; exit 1 ;;
esac

BIN_EXT=""
if [[ "${OS}" == "Windows" ]]; then
    BIN_EXT=".exe"
fi

HELM_DOCS_VERSION="1.14.2"
BIN_DIR="$(pwd)/.bin"
HELM_DOCS_BIN="${BIN_DIR}/helm-docs${BIN_EXT}"

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

    ARCH="$(uname -m)"
    case "${ARCH}" in
        x86_64|amd64) ARCH="x86_64" ;;
        arm64|aarch64) ARCH="arm64" ;;
        *) echo "Unsupported architecture: ${ARCH}" >&2; exit 1 ;;
    esac

    # Pinned SHA256 checksums from official v1.14.2 release:
    # https://github.com/norwoodj/helm-docs/releases/download/v1.14.2/checksums.txt
    EXPECTED_SHA=""
    case "${OS}_${ARCH}" in
        Darwin_arm64)   EXPECTED_SHA="2d8399db5b33d240d5f8985241bcf5483563150b968e3229823822979f3e4b8b" ;;
        Darwin_x86_64)  EXPECTED_SHA="b2f1ffd0feef8dc0901a38a2053481d1d67b63ca30da4ac774166c6b52fa2245" ;;
        Linux_arm64)    EXPECTED_SHA="c3787212332386dcd122debef7848feb165aa701467ae3e3442df7638f3ac4e4" ;;
        Linux_x86_64)   EXPECTED_SHA="a8cf72ada34fad93285ba2a452b38bdc5bd52cc9a571236244ec31022928d6cc" ;;
        Windows_arm64)  EXPECTED_SHA="c6a5f43e89837bd29749298ca27ed350ca8ee6ef64e25ce9653e709171dbbaa4" ;;
        Windows_x86_64) EXPECTED_SHA="3c54fd78d99e2769cf83a0faf12cf6cd4de1cac6ed7bee8d908a2c8fc23f538c" ;;
        *) echo "Unsupported OS/architecture or no pinned checksum for: ${OS}_${ARCH}" >&2; exit 1 ;;
    esac

    TAR_NAME="helm-docs_${HELM_DOCS_VERSION}_${OS}_${ARCH}.tar.gz"
    TAR_PATH="${BIN_DIR}/${TAR_NAME}"
    DOWNLOAD_URL="https://github.com/norwoodj/helm-docs/releases/download/v${HELM_DOCS_VERSION}/${TAR_NAME}"

    echo "Downloading helm-docs v${HELM_DOCS_VERSION} from ${DOWNLOAD_URL}..."
    curl -sSL "${DOWNLOAD_URL}" -o "${TAR_PATH}"

    # Verify SHA256 checksum to prevent supply chain tampering
    echo "Verifying SHA256 checksum..."
    if command -v sha256sum &> /dev/null; then
        ACTUAL_SHA="$(sha256sum "${TAR_PATH}" | awk '{print $1}')"
    elif command -v shasum &> /dev/null; then
        ACTUAL_SHA="$(shasum -a 256 "${TAR_PATH}" | awk '{print $1}')"
    else
        echo "Error: Neither sha256sum nor shasum is available to verify checksum." >&2
        rm -f "${TAR_PATH}"
        exit 1
    fi

    if [[ "${ACTUAL_SHA}" != "${EXPECTED_SHA}" ]]; then
        echo "SECURITY ERROR: Checksum verification failed for ${TAR_NAME}!" >&2
        echo "Expected: ${EXPECTED_SHA}" >&2
        echo "Got:      ${ACTUAL_SHA}" >&2
        rm -f "${TAR_PATH}"
        exit 1
    fi

    echo "Checksum verified successfully (${ACTUAL_SHA})."
    tar -xzf "${TAR_PATH}" -C "${BIN_DIR}" "helm-docs${BIN_EXT}"
    rm -f "${TAR_PATH}"
    chmod +x "${HELM_DOCS_BIN}"
    HELM_DOCS_CMD="${HELM_DOCS_BIN}"
    echo "Successfully installed helm-docs to ${HELM_DOCS_BIN}"
fi

# Run helm-docs
"${HELM_DOCS_CMD}" \
    --chart-search-root=helm/olake \
    --template-files=OLAKE_README.md.gotmpl \
    --output-file=OLAKE_README.md
